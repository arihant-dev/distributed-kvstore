package store

import (
	"log"
	"os"
	"path/filepath"
	"runtime"
)

func init() {
	// Only run this on local developer machines during testing, not in CI/CD pipelines
	if os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != "" {
		return
	}
	setupGitHook()
}

func setupGitHook() {
	// Get file path of this test file to locate repository root
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return
	}
	repoRoot := filepath.Dir(filepath.Dir(filename))
	gitHooksDir := filepath.Join(repoRoot, ".git", "hooks")

	// Check if .git/hooks directory exists
	if _, err := os.Stat(gitHooksDir); os.IsNotExist(err) {
		return // Not a git repo or running inside a container where .git is not mounted
	}

	preCommitPath := filepath.Join(gitHooksDir, "pre-commit")

	// The pre-commit hook script content
	hookContent := `#!/bin/bash
# Automatically generated pre-commit hook. Do not edit directly.
REPO_DIR="$(git rev-parse --show-toplevel)"
cd "$REPO_DIR"

echo "=== Running Pre-Commit Hooks ==="

# 1. Run Go Format check
echo "Checking code formatting (go fmt)..."
unformatted=$(go fmt ./...)
if [ -n "$unformatted" ]; then
    echo "❌ Error: The following files are not formatted correctly:"
    echo "$unformatted"
    echo "Please run 'go fmt ./...' and stage the changes."
    exit 1
fi
echo "✅ Code formatting is clean."

# 2. Run Go Vet (static analysis)
echo "Running static analysis (go vet)..."
if ! go vet ./...; then
    echo "❌ Error: 'go vet' failed. Fix static analysis issues before committing."
    exit 1
fi
echo "✅ Static analysis passed."

# 3. Run Unit Tests & check coverage
echo "Running unit tests with coverage..."
COVERAGE_FILE="/tmp/coverage.out"
if ! go test -coverprofile="$COVERAGE_FILE" ./server ./store; then
    echo "❌ Error: Some unit tests failed. Commit aborted."
    exit 1
fi
echo "✅ All unit tests passed."

# 4. Check coverage percentage threshold (e.g. 15%)
THRESHOLD=15
TOTAL_COVERAGE=$(go tool cover -func="$COVERAGE_FILE" | grep "total:" | awk '{print $3}' | tr -d '%')
if [ -z "$TOTAL_COVERAGE" ]; then
    TOTAL_COVERAGE=0
fi
COVERAGE_INT=$(printf "%.0f" "$TOTAL_COVERAGE")
echo "Total test coverage (server/store): $TOTAL_COVERAGE% (Threshold: $THRESHOLD%)"
if [ "$COVERAGE_INT" -lt "$THRESHOLD" ]; then
    echo "❌ Error: Test coverage ($TOTAL_COVERAGE%) is below the required threshold of $THRESHOLD%."
    echo "Please write more unit tests to increase coverage."
    exit 1
fi
echo "✅ Test coverage is sufficient."
echo "=== Pre-Commit Checks Passed! ==="
exit 0
`

	// Write the hook file and make it executable
	err := os.WriteFile(preCommitPath, []byte(hookContent), 0755)
	if err != nil {
		log.Printf("Warning: Failed to install git pre-commit hook: %v", err)
	}
}
