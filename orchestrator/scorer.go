package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// CategoryScore holds the result for one scoring category.
type CategoryScore struct {
	Name     string  `json:"name"`
	MaxScore float64 `json:"max_score"`
	Score    float64 `json:"score"`
	Details  string  `json:"details"`
}

// Scorecard is the full scoring report for a participant.
type Scorecard struct {
	Participant string          `json:"participant"`
	Timestamp   string          `json:"timestamp"`
	Categories  []CategoryScore `json:"categories"`
	TotalScore  float64         `json:"total_score"`
	MaxTotal    float64         `json:"max_total"`
	Percentage  float64         `json:"percentage"`
}

// NewScorecard creates a new empty scorecard for a participant.
func NewScorecard(participant string) *Scorecard {
	return &Scorecard{
		Participant: participant,
		Timestamp:   time.Now().UTC().Format(time.RFC3339),
		Categories:  []CategoryScore{},
	}
}

// AddCategory adds a scored category to the scorecard.
func (s *Scorecard) AddCategory(name string, maxScore, score float64, details string) {
	if score < 0 {
		score = 0
	}
	if score > maxScore {
		score = maxScore
	}
	s.Categories = append(s.Categories, CategoryScore{
		Name:     name,
		MaxScore: maxScore,
		Score:    score,
		Details:  details,
	})
	s.recalculate()
}

// recalculate updates totals after a category is added.
func (s *Scorecard) recalculate() {
	s.TotalScore = 0
	s.MaxTotal = 0
	for _, c := range s.Categories {
		s.TotalScore += c.Score
		s.MaxTotal += c.MaxScore
	}
	if s.MaxTotal > 0 {
		s.Percentage = (s.TotalScore / s.MaxTotal) * 100
	}
}

// WriteJSON writes the scorecard to a JSON file.
func (s *Scorecard) WriteJSON(path string) error {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal scorecard: %w", err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("write scorecard to %s: %w", path, err)
	}
	return nil
}

// PrintSummary prints a formatted summary of the scorecard to stdout.
func (s *Scorecard) PrintSummary() {
	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Printf("║  SCORING REPORT: %-43s║\n", s.Participant)
	fmt.Printf("║  Timestamp: %-48s║\n", s.Timestamp)
	fmt.Println("╠══════════════════════════════════════════════════════════════╣")

	for _, c := range s.Categories {
		fmt.Printf("║  %-40s %5.1f / %4.0f pts ║\n", c.Name, c.Score, c.MaxScore)
		if c.Details != "" {
			// Wrap details to fit in the box
			detail := c.Details
			if len(detail) > 56 {
				detail = detail[:53] + "..."
			}
			fmt.Printf("║    %-56s║\n", detail)
		}
	}

	fmt.Println("╠══════════════════════════════════════════════════════════════╣")
	fmt.Printf("║  TOTAL SCORE: %39.1f / %.0f ║\n", s.TotalScore, s.MaxTotal)
	fmt.Printf("║  PERCENTAGE:  %40.1f%%  ║\n", s.Percentage)
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Println()
}
