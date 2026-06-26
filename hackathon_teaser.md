# 🔥 Build It, Break It, Fix It

### Distributed Systems Hackathon — This Sunday

---

## What Is This?

A solo hackathon where you'll build a **real distributed system from scratch** — and then we'll try to **break it**.

This isn't a typical hackathon. We're not building apps. We're not judging features or UI. We're going deep into how distributed systems actually work — the communication, the failures, the trade-offs that every large-scale system has to make.

**You will build. We will break. You will (try to) survive.**

---

## The Details

| | |
|---|---|
| **When** | This Sunday, ~6 hours |
| **Format** | Solo — you build it, you defend it |
| **What** | You'll be given a distributed systems challenge at the start. Build it across multiple nodes. |
| **Language** | Your choice — Go, Rust, Java, Python, C++, whatever you're comfortable with |
| **Deployment** | Your system will run on the cloud (3 nodes). Docker Compose or Kubernetes — your call. |
| **AI tools** | Fully allowed (Copilot, ChatGPT, Claude, Cursor, etc.) |
| **Prize** | Knowledge that'll make you a better engineer. Also, lunch. 🍕 |

---

## What to Expect

- You'll get the **exact challenge** at the start of the hackathon — not before
- The challenge involves building a system that runs across **multiple nodes** (think: separate servers/containers that talk to each other)
- Your system will be judged on how well it handles **failures and load** — not how many features it has
- There will be a **chaos round** where things will go wrong with your system. On purpose. By us. 😈
- At the end, you'll present your **design decisions** — what trade-offs you made and why

> This is not about knowing the "right answer." It's about thinking through hard problems and making deliberate engineering decisions. People with different backgrounds will approach this differently — that's the point.

---

## What This is NOT

- ❌ A "use Kubernetes and Kafka" hackathon — you're building the distributed logic yourself
- ❌ A speed contest — thoughtful design beats fast coding
- ❌ A memorization test — you don't need to know algorithms by heart
- ❌ Intimidating — if you've never built distributed systems, that's fine. You'll learn more in 6 hours than in a semester.

---

## Preparation Checklist

Do these **before Sunday** so you don't waste hackathon time on setup:

### Must Do ✅

- [ ] **Set up a cloud account** (if you don't have one already)
  - **Google Cloud** (recommended): [cloud.google.com](https://cloud.google.com) — $300 free credits for new accounts
  - **AWS**: [aws.amazon.com](https://aws.amazon.com) — free tier (750 hrs/month of t3.micro)
  - **Azure**: [azure.microsoft.com/en-us/free/students](https://azure.microsoft.com/en-us/free/students/) — $100 free with `.edu` email
  - ⚠️ **Do this NOW.** Credit card verification can take time. Don't do this Sunday morning.

- [ ] **Install Docker Desktop** on your laptop
  - [docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop/)
  - After installing, make sure you can run: `docker run hello-world`

- [ ] **Make sure you can spin up a cloud VM and SSH into it**
  - Spin up one small VM (e2-micro on GCP, t3.micro on AWS, B1s on Azure)
  - SSH into it: `ssh user@<vm-ip>`
  - Install Docker on it: follow your cloud provider's guide
  - Then **shut it down** so you don't burn credits
  - If you can do this, you're ready.

- [ ] **Pick your programming language** and make sure you know how to:
  - Start an HTTP server (handle GET/POST requests)
  - Open a TCP connection to another process
  - Run multiple goroutines / threads / async tasks
  - Read/write JSON

- [ ] **Set up a GitHub repo** — you'll push your code here for submission

### Should Do 📖

Read **at least one** of these. Seriously — even 10 minutes of reading will give you a massive head start:

| What | Time | Link |
|------|------|------|
| Notes on Distributed Systems for Young Bloods | 10 min | [somethingsimilar.com/2013/01/14/notes-on-distributed-systems-for-young-bloods](https://www.somethingsimilar.com/2013/01/14/notes-on-distributed-systems-for-young-bloods/) |
| The CAP FAQ | 15 min | [the-paper-trail.org/page/cap-faq](https://www.the-paper-trail.org/page/cap-faq/) |
| DDIA Chapters 5 & 8 | 2 hrs | [dataintensive.net](https://dataintensive.net/) — "Replication" and "The Trouble with Distributed Systems" |
| Martin Kleppmann's lectures 1-3 | 3 hrs | [YouTube playlist](https://www.youtube.com/playlist?list=PLeKd45zvjcDFUEv_ohr_HdUFe97RItdiB) |

> **If you only have 10 minutes**, read the "Notes on Distributed Systems for Young Bloods." It's the single best short intro.

---

## Concepts Worth Knowing

You don't need to be an expert in any of these — but having heard of them will help:

- **Replication** — storing the same data on multiple machines. Why? What can go wrong?
- **Consistency** — when you write data on one machine, when do other machines see it? Immediately? Eventually? Never?
- **CAP Theorem** — you can't have everything. What are you willing to give up?
- **Failure detection** — how do you know if another server is dead vs just slow?
- **Heartbeats** — periodic "I'm alive" messages between servers

Don't worry if these are new. The hackathon starts with a **30-minute primer talk** that covers the essentials.

---

## Language Tips

Not sure what to use? Here's a quick guide:

| Language | Strengths | Considerations |
|----------|-----------|----------------|
| **Go** | Built for this. Goroutines make concurrency trivial. Great HTTP/TCP stdlib. | If you know Go, use Go. |
| **Java** | Solid threading. Lots of libraries. | Verbose but reliable. |
| **Python** | Fast to prototype. Easy HTTP with Flask/FastAPI. | GIL limits parallelism — use `asyncio` or `multiprocessing`. |
| **Rust** | Prevents data races at compile time. | Only if you already know Rust. Don't learn Rust during a hackathon. |
| **Node.js** | Good for many connections. | Single-threaded — be careful with CPU-bound work. |
| **C++** | Fast. | Debugging distributed C++ in 6 hours is... ambitious. |

---

## Warm-Up Exercise (Optional, but Recommended)

If you want to come prepared, try this **30-minute exercise** on your laptop:

> **Build two processes that talk to each other.**
>
> 1. Process A starts an HTTP server on port 8080
> 2. Process B starts an HTTP server on port 8081
> 3. Process A sends a message to Process B via HTTP POST
> 4. Process B acknowledges the message
> 5. Now add a heartbeat: every 2 seconds, A pings B and B pings A. Log when a ping is missed.
> 6. Kill Process B. What does Process A see? How long until it notices?
>
> If you can do this, you have the building blocks for Sunday.

---

## Questions?

Reach out to Arihant. See you Sunday. Come hungry (for knowledge and lunch).

---

*Don't overthink prep. Show up curious, bring your laptop, have Docker and a cloud account ready. The rest we'll figure out together.*
