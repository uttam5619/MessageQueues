# Kafka POST Load Test

This document records the latest load test of `POST http://localhost:3040/kafka/post` and provides the command to reproduce it.

## Reproduce the test

Prerequisites:

- Docker with Compose and the `grafana/k6` image available.
- The Kafka and MongoDB services running.
- The Kafka application running and listening on port `3040`.

From the repository root, run this in PowerShell:

```powershell
docker compose -f .\kafka\docker-compose.yml --profile load-test run --rm `
  -e LOAD_TEST_RATE=2000 `
  -e LOAD_TEST_DURATION=150s `
  -e LOAD_TEST_PRE_ALLOCATED_VUS=6000 `
  -e LOAD_TEST_MAX_VUS=10000 `
  k6 run --summary-trend-stats 'avg,min,med,max,p(90),p(95),p(99)' /scripts/test.js
```

k6 prints live progress and the final summary to the terminal. The test script is [`src/test.js`](./src/test.js). It sends JSON POST requests and checks that each response has status `202`. Its pass thresholds are a request failure rate below 1% and p95 latency below 1,000 ms.

## Recorded result

Run date: **2026-10-17**. The test was configured for 2,000 iterations per second over 150 seconds (a target of 300,000 iterations).

| Metric | Result |
| --- | ---: |
| Completed requests | 299,999 |
| Average request rate | 1,998.06 requests/second |
| Dropped iterations | 0 |
| Responses accepted with HTTP 202 | 276,947 (92.31%) |
| Failed HTTP status checks | 23,052 (7.68%) |
| Average response time | 661.54 ms |
| Median response time | 436.34 ms |
| p95 response time | 1.89 s |
| p99 response time | 2.43 s |
| Maximum response time | 9.51 s |

**Outcome: failed.** The target request volume and rate were reached with no dropped iterations, but both configured thresholds were exceeded: the 7.68% failure rate was above 1%, and p95 latency of 1.89 seconds was above 1 second. k6 therefore exited with a threshold-failure status. Results can vary between runs depending on the machine and the state/capacity of the application, Kafka, and MongoDB.