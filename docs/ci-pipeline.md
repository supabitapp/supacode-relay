# CI pipeline

The measured end-to-end speedup is inconclusive. Static checks overlap race tests on the existing runner, but two branch attempts do not establish a shorter PR wait.

Measured on 2026-10-12 at [01b8b4b](https://github.com/supabitapp/supacode-relay/commit/01b8b4bdf40192bd1fa2c46414c29bd8f17a6885). The [CI workflow](../.github/workflows/ci.yml) runs `make -j2 test` and an independent vulnerability audit. The [deployment workflow](../.github/workflows/deploy.yml) waits for reusable CI before building, installing, and probing production.

## Before and after

Wall time spans `run_started_at` to the last job's `completed_at`, excluding the delay before GitHub starts the workflow. Runner time sums job durations, including runner startup and cleanup. Percentiles select sorted index `floor((n - 1) * p)`, matching the ci-speedup timing script. With two samples, both reported percentiles select the shorter attempt; the range and mean expose the slower attempt.

| Metric | Before samples | Before median / p90 | After samples | After median / p90 | Before range | After range |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| PR wall time | 4 | 38s / 39s | 2 | 38s / 38s | 34-41s | 38-60s |
| Test job | 4 | 37s / 37s | 2 | 35s / 35s | 33-39s | 35-58s |
| Test recipe | 4 | 20s / 23s | 2 | 20s / 20s | 19-24s | 20-21s |
| Audit job | 4 | 17s / 18s | 2 | 19s / 19s | 17-22s | 19-23s |
| Total runner time | 4 | 55s / 55s | 2 | 58s / 58s | 54-56s | 58-77s |
| Main, reusable CI layout | 7 | 93s / 95s | 0 | Unmeasured | 83-96s | Unmeasured |
| Main, broader historical layouts | 20 | 121s / 139s | 0 | Unmeasured | 81-179s | Unmeasured |

Observed median wall-time saving: **0s (0%)**. Projected saving: **3s (7.9%)**, from 38s to 35s, with 0.05 fewer runner-minutes. The observed mean wall time rose from 38s to 49s. Median runner time rose by 3s (0.05 runner-minutes, 5.5%); no cost saving is established. Job count, runner label, and maximum job concurrency are unchanged. Dollar impact depends on the existing runner contract and is unmeasured.

| PR run | Attempt | Configuration | Wall | Test recipe | Runner time |
| --- | ---: | --- | ---: | ---: | ---: |
| [38071983069](https://github.com/supabitapp/supacode-relay/actions/runs/38071983069) | 1 | Serial | 34s | 19s | 55s |
| [38073300467](https://github.com/supabitapp/supacode-relay/actions/runs/38073300467) | 1 | Serial | 41s | 20s | 56s |
| [38128017354](https://github.com/supabitapp/supacode-relay/actions/runs/38128017354) | 1 | Serial | 39s | 24s | 55s |
| [38149987794](https://github.com/supabitapp/supacode-relay/actions/runs/38149987794) | 1 | Serial | 38s | 23s | 54s |
| [38322295330](https://github.com/supabitapp/supacode-relay/actions/runs/38322295330/attempts/1) | 1 | Parallel | 60s | 20s | 77s |
| [38322295330](https://github.com/supabitapp/supacode-relay/actions/runs/38322295330/attempts/2) | 2 | Parallel | 38s | 21s | 58s |

Only five historical PR run IDs existed when sampled. Run [38073759005](https://github.com/supabitapp/supacode-relay/actions/runs/38073759005) mixed a failed attempt with a partial rerun and is excluded. The two after samples rerun the same SHA, so they are not independent commits. Earlier PRs also contain different application revisions. Twenty comparable PR samples and a controlled comparison are unavailable.

The seven comparable main runs are [38073286869](https://github.com/supabitapp/supacode-relay/actions/runs/38073286869), [38073369368](https://github.com/supabitapp/supacode-relay/actions/runs/38073369368), [38074333567](https://github.com/supabitapp/supacode-relay/actions/runs/38074333567), [38129151881](https://github.com/supabitapp/supacode-relay/actions/runs/38129151881), [38145300298](https://github.com/supabitapp/supacode-relay/actions/runs/38145300298), [38150269233](https://github.com/supabitapp/supacode-relay/actions/runs/38150269233), and [38251983941](https://github.com/supabitapp/supacode-relay/actions/runs/38251983941). The broader reference includes the 20 most recent successful deploy runs at measurement time and spans earlier workflow layouts. No branch measurement deploys production.

## Critical path and verification

The PR critical path remains `test`, with `audit` independent. Main follows `ci / test -> deploy`. Make's `test` target requires both `check` and `test-race`; either failure still fails the existing check. Formatting, module tidiness, both vet commands, and `go test -race -count=1 -shuffle=on ./...` retain their scope. PR cancellation, deploy serialization, and production probes retain their guards.

The representative baseline is run 38149987794. Entries below show seconds from workflow start and step duration. Runner allocation includes the interval before the first step, which exceeds the API's nominal queue wait.

| Test stage | Class | Before offset / duration | After attempt 1 offset / duration | After attempt 2 offset / duration |
| --- | --- | ---: | ---: | ---: |
| Workflow start to first step | Gating | 0s / 7s | 0s / 22s | 0s / 8s |
| Set up job | Setup | 7s / 0s | 22s / 2s | 8s / 1s |
| Checkout | Setup | 7s / 1s | 24s / 5s | 9s / 0s |
| Go setup and cache restore | Setup | 8s / 5s | 29s / 10s | 9s / 6s |
| Make test recipe | Toolchain and execution | 13s / 23s | 39s / 20s | 15s / 21s |
| Cleanup through job completion | Setup | 36s / 2s | 59s / 1s | 36s / 2s |

The baseline log spends about 3.2s on static checks before starting the race suite; its E2E package reports 17.213s. In attempt 1, `go test` starts at 23:28:55.739 UTC, and both vet commands start before its final output at 23:29:15.022 UTC. Attempt 2 repeats that overlap and reports 17.016s for E2E. The command log confirms the parallel lever fired on both attempts.

Attempt 1 delays the recipe by 26s, of which 15s precede the first step and 11s occur in setup. Attempt 2 still starts the recipe 2s later than the representative baseline. Its recipe saves 2s while total wall time matches the baseline. These setup shifts and test variation prevent attributing an end-to-end saving within 20% of the prediction.

Keep the parallel layout: it removes a proven serial dependency, preserves all checks, and shows no recipe slowdown across these samples. The slower first attempt is explained by setup timestamps rather than a longer recipe. Re-measure with the next comparable PR runs before claiming a gain or adding another lever.

Three local serial runs and three parallel runs passed. Their median durations were 19.502s and 17.114s, respectively; local results are validation, not the CI after column. `make audit`, `make build`, actionlint with ShellCheck, and whitespace validation also passed. Both branch attempts passed `test` and `audit`.

## Trend and remaining levers

| Measurement | Tracked test files | After samples | Median PR time | p90 | Minutes per 100 files |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2026-10-12 | 32 | 2 | 0:38 | 0:38 | 1.98 |

The count comes from `git ls-tree -r --name-only 01b8b4b` filtered to `_test.go`, including tagged and opt-in files. One measurement cannot establish weekly growth. Re-measure after 20 comparable successful PR runs, at 40 tracked test files, or when median PR time exceeds 60s.

Additional test jobs would duplicate roughly 13s of startup and setup in the representative baseline. Sharding waits until execution dominates that tax and repeated samples justify it. Runner changes require an unchanged-pipeline comparison and approval for increased spending. Deploy optimization waits for measurements after merge; production installation and smoke checks remain mandatory. Cache changes wait for evidence that restore or save dominates the path.

To reproduce, fetch each run and its attempt-specific jobs through `gh api repos/supabitapp/supacode-relay/actions/runs/<id>` and `gh api repos/supabitapp/supacode-relay/actions/runs/<id>/attempts/<attempt>/jobs`. Calculate durations from timestamps, drop skipped jobs, and apply the percentile index above. Fetch test logs with `gh run view <id> --repo supabitapp/supacode-relay --attempt <attempt> --log`.
