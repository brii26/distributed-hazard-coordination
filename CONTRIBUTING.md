# Contributing

## Branching

- `main` is protected: no direct pushes, every change lands via a pull request, and at least one other teammate must approve before merge. (Since nobody can approve their own PR, this always needs a second person.)
- Branch names: `<issue-number>-<short-slug>` for work tied to a single issue, e.g. `12-auth-login-refresh-rpcs`. For work spanning several issues at once (see "Multiple issues in one branch" below), use a short topic name instead: `feat/<topic>`, e.g. `feat/aggregator-mapping`.

## Pull requests

**Title:** prefer `[#<issue-number>] <short description>` when the PR is driven by a tracked issue, e.g. `[#16] Independent single-in-flight pollers`. This is a convention, not a CI-enforced format — nothing blocks a merge if you don't follow it.

Free-form titles are fine and expected for anything that doesn't map to a tracked issue: hotfixes, emergency patches, small cleanups you noticed in passing. Use `[hotfix]` as a prefix for anything urgent enough to want jumping the normal backlog order, e.g. `[hotfix] Fix panic in aggregator poller on empty response`.

**Body:** link the issue(s) it addresses using GitHub's closing keywords so the issue auto-closes on merge and the link shows up on both sides:

```
Closes #16
```

Use `Refs #16` instead of `Closes #16` if the PR only partially addresses an issue (issue stays open after merge).

**Multiple issues in one branch:** if a branch's work naturally covers more than one issue (e.g. #17 and #18, the seismic and volcanic mapping — same code path, awkward to split), that's fine. Name the PR after the overarching change, not either issue individually, e.g. `[#17,#18] HazardEvent mapping for both sources`, and list every issue it closes in the body:

```
Closes #17
Closes #18
```

**Reviews:** every PR needs ≥1 approval before merge (branch protection enforces this). CI (`gofmt`, `go vet`, `go test`, `golangci-lint`, Docker build — see `.github/workflows/ci.yml`) must also pass; these are required status checks, not optional.

## Dependency order

Before starting an issue, check `doc/BACKLOG_DEPENDENCIES.md` (or the issue's own "Blocked by" field on GitHub) — issues are linked with real dependency relationships, not just prose. If an issue you want is blocked, either pick something from the "ready now" list instead, or coordinate with whoever owns the blocker.

## Stacked PRs

If you want to start issue B while issue A's PR (which B depends on) is still open and unreviewed, don't wait — branch B off A's branch instead of off `main`:

```
git checkout -b 18-volcanic-mapping 17-seismic-mapping
```

Open B's PR targeting A's branch (not `main`). Note in B's PR description that it's stacked, e.g. "Stacked on #<A's PR number> — review that first." When A merges into `main`, retarget B's PR to `main` (GitHub usually does this automatically once the base branch is gone) and it'll only show B's actual diff from then on.

Keep stacks shallow — two or three deep at most. If a stack is growing past that, it's usually a sign the underlying issue breakdown needs splitting further, not that the stack needs to get taller.
