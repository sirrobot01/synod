# Contributor Instructions

These instructions apply to every agent and every model that works on this repository.
Read the whole file before you change anything.

## Start here

The plan lives only in the main checkout, at these absolute paths.
It is not tracked by Git, so a worktree or a clone does not contain it.
Read it from these paths even when you work in a worktree.

| Path | Purpose |
| --- | --- |
| `/Users/mukhtarakere/cools/synod/tmp/docs/00-product-brief.md` | Product direction. It wins over every other document. |
| `/Users/mukhtarakere/cools/synod/tmp/docs/engineering.md` | Code, test, and completion rules. |
| `/Users/mukhtarakere/cools/synod/tmp/tasks/TASKS.md` | The one task tracker: board, notes, and done log. |
| `/Users/mukhtarakere/cools/synod/tmp/tasks/HOSTS.md` | The real Linux hosts for end-to-end proof. |
| `/Users/mukhtarakere/cools/synod/tmp/design/` | The approved web UI design. |

Never create a `tmp/` plan inside a worktree. Never copy the plan into Git.
If these paths do not exist, stop and ask the user for them.

## Direction

Synod backs up the Docker apps of a home lab and restores them on any device.
Devices can be amd64 or arm64, and some are only online part of the time.
The repository and its password must be enough to restore everything.
Restore is the product. A backup that was never restored is only a claim.
Before you add a feature, check that it serves the brief.
If a task pulls away from the brief, stop and ask the user.
Rewriting, re-engineering, and moving code are allowed. Staying on course matters most.

## Keep going

Do not ask the user what to do next when the plan answers it.
After you finish a task, take the next one from `TASKS.md` and start it.
Do not ask "Should I start T05 now?". Start it.
Ask the user only for a real decision: a product or design choice that the brief leaves open, a conflict with the brief, an action that is destructive or visible outside this machine, or a claim that looks abandoned.
When you ask, give your recommendation, and continue with other unblocked work while you wait.

## Parallel work

Several agents, on different models, can work at the same time.
Follow these rules so that no two agents collide.

1. Take one `todo` task whose needs are done, or the task that the user names. Work on one task at a time.
2. Claim it in `TASKS.md` before you change code: set `doing` and your agent and model name.
3. Do not take a task that another agent owns. Ask the user when a claim looks abandoned.
4. Use a worktree only for a large feature or fix: `git worktree add /Users/mukhtarakere/cools/synod-worktrees/TNN -b tNN main`.
   Make small changes, such as documentation, README edits, and configuration, directly on `main`.
5. In a worktree, change code only in that worktree. Change plan files only at the main checkout paths above.
6. Shared contracts are proto files, migrations, `go.mod`, API shapes, and shared domain types. Before you change one, check the board for another `doing` task that changes it. If one exists, stop and ask the user.
7. Before a merge, rebase on `main` and run the task's checks again in your worktree.
8. Merge from the main checkout with `git merge --ff-only tNN`. Then remove the worktree and the branch.
9. When you stop, write the exact next action in the task's note. Clear the owner if you will not continue.

## Code

Follow the language's standard practices.
For Go, prefer early returns, small interfaces, and ordinary error values.
Use the Modern Go Guidelines skill or tool when it is available.
Use established libraries when they simplify implementation or maintenance.
Cobra is the selected CLI command library. Chi is permitted for API routing.
Use the standard library when it fits the requirement.
Do not add abstractions, generics, or layers without a concrete need.
Give an exported declaration a one-line doc comment when it adds information.
A comment that explains what a method does is good code.
Do not comment a declaration whose name already states what it does.
Do not restate the code in prose and do not narrate an implementation line by line.
Create a helper only when necessary.
Use the standard library directly when it provides the required function.
Run `gofmt` on every Go file you change.

## Pre-release schema and contracts

Synod has no released version. There are no installations to upgrade.
Therefore no change needs backward compatibility with an earlier Synod build.

Edit the migration that creates an object. Do not add a migration that alters it.
Keep one migration for each subsystem and keep the set small.
An ALTER is correct only when it adds a reference to an object that a later migration creates.
Record that reason in the migration.
Change a persisted format, a protocol message, or an API shape in place.
Delete a field that no longer serves the design. Do not deprecate it.
Regenerate protobuf code with `task proto`. Check that an unchanged `.proto` produces no diff first.

A changed migration fails the checksum check on an existing database.
That is expected. Drop the development database with `task dev:reset` and apply the migrations again.
Never write a data-preserving migration for development data.

This section ends at the v0.1 release.

## Validation

Test meaningful success and failure behaviour at the appropriate boundary.
Use real dependencies for critical transaction and orchestration tests.

Keep feature work fast. The owner decides when heavy checks run.

- During feature work, run `go build ./...`, `gofmt`, `go vet`, and the fast tests of the changed packages.
- Fast tests are the tests that run without `SYNOD_TEST_DOCKER_HOST` and without MinIO. Docker and MinIO tests skip themselves when these are not set.
- Write tests for new behaviour. You do not need to run the heavy tests for each feature.
- Run Docker tests, MinIO tests, and the full suite only when the owner asks for them. The owner usually asks before a release.
- GitHub Actions runs the fast checks on each push. The owner can start the heavy tests from the **test** workflow.
- Do not add proof, benchmark, or helper scripts to the repository. Use Go tests and the GitHub workflows.
- A release is a version tag. GoReleaser builds and publishes it. Push a tag only when the owner asks.
- A task is done when its code builds and its fast checks pass. Record "heavy checks not run" in the done log.
- Run tests with `-race -p 1`. Without `-p 1`, packages compete for Docker and fail on timing.
- Controller tests use SQLite files in temporary directories. Run `go test -race -p 1 -count=1 <packages>`.
- For a Docker test, set `SYNOD_TEST_DOCKER_HOST=unix://<docker socket>`. Use disposable containers and free ports. Use `-run` to select heavy tests.
- Never change the tree while a `go test` run is in flight. Never use `git stash` to get a baseline. Use a separate worktree at the base revision.
- A failure that also happens on the base revision is not yours. Record it in the task note in `TASKS.md`.
- Enrollment tests in `internal/api` can fail on timing under heavy load. Run a failed timing test again alone before you draw a conclusion.
- For web changes, run `npx svelte-check`, `npx vitest run`, and `npm run build` in `web/`. The `dist` output is committed.
- Some behaviour needs real Linux hosts: host networking, `/proc`, and mTLS between machines. Read `HOSTS.md` before you use them. Use the real hosts only when the owner asks.

## Records

Keep records short. `TASKS.md` explains the format.
Record changed behaviour, commands, results, and remaining limits in the done log.
Do not mark a check passed before it finishes.
Do not mark a task done before its checks pass on the integrated result.

## Commits

Commit each completed task or milestone with a short imperative message.
Stage only the files that belong to that task.
Commit or push only when the task's checks pass.

## Technical documentation

Follow ASD-STE100.
Use short sentences, active voice, and simple terms.
Give one instruction per sentence.
Document actual behaviour and state unverified requirements clearly.
