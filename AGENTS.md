# Contributor Instructions

## Code

Follow the language's standard practices.
For Go, prefer early returns, small interfaces, and ordinary error values.
Use the Modern Go Guidelines skill when it is available.
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

## Local task workflow

When the local development workspace is available, read `tmp/tasks/README.md`.
Read the current task and the specifications required by `tmp/docs/23-contributor-contract.md`.
Update the task record when you start work and when you stop.
Record the current state and the exact next action.
Change one task at a time.
If local specifications are absent, ask the user for the required contract.
Do not infer missing orchestration behaviour.
Keep all files under tmp ignored by Git.

## Pre-release schema and contracts

Synod has no released version. There are no installations to upgrade.
Therefore no change needs backward compatibility with an earlier Synod build.

Edit the migration that creates an object. Do not add a migration that alters it.
Keep one migration for each subsystem and keep the set small.
An ALTER is correct only when it adds a reference to an object that a later migration creates.
Record that reason in the migration.
Change a persisted format, a protocol message, or an API shape in place.
Delete a field that no longer serves the design. Do not deprecate it.

A changed migration fails the checksum check on an existing database.
That is expected. Drop the development database and apply the migrations again.
Never write a data-preserving migration for development data.

This section ends at the v0.1 release.
After that release, `tmp/docs/27-protocols-versioning.md` governs every schema and contract change.

## Validation and records

Test meaningful success and failure behaviour at the appropriate boundary.
Use real dependencies for critical transaction and orchestration tests.
Record changed behaviour, files, contracts, test commands, results, and remaining limits.
Write that record in the task file before you stop work.
Do not mark a milestone complete before review and integrated validation.

## Commits

Commit each completed milestone with a short imperative message.
Stage only the files that belong to that milestone.

## Technical documentation

Follow ASD-STE100.
Use short sentences, active voice, and simple terms.
Give one instruction per sentence.
Document actual behaviour and state unverified requirements clearly.
