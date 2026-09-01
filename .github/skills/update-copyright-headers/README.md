# update-copyright-headers Skill

A Copilot skill that checks and updates the Progress Software copyright header in this repo's Java source files, computing the year deterministically from each file's own git history:

```
Copyright (c) 2011-<year> Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
```

## Prerequisites

- Copilot CLI agent with access to this repo
- `bash` (3.2+) and `git` — no other runtime or dependency

## What it does

1. Checks all tracked files, plus changed and untracked files by default, and derives each file’s <year> from its Git history. Pure year-bump commits are ignored.
2. Reports every `NEEDS_UPDATE` and `MISSING` file with its current and expected header line, giving you the full list before any confirmation prompt.
3. Waits for confirmation in a subsequent turn before modifying any files. It never stages, commits, or pushes changes automatically.
4. Commits changes only when explicitly requested.

## How to use

Ask Copilot in natural language. The skill activates on keywords like **"copyright header"**, **"copyright notice"**, or **"copyright year"**, or before finishing a commit or PR.

**Examples:**

| What you want | What to say |
|---|---|
| Check repository headers | *"Check copyright headers"* |
| Check the whole repo | *"Check copyright headers on all files"* |
| Update after reviewing the list | *"Update copyright headers, I've reviewed the list"* |
| Update and commit | *"Update copyright headers and commit as MLE-1234 for the 11.4.0 release"* |
| Force the skill by name | *"Use the /update-copyright-headers skill to check headers"* |


## What's excluded

- `.github/*`, `README.md`, `CONTRIBUTING.md` — config/docs, not source
- `LICENSE.txt`, `NOTICE.txt` — carry their own required legal text, not this tool's to rewrite
- `src/test/*` — this repo's test sources are conventionally headerless
- `src/main/java/com/marklogic/mapreduce/test/*` — legacy test helper sources are conventionally headerless
- `src/main/java/com/marklogic/contentpump/test/*` — legacy test helper sources are conventionally headerless
- Dotfiles, and any extension other than `.java` (unless named explicitly on the command line)

Each exclusion is commented with its rationale next to `FILESEXCLUDED`/`FILEEXTENSIONS` near the top of `scripts/copyright_headers.sh`.

## Running the script directly

You don't need Copilot for this — `scripts/copyright_headers.sh` also works as a plain, standalone script, run from this directory:

```bash
scripts/copyright_headers.sh check                        # all tracked + changed/untracked files
scripts/copyright_headers.sh check --all                  # same full repository scope, explicit
scripts/copyright_headers.sh check path/to/File.java       # specific file(s)
scripts/copyright_headers.sh update [--all] [path ...]     # same, but rewrites headers in place
scripts/copyright_headers.sh --help                        # full flag reference
```

## Files

| File | Purpose |
|---|---|
| `SKILL.md` | Instructions for Copilot (agent prompt) |
| `scripts/copyright_headers.sh` | Script invoked by the skill |
| `README.md` | This file (instructions for humans) |
