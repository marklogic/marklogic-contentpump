---
name: update-copyright-headers
description: Check or update Progress Software copyright headers ("Copyright (c) 2011-xxxx Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.") in this repository's source files. Use when asked to check or update a copyright header, notice, or year, or before finishing a commit or PR. The year is computed deterministically from each file's own git history, never copied from another file or bulk-bumped to the current year.
---

# Update copyright headers

`scripts/copyright_headers.sh` checks and updates the header sentence deterministically: same file, same git history in, same year out, every run. It's a plain bash script (bash 3.2+, no other runtime needed) that only reads `git log`/`git diff` and rewrites file content; it never stages, commits, or pushes anything itself. Always let the script determine the year and make the edit; hand-editing a copyright line or guessing at a year defeats the determinism this tool exists for.

## Steps

1. **Run `check`.** Read-only and safe any time, including on your own initiative (e.g. before finishing a PR):
   ```bash
   bash "$(git rev-parse --show-toplevel)/.github/skills/update-copyright-headers/scripts/copyright_headers.sh" check [--all] [path ...]
   ```
   No arguments = all tracked files plus changed/untracked files. `--all` is the same full repository scope, retained as an explicit spelling. Explicit `path`s = just those files (still excluded by `FILESEXCLUDED`, but bypasses the `FILEEXTENSIONS` allow-list). Run `--help` for the full flag reference and the year rule's exact wording.
2. **Present the NEEDS_UPDATE/MISSING list in a standalone response, then stop.** Show every affected file with its current and expected header line (or "missing"), plus the OK/NEEDS_UPDATE/MISSING/EXCLUDED counts, and give the user an opportunity to review it. Do not call `ask_user`, request confirmation, or run `update` in this same response. The complete list must be visible before any confirmation prompt is presented. 
3. **Run `update` only after the list was shown in a prior response and the user explicitly confirms.** On the next user turn, use `ask_user` for the confirmation when the user has not already clearly approved the update after reviewing the list. Never combine the list presentation and confirmation prompt in one response. This gate applies every time, even if the original request already sounded like "fix the headers"; presenting the list is not confirmation, and running `update` on your own initiative is never appropriate.
   ```bash
   bash "$(git rev-parse --show-toplevel)/.github/skills/update-copyright-headers/scripts/copyright_headers.sh" update [--all] [path ...]
   ```
   Relay which files were actually changed. The result is an uncommitted working-tree diff; leave it there for the user to inspect (`git diff`) rather than assuming the next step.
4. **Never run `git commit` unless the user explicitly asks you to.** Checking or updating headers carries no implied permission to commit them.
5. **When the user does ask you to commit**, ask for the MLE ticket number and the release name/version if either wasn't already given, then commit with exactly:
   ```
   MLE-<ticket>: Bulk copyright update for <release> release
   ```

## The year rule

For each file, `<endyear>` is the year of that file's most recent commit that changes something other than the copyright/license header itself: a pure "bump the year" or "reword the notice" commit doesn't count and is skipped when walking history. Renames and copies are followed correctly. A file with real uncommitted changes, or that's new/untracked, gets the current year. When `<endyear>` equals `<startyear>` (2011), the header is a single year (`Copyright (c) 2011 ...`) rather than a redundant `2011-2011` range. See the script's own docstring/`--help` for the exact algorithm.

## Adjusting scope

If this repo's conventions change (a new source directory, a new language, a file that should no longer be excluded), edit `STARTYEAR`/`FILESEXCLUDED`/`FILEEXTENSIONS` directly in `scripts/copyright_headers.sh`; each entry there is commented with why it's there. Don't guess generically ("exclude tests") or invent a list from first principles. Derive changes from this repo's own precedent:
- Grep a sample of files in the candidate category for the existing header sentence to see whether that category already carries headers by convention. Exclude only categories that are genuinely conventionally headerless, not categories that merely happen to be missing a header on some files (that's a gap to report, not a convention to codify).
- Files that legitimately contain *other* copyright/license text of their own (e.g. `LICENSE.txt`, `NOTICE.txt`) belong excluded too: this tool's header match is a strict sentence match and isn't meant to police those files' own required legal text.
- Don't add exclusions for paths that already can't match (build/output directories are `.gitignore`d, and `FILEEXTENSIONS` already limits scope by type).
