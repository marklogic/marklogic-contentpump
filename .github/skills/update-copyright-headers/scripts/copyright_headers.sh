#!/usr/bin/env bash
#
# Deterministic checker/updater for Progress Software copyright headers in
# this repository (marklogic/marklogic-contentpump).
#
# Expected header sentence:
#   Copyright (c) <startyear>-<endyear> Progress Software Corporation and/or
#   its subsidiaries or affiliates. All Rights Reserved.
#
# <startyear> is fixed at STARTYEAR below. <endyear> is computed per file as
# the year of that file's last REAL commit: the most recent commit that
# changed something other than the copyright line itself. Commits whose only
# change is the copyright line (a year bump, a wording/rebrand fix, ...) are
# skipped when looking for that commit, and uncommitted edits mean the file
# is "being touched now", so <endyear> is simply today's year.
#
# This script is self-contained: scope (STARTYEAR / FILESEXCLUDED /
# FILEEXTENSIONS below) is fixed in this file, not read from any config file
# in the repo at runtime. See SKILL.md next to this script for usage and the
# reasoning behind these choices.
#
# Written for bash 3.2+ (the version macOS ships by default): no associative
# arrays, no `${var,,}`, no `mapfile`. Requires only bash + git.
set -u

SCAN_LINES=20  # how many leading lines to scan for an existing header

STARTYEAR=2011  # fixed range start for every header in this repo

# Paths this tool never touches, regardless of FILEEXTENSIONS. Exact match
# or simple `*` wildcard (translated to regex `.*`, so it matches across /
# too, e.g. `src/test/*` covers everything under src/test/). Dotfiles are
# always excluded too (see is_excluded).
#   .github/*             workflow/CI config, not source
#   README.md              root doc, not source
#   CONTRIBUTING.md         root doc, not source
#   LICENSE.txt             the Apache License text itself, not this repo's
#                           notice; must not be rewritten
#   NOTICE.txt              carries this repo's own copyright line using
#                           "Copyright ©" (the © symbol, not "(c)"), plus a
#                           long list of third-party notices in whatever
#                           format each upstream project used; none of that
#                           is this tool's to rewrite
#   src/test/*              this repo's actual convention: essentially none
#                           of the existing test sources carry a header
#                           (about 1 of 31 do), so adding one would be
#                           inventing a convention rather than following one
#   src/main/java/com/marklogic/mapreduce/test/*
#                           test helper sources in this legacy package are
#                           conventionally headerless as well
#   src/main/java/com/marklogic/contentpump/test/*
#                           test helper sources in this legacy package are
#                           conventionally headerless as well
FILESEXCLUDED=(
    ".github/*"
    "README.md"
    "CONTRIBUTING.md"
    "LICENSE.txt"
    "NOTICE.txt"
    "src/test/*"
    "src/main/java/com/marklogic/mapreduce/test/*"
    "src/main/java/com/marklogic/contentpump/test/*"
)

# Extensions checked in the full-repository scope; explicit file arguments
# bypass this. .java is this repo's only convention-covered source type: main
# sources are majority-headered, and every other tracked extension (.xml,
# .xqy, .sjs, .properties, ...) is 0% headered today.
FILEEXTENSIONS=(".java")

TEMPLATE_START="Copyright (c) "
TEMPLATE_MID=" Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved."

# Same sentence, matched against a lowercased line, tolerant of extra
# internal whitespace. Group 1/2 are the two years.
COPYRIGHT_RE='^copyright[[:space:]]*\(c\)[[:space:]]*([0-9]{4})-([0-9]{4})[[:space:]]*progress software corporation and/or its subsidiaries or affiliates\.[[:space:]]*all rights reserved\.$'
# Leading comment decoration: *, #, /, whitespace, plus block openers
# (<!-- for XML/HTML, (: for XQuery). Group 1 captures it, to restore as-is.
LEADING_RE='^([[:space:]]*[*#/]*[[:space:]]*(<!--|\(:)?[[:space:]]*)'
# Trailing block terminators: */, -->, :). Group 1 captures it, to restore.
TRAILING_RE='([[:space:]]*(\*/|-->|:\)))[[:space:]]*$'

# Full header block per file extension. {copyright} is replaced with the
# templated sentence; everything else is fixed Apache-2.0 boilerplate copied
# verbatim from existing repo files so newly-inserted headers are
# byte-identical in style to the ~90% of files that already carry one.
JAVA_BLOCK='/*
 * {copyright}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
'
HASH_BLOCK='# {copyright}
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
'
XML_BLOCK='<!--
    {copyright}

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
-->
'

# Statuses a checked file can end up with.
OK=OK; NEEDS_UPDATE=NEEDS_UPDATE; MISSING=MISSING; EXCLUDED=EXCLUDED; ERROR=ERROR

# ---------------------------------------------------------------------------
# Small portable helpers (bash 3.2 safe: no ${var,,}, no associative arrays)
# ---------------------------------------------------------------------------

# Sets TRIM_RESULT to $1 with leading/trailing whitespace removed.
trim() {
    local s="$1"
    s="${s#"${s%%[![:space:]]*}"}"
    s="${s%"${s##*[![:space:]]}"}"
    TRIM_RESULT="$s"
}

# Sets LOWER_RESULT to a lowercased copy of $1. Only used a handful of times
# per file (never inside the per-diff-line hot loop), so the `tr` spawn cost
# is negligible.
lower() {
    LOWER_RESULT=$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')
}

# Case-insensitive equality check without spawning a subprocess: toggles
# bash's own nocasematch option for the comparison and restores it exactly
# as found, so callers elsewhere in the script (e.g. is_excluded's path
# matching) keep their normal case-sensitive behavior.
ci_equals() {
    local a="$1" b="$2" restore=1 result=1
    shopt -q nocasematch || restore=0
    shopt -s nocasematch
    [[ "$a" == "$b" ]] && result=0
    (( restore == 0 )) && shopt -u nocasematch
    return $result
}

ci_starts_with() {
    local a="$1" prefix="$2" restore=1 result=1
    shopt -q nocasematch || restore=0
    shopt -s nocasematch
    [[ "$a" == "$prefix"* ]] && result=0
    (( restore == 0 )) && shopt -u nocasematch
    return $result
}

get_header_block() {
    # Sets HEADER_BLOCK_RESULT for extension $1, or "" if unregistered.
    case "$1" in
        .java|.js|.ts|.c|.cpp|.h|.hpp|.cs|.go|.scala|.kt) HEADER_BLOCK_RESULT="$JAVA_BLOCK" ;;
        .py|.sh|.rb|.yaml|.yml) HEADER_BLOCK_RESULT="$HASH_BLOCK" ;;
        .xml|.html|.htm) HEADER_BLOCK_RESULT="$XML_BLOCK" ;;
        *) HEADER_BLOCK_RESULT="" ;;
    esac
}

# Cleaned, lowercased text of every fixed (non-{copyright}) line in the
# header templates, e.g. "licensed under the apache license, version 2.0
# (the "license");". Derived from the same blocks used for insertion (single
# source of truth) rather than duplicated as a separate literal list.
# Includes "" — a comment-decoration-only line (bare "*", "#", ...) cleans to
# empty, whether it's a header separator or an incidental blank source line;
# either way it carries no content of its own worth calling "substantive".
BOILERPLATE_LINES=()
compute_boilerplate_lines() {
    local block line cl seen existing
    for block in "$JAVA_BLOCK" "$HASH_BLOCK" "$XML_BLOCK"; do
        while IFS= read -r line || [[ -n "$line" ]]; do
            case "$line" in *'{copyright}'*) continue ;; esac
            clean_line "$line"
            lower "$CLEAN_LINE_RESULT"
            cl="$LOWER_RESULT"
            seen=0
            for existing in "${BOILERPLATE_LINES[@]:-}"; do
                [[ "$existing" == "$cl" ]] && { seen=1; break; }
            done
            (( seen == 0 )) && BOILERPLATE_LINES+=("$cl")
        done < <(printf '%s' "$block")
    done
}

# ---------------------------------------------------------------------------
# Path exclusion
# ---------------------------------------------------------------------------

# Dotfiles always excluded; FILESEXCLUDED entries match exactly or as a
# simple `*` wildcard (translated to regex `.*`, so `dir/*` matches anything
# under `dir/` — there is no separate recursive-glob syntax).
is_excluded() {
    local relpath="$1" base pattern regex
    base="${relpath##*/}"
    [[ "$base" == .* ]] && return 0
    for pattern in "${FILESEXCLUDED[@]}"; do
        if [[ "$relpath" == "$pattern" ]]; then
            return 0
        fi
        if [[ "$pattern" == *'*'* ]]; then
            regex="^${pattern//\*/.*}\$"
            if [[ "$relpath" =~ $regex ]]; then
                return 0
            fi
        fi
    done
    return 1
}

# ---------------------------------------------------------------------------
# Header-line text handling
# ---------------------------------------------------------------------------

# Sets CLEAN_LINE_RESULT: $1 with leading/trailing comment decoration and
# surrounding whitespace stripped (mirrors Python's LEADING_RE/TRAILING_RE
# .sub('', ...) followed by .strip()).
clean_line() {
    local line="$1" mlen
    if [[ "$line" =~ $LEADING_RE ]]; then
        line="${line:${#BASH_REMATCH[1]}}"
    fi
    if [[ "$line" =~ $TRAILING_RE ]]; then
        mlen=${#BASH_REMATCH[0]}
        line="${line:0:${#line}-mlen}"
    fi
    trim "$line"
    CLEAN_LINE_RESULT="$TRIM_RESULT"
}

looks_like_copyright() {
    clean_line "$1"
    ci_starts_with "$CLEAN_LINE_RESULT" "copyright"
}

# True for a copyright sentence, a fixed license-block line, or a blank
# line. Used to decide whether a diff hunk changes only header machinery
# (inserting/updating/removing the whole block counts as one unit) rather
# than the file's actual content.
is_header_boilerplate_line() {
    clean_line "$1"
    local cleaned="$CLEAN_LINE_RESULT" bp
    ci_starts_with "$cleaned" "copyright" && return 0
    for bp in "${BOILERPLATE_LINES[@]:-}"; do
        if ci_equals "$cleaned" "$bp"; then
            return 0
        fi
    done
    return 1
}

# True if the diff changes anything besides header/copyright machinery (a
# copyright line, a fixed license-block line, or a blank line) — so
# inserting, updating, or removing the whole boilerplate block counts as one
# non-substantive unit, same as editing just the copyright line.
is_substantive_diff() {
    local diff_text="$1" line content
    while IFS= read -r line || [[ -n "$line" ]]; do
        case "$line" in
            '+++'*|'---'*) continue ;;
            '+'*|'-'*)
                content="${line:1}"
                if ! is_header_boilerplate_line "$content"; then
                    return 0
                fi
                ;;
        esac
    done <<< "$diff_text"
    return 1
}

# ---------------------------------------------------------------------------
# Git plumbing
# ---------------------------------------------------------------------------

is_tracked() {
    git -C "$1" ls-files --error-unmatch -- "$2" >/dev/null 2>&1
}

# Sets TARGET_YEAR and YEAR_REASON — the deterministic <endyear> for this
# file.
#
# 1. Untracked (new, never committed) -> today's year.
# 2. Uncommitted changes vs HEAD that touch more than the copyright line
#    -> today's year (the file is being edited right now).
# 3. Otherwise walk commit history (newest first, following renames) for the
#    most recent commit that changed more than the copyright line; use that
#    commit's author-date year.
# 4. If history has no such commit (shouldn't happen for a real source
#    file), fall back to today's year.
compute_target_year() {
    local repo_root="$1" relpath="$2" today_year="$3"
    local diff log record header patch sha year subject found=0

    if ! is_tracked "$repo_root" "$relpath"; then
        TARGET_YEAR="$today_year"
        YEAR_REASON="new/untracked file"
        return
    fi

    diff=$(git -C "$repo_root" diff HEAD -- "$relpath")
    if [[ -n "$diff" ]] && is_substantive_diff "$diff"; then
        TARGET_YEAR="$today_year"
        YEAR_REASON="uncommitted changes"
        return
    fi

    # Each commit's record is prefixed with a 0x01 byte (git's %x01), so we
    # can split the combined log+patch stream on it with `read -d`. Fields
    # within the header line are separated by 0x1f (%x1f, unit separator).
    log=$(git -C "$repo_root" log --follow -p \
        --format='%x01%H%x1f%ad%x1f%s' --date=format:%Y -- "$relpath")
    while IFS= read -r -d $'\x01' record || [[ -n "$record" ]]; do
        [[ -z "$record" ]] && continue
        header="${record%%$'\n'*}"
        patch="${record#*$'\n'}"
        IFS=$'\x1f' read -r sha year subject <<< "$header"
        if is_substantive_diff "$patch"; then
            TARGET_YEAR="$year"
            YEAR_REASON="commit ${sha:0:8} ($year): $subject"
            found=1
            break
        fi
    done <<< "$log"

    if (( found == 0 )); then
        TARGET_YEAR="$today_year"
        YEAR_REASON="no substantive history found; defaulted to current year"
    fi
}

# ---------------------------------------------------------------------------
# File line I/O (preserves CRLF-vs-LF per line and trailing-newline-or-not,
# so files this tool doesn't touch content-wise round-trip byte-identical)
# ---------------------------------------------------------------------------

# Sets FILE_LINES (array, one element per line, no trailing \n; an embedded
# trailing \r from a CRLF file is left in place) and FILE_HAS_TRAILING_NEWLINE
# (1 if the file's last byte is \n, else 0).
read_file_lines() {
    local path="$1" line
    FILE_LINES=()
    while IFS= read -r line || [[ -n "$line" ]]; do
        FILE_LINES+=("$line")
    done < "$path"
    if [[ -n "$(tail -c1 -- "$path" 2>/dev/null)" ]]; then
        FILE_HAS_TRAILING_NEWLINE=0
    else
        FILE_HAS_TRAILING_NEWLINE=1
    fi
}

write_file_lines() {
    local path="$1" n=${#FILE_LINES[@]} i
    {
        for (( i=0; i<n; i++ )); do
            if (( i == n-1 )) && (( FILE_HAS_TRAILING_NEWLINE == 0 )); then
                printf '%s' "${FILE_LINES[$i]}"
            else
                printf '%s\n' "${FILE_LINES[$i]}"
            fi
        done
    } > "$path"
}

# Sets SPLIT_LINES (array) from multi-line string $1, one element per line.
# Uses process substitution rather than a herestring: `<<<` always appends
# its own trailing newline, which would manufacture a phantom empty final
# line since $1 (a HEADER_BLOCKS template) already ends in one.
split_into_lines() {
    local text="$1" line
    SPLIT_LINES=()
    while IFS= read -r line || [[ -n "$line" ]]; do
        SPLIT_LINES+=("$line")
    done < <(printf '%s' "$text")
}

# Inserts SPLIT_LINES into FILE_LINES at index $1.
insert_lines_at() {
    local idx=$1
    FILE_LINES=( "${FILE_LINES[@]:0:idx}" "${SPLIT_LINES[@]}" "${FILE_LINES[@]:idx}" )
}

# Sets HEADER_LINE_INDEX (empty if not found) by scanning the first
# SCAN_LINES entries of FILE_LINES for an existing copyright line.
find_header_line() {
    local i max=${#FILE_LINES[@]}
    HEADER_LINE_INDEX=""
    (( max > SCAN_LINES )) && max=$SCAN_LINES
    for (( i=0; i<max; i++ )); do
        if looks_like_copyright "${FILE_LINES[$i]}"; then
            HEADER_LINE_INDEX=$i
            return
        fi
    done
}

# Sets EXPECTED_SENTENCE for target year $1.
build_expected_sentence() {
    EXPECTED_SENTENCE="${TEMPLATE_START}${STARTYEAR}-${1}${TEMPLATE_MID}"
}

# ---------------------------------------------------------------------------
# Per-file check / update
# ---------------------------------------------------------------------------

# Populates the RESULT_* globals for relpath $2 (repo root $1, current year
# $3). $4 ("force_scope"): 1 skips the FILEEXTENSIONS allow-list (used for
# files named explicitly on the command line) but still honors
# FILESEXCLUDED.
check_file() {
    local repo_root="$1" relpath="$2" today_year="$3" force_scope="$4"
    local ext="" abspath cleaned lower_cleaned target_year year_reason

    case "$relpath" in
        *.*) ext=".${relpath##*.}" ;;
    esac

    RESULT_STATUS=""; RESULT_EXPECTED=""; RESULT_CURRENT=""
    RESULT_TARGET_YEAR=""; RESULT_YEAR_REASON=""; RESULT_EXT="$ext"
    RESULT_LINE_INDEX=""; RESULT_REASON=""; RESULT_MESSAGE=""

    if is_excluded "$relpath"; then
        RESULT_STATUS="$EXCLUDED"; RESULT_REASON="matches FILESEXCLUDED / dotfile"
        return
    fi
    if [[ "$force_scope" != 1 ]]; then
        local in_scope=0 e
        for e in "${FILEEXTENSIONS[@]}"; do
            [[ "$ext" == "$e" ]] && { in_scope=1; break; }
        done
        if (( in_scope == 0 )); then
            RESULT_STATUS="$EXCLUDED"; RESULT_REASON="extension '$ext' not in scope"
            return
        fi
    fi

    abspath="$repo_root/$relpath"
    if [[ ! -r "$abspath" ]]; then
        RESULT_STATUS="$ERROR"; RESULT_MESSAGE="cannot read file"
        return
    fi
    read_file_lines "$abspath"

    compute_target_year "$repo_root" "$relpath" "$today_year"
    target_year="$TARGET_YEAR"; year_reason="$YEAR_REASON"
    build_expected_sentence "$target_year"

    find_header_line
    if [[ -z "$HEADER_LINE_INDEX" ]]; then
        RESULT_STATUS="$MISSING"; RESULT_EXPECTED="$EXPECTED_SENTENCE"
        RESULT_TARGET_YEAR="$target_year"; RESULT_YEAR_REASON="$year_reason"
        return
    fi

    clean_line "${FILE_LINES[$HEADER_LINE_INDEX]}"
    cleaned="$CLEAN_LINE_RESULT"
    lower "$cleaned"
    lower_cleaned="$LOWER_RESULT"
    if [[ "$lower_cleaned" =~ $COPYRIGHT_RE ]] \
        && [[ "${BASH_REMATCH[1]}" == "$STARTYEAR" ]] \
        && [[ "${BASH_REMATCH[2]}" == "$target_year" ]]; then
        RESULT_STATUS="$OK"
        return
    fi

    RESULT_STATUS="$NEEDS_UPDATE"; RESULT_LINE_INDEX="$HEADER_LINE_INDEX"
    RESULT_CURRENT="$cleaned"; RESULT_EXPECTED="$EXPECTED_SENTENCE"
    RESULT_TARGET_YEAR="$target_year"; RESULT_YEAR_REASON="$year_reason"
}

# Applies the update implied by the current RESULT_* globals (status must be
# NEEDS_UPDATE or MISSING) to relpath $2 under repo root $1. Sets
# UPDATE_OK (1/0) and UPDATE_ERROR.
update_file() {
    local repo_root="$1" relpath="$2" abspath="$repo_root/$2"
    read_file_lines "$abspath"
    UPDATE_OK=1; UPDATE_ERROR=""

    if [[ "$RESULT_STATUS" == "$MISSING" ]]; then
        get_header_block "$RESULT_EXT"
        if [[ -z "$HEADER_BLOCK_RESULT" ]]; then
            UPDATE_OK=0; UPDATE_ERROR="no header template registered for '$RESULT_EXT'"
            return
        fi
        local text="${HEADER_BLOCK_RESULT/\{copyright\}/$RESULT_EXPECTED}"
        split_into_lines "$text"
        local insert_at=0
        [[ "${FILE_LINES[0]:-}" == '#!'* ]] && insert_at=1
        insert_lines_at "$insert_at"

    elif [[ "$RESULT_STATUS" == "$NEEDS_UPDATE" ]]; then
        local idx=$RESULT_LINE_INDEX original stripped line_ending prefix remainder suffix mlen
        original="${FILE_LINES[$idx]}"
        line_ending=""
        stripped="$original"
        if [[ "$stripped" == *$'\r' ]]; then
            line_ending=$'\r'
            stripped="${stripped%$'\r'}"
        fi
        prefix=""
        if [[ "$stripped" =~ $LEADING_RE ]]; then
            prefix="${BASH_REMATCH[1]}"
        fi
        remainder="${stripped:${#prefix}}"
        suffix=""
        if [[ "$remainder" =~ $TRAILING_RE ]]; then
            suffix="${BASH_REMATCH[1]}"
        fi
        FILE_LINES[$idx]="${prefix}${RESULT_EXPECTED}${suffix}${line_ending}"
    else
        UPDATE_OK=0; UPDATE_ERROR="nothing to update for status $RESULT_STATUS"
        return
    fi

    write_file_lines "$abspath"
}

# ---------------------------------------------------------------------------
# Scope resolution
# ---------------------------------------------------------------------------

# Sets RELPATH_RESULT: the relative path from base dir $2 (absolute, no
# trailing slash) to absolute path $1, using `../` components as needed —
# mirrors Python's os.path.relpath for paths that fall outside repo_root
# (e.g. via a symlink), unlike a plain prefix-strip.
relpath_from() {
    local target="$1" base="$2"
    local IFS=/
    local -a t=($target) b=($base)
    local i=0 up j result=""
    while [[ $i -lt ${#t[@]} && $i -lt ${#b[@]} && "${t[$i]}" == "${b[$i]}" ]]; do
        (( i++ ))
    done
    up=$(( ${#b[@]} - i ))
    for (( j=0; j<up; j++ )); do result+="../"; done
    for (( j=i; j<${#t[@]}; j++ )); do result+="${t[$j]}/"; done
    RELPATH_RESULT="${result%/}"
}

# Sets RELPATHS (array) and FORCE_SCOPE (1/0).
resolve_scope() {
    local repo_root="$1" _use_all="$2"; shift 2
    local explicit_paths=("$@")
    RELPATHS=()

    if (( ${#explicit_paths[@]} > 0 )); then
        FORCE_SCOPE=1
        local p abs dir base resolved_dir
        for p in "${explicit_paths[@]}"; do
            if [[ "$p" = /* ]]; then abs="$p"; else abs="$(pwd)/$p"; fi
            dir="${abs%/*}"; base="${abs##*/}"
            if resolved_dir=$(cd -- "$dir" 2>/dev/null && pwd -P); then
                abs="$resolved_dir/$base"
            fi
            relpath_from "$abs" "$repo_root"
            RELPATHS+=("$RELPATH_RESULT")
        done
        return
    fi

    FORCE_SCOPE=0
    # The default scope is the full repository plus changed/untracked paths.
    # Keep the path list deduplicated because changed tracked files appear in
    # both the tracked and diff results.
    local seen_list=$'\x1e' p
    while IFS= read -r p; do
        [[ -z "$p" ]] && continue
        case "$seen_list" in *$'\x1e'"$p"$'\x1e'*) continue ;; esac
        seen_list+="$p"$'\x1e'
        RELPATHS+=("$p")
    done < <(
        git -C "$repo_root" ls-files
        git -C "$repo_root" diff --name-only HEAD
        git -C "$repo_root" ls-files --others --exclude-standard
    )
}

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

usage() {
    cat <<'EOF'
Usage: copyright_headers.sh {check|update} [--all] [--verbose] [path ...]

  check           Report header status; makes no changes. Exit 1 if any
                  file is NEEDS_UPDATE or MISSING.
  update          Same report, and rewrites each NEEDS_UPDATE/MISSING file
                  in place. Exit 1 if any file failed to update.

  --all           Explicitly request the full repository scope. This is the
                  default: all tracked files plus changed/untracked files.
  --verbose       Also list OK and EXCLUDED files.
  path ...        Specific files to process. Bypasses the FILEEXTENSIONS
                  allow-list (still honors FILESEXCLUDED).

With no path arguments, all tracked files plus changed/untracked files are
examined. `--all` is retained as an explicit spelling of that same scope.

The year rule: for each file, <endyear> is the year of that file's most
recent commit that changes something other than the copyright/license
header itself; a pure "bump the year" or "reword the notice" commit doesn't
count and is skipped when walking history. A file with real uncommitted
changes, or that's new/untracked, gets the current year.
EOF
}

main() {
    if [[ $# -eq 0 ]]; then
        usage >&2
        exit 2
    fi
    case "$1" in
        -h|--help)
            usage
            exit 0
            ;;
    esac

    local mode="$1"; shift
    case "$mode" in
        check|update) ;;
        *) echo "Error: mode must be 'check' or 'update', got '$mode'" >&2; usage >&2; exit 2 ;;
    esac

    local use_all=0 verbose=0
    local paths=()
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --all) use_all=1 ;;
            --verbose) verbose=1 ;;
            -h|--help) usage; exit 0 ;;
            --) shift; while [[ $# -gt 0 ]]; do paths+=("$1"); shift; done; break ;;
            -*) echo "Error: unknown option '$1'" >&2; usage >&2; exit 2 ;;
            *) paths+=("$1") ;;
        esac
        shift
    done

    local repo_root
    repo_root=$(git rev-parse --show-toplevel 2>/dev/null)
    if [[ -z "$repo_root" ]]; then
        echo "Error: not inside a git repository." >&2
        exit 1
    fi

    compute_boilerplate_lines
    local today_year
    today_year=$(date +%Y)

    resolve_scope "$repo_root" "$use_all" "${paths[@]}"
    local force_scope=$FORCE_SCOPE

    # Parallel arrays: one slot per examined file, indexed identically.
    local -a R_PATH R_STATUS R_CURRENT R_EXPECTED R_TARGET_YEAR R_YEAR_REASON R_EXT R_LINE_INDEX R_REASON R_MESSAGE
    local relpath count_ok=0 count_needs=0 count_missing=0 count_excluded=0 count_error=0

    for relpath in "${RELPATHS[@]}"; do
        check_file "$repo_root" "$relpath" "$today_year" "$force_scope"
        R_PATH+=("$relpath"); R_STATUS+=("$RESULT_STATUS")
        R_CURRENT+=("$RESULT_CURRENT"); R_EXPECTED+=("$RESULT_EXPECTED")
        R_TARGET_YEAR+=("$RESULT_TARGET_YEAR"); R_YEAR_REASON+=("$RESULT_YEAR_REASON")
        R_EXT+=("$RESULT_EXT"); R_LINE_INDEX+=("$RESULT_LINE_INDEX")
        R_REASON+=("$RESULT_REASON"); R_MESSAGE+=("$RESULT_MESSAGE")
        case "$RESULT_STATUS" in
            "$OK") (( count_ok++ )) ;;
            "$NEEDS_UPDATE") (( count_needs++ )) ;;
            "$MISSING") (( count_missing++ )) ;;
            "$EXCLUDED") (( count_excluded++ )) ;;
            "$ERROR") (( count_error++ )) ;;
        esac
    done

    local ext_list="" e scope_description
    for e in "${FILEEXTENSIONS[@]}"; do
        [[ -z "$ext_list" ]] && ext_list="$e" || ext_list="$ext_list,$e"
    done
    if (( force_scope == 1 )); then
        scope_description="explicit paths"
    else
        scope_description="all tracked + changed/untracked"
    fi
    echo "Copyright header $mode — ${#RELPATHS[@]} file(s) examined (scope: $scope_description; start year $STARTYEAR, extensions $ext_list)"
    echo "  OK=$count_ok  NEEDS_UPDATE=$count_needs  MISSING=$count_missing  EXCLUDED=$count_excluded  ERROR=$count_error"
    echo

    local updated=0 failed=0 i n=${#R_PATH[@]} status
    local -a order=("$NEEDS_UPDATE" "$MISSING" "$ERROR")
    local want
    for want in "${order[@]}"; do
        for (( i=0; i<n; i++ )); do
            status="${R_STATUS[$i]}"
            [[ "$status" != "$want" ]] && continue
            if [[ "$status" == "$ERROR" ]]; then
                echo "[ERROR] ${R_PATH[$i]}: ${R_MESSAGE[$i]}"
                continue
            fi
            echo "[$status] ${R_PATH[$i]}"
            [[ "$status" == "$NEEDS_UPDATE" ]] && echo "    current : ${R_CURRENT[$i]}"
            echo "    expected: ${R_EXPECTED[$i]}"
            echo "    target year ${R_TARGET_YEAR[$i]} <- ${R_YEAR_REASON[$i]}"
            if [[ "$mode" == "update" ]]; then
                RESULT_STATUS="$status"; RESULT_EXPECTED="${R_EXPECTED[$i]}"
                RESULT_EXT="${R_EXT[$i]}"; RESULT_LINE_INDEX="${R_LINE_INDEX[$i]}"
                update_file "$repo_root" "${R_PATH[$i]}"
                if [[ "$UPDATE_OK" == 1 ]]; then
                    (( updated++ ))
                    echo "    -> updated"
                else
                    (( failed++ ))
                    echo "    -> FAILED: $UPDATE_ERROR"
                fi
            fi
            echo
        done
    done

    if [[ "$verbose" == 1 ]]; then
        for want in "$OK" "$EXCLUDED"; do
            for (( i=0; i<n; i++ )); do
                [[ "${R_STATUS[$i]}" != "$want" ]] && continue
                if [[ "$want" == "$EXCLUDED" && -n "${R_REASON[$i]}" ]]; then
                    echo "[$want] ${R_PATH[$i]} (${R_REASON[$i]})"
                else
                    echo "[$want] ${R_PATH[$i]}"
                fi
            done
        done
    fi

    if [[ "$mode" == "update" ]]; then
        echo "Updated $updated file(s); $failed failure(s)."
        (( failed > 0 )) && exit 1
        exit 0
    else
        (( count_needs > 0 || count_missing > 0 )) && exit 1
        exit 0
    fi
}

main "$@"
