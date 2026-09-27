#!/bin/bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
classifier="$script_dir/ci-change-classifier.sh"
aggregator="$script_dir/ci-images-aggregate.sh"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/nonrepo"

pass=0
fail=0
case_n=0

report() {
  local status="$1" desc="$2" detail="${3:-}"
  if [ -n "$detail" ]; then
    echo "$status: $desc ($detail)"
  else
    echo "$status: $desc"
  fi
  if [ "$status" = "PASS" ]; then
    pass=$((pass + 1))
  else
    fail=$((fail + 1))
  fi
}

check_classifier() {
  local desc="$1" expected="$2" content="$3"
  local f out rc
  case_n=$((case_n + 1))
  f="$work/files-$case_n.txt"
  printf '%s' "$content" > "$f"
  out="$(env -u GITHUB_OUTPUT bash "$classifier" --files-from "$f" 2>"$work/err-$case_n.log")" && rc=0 || rc=$?
  if [ "$rc" -eq 0 ] && [ "$out" = "$expected" ]; then
    report PASS "classifier: $desc"
  else
    report FAIL "classifier: $desc" "rc=$rc out=[$out] want=[$expected]"
  fi
}

check_classifier_error() {
  local desc="$1"
  shift
  local out rc
  out="$(cd "$work/nonrepo" && bash "$classifier" "$@" 2>/dev/null)" && rc=0 || rc=$?
  if [ "$rc" -ne 0 ] && ! printf '%s' "$out" | grep -q 'images_required='; then
    report PASS "classifier fail-closed: $desc"
  else
    report FAIL "classifier fail-closed: $desc" "rc=$rc out=[$out]"
  fi
}

check_aggregator() {
  local desc="$1" expected="$2"
  shift 2
  local rc
  bash "$aggregator" "$@" >/dev/null 2>&1 && rc=0 || rc=$?
  if [ "$rc" -eq "$expected" ]; then
    report PASS "aggregator: $desc"
  else
    report FAIL "aggregator: $desc" "rc=$rc want=$expected"
  fi
}

git_setup() {
  local repo="$1"
  git init -q "$repo"
  git -C "$repo" config user.email "ci@localhost"
  git -C "$repo" config user.name "ci"
  mkdir -p "$repo/docs"
  printf 'readme\n' > "$repo/README.md"
  printf 'ok\n' > "$repo/docs/ok.md"
  git -C "$repo" add -A
  git -C "$repo" commit -q -m "docs baseline"
  printf 'cafe\n' > "$repo/docs/café.md"
  git -C "$repo" add -A
  git -C "$repo" commit -q -m "non-ascii docs"
  git -C "$repo" rev-parse HEAD~1
}

check_classifier_gitmode() {
  local desc="$1" expected="$2"
  case_n=$((case_n + 1))
  local repo base out rc
  repo="$work/gitrepo-$case_n"
  base="$(git_setup "$repo" 2>"$work/err-gitmode-$case_n.log")" && rc=0 || rc=$?
  if [ "$rc" -ne 0 ]; then
    report FAIL "classifier git-mode: $desc" "git setup rc=$rc"
    return
  fi
  out="$(cd "$repo" && env -u GITHUB_OUTPUT bash "$classifier" --base-ref "$base" 2>"$work/err-gitmode-$case_n.log")" && rc=0 || rc=$?
  if [ "$rc" -eq 0 ] && [ "$out" = "$expected" ]; then
    report PASS "classifier git-mode: $desc"
  else
    report FAIL "classifier git-mode: $desc" "rc=$rc out=[$out] want=[$expected]"
  fi
}

check_classifier "README.md" "images_required=false" "README.md"
check_classifier "docs plus nested README" "images_required=false" "docs/repository-settings.md
internal/foo/README.md"
check_classifier "docs to code rename" "images_required=true" "docs/a.md
x.go"
check_classifier "Dockerfile" "images_required=true" "Dockerfile"
check_classifier ".dockerignore" "images_required=true" ".dockerignore"
check_classifier "Makefile" "images_required=true" "Makefile"
check_classifier "go.mod" "images_required=true" "go.mod"
check_classifier "CI workflow" "images_required=true" ".github/workflows/ci.yaml"
check_classifier "hack script" "images_required=true" "hack/verify-courier-go-runtime.sh"
check_classifier "chart values" "images_required=true" "charts/courier/values.yaml"
check_classifier "unknown path" "images_required=true" "src/some/new/unknown/path.txt"
check_classifier "empty file" "images_required=true" ""
check_classifier "path with space" "images_required=false" "docs/my notes.md"

check_classifier_error "unreadable files-from" --files-from /nonexistent/missing
check_classifier_error "no mode flag"
check_classifier_error "unresolvable base-ref" --base-ref refs/heads/nope-nope
check_classifier_error "both mode flags" --base-ref main --files-from "$work/nonrepo/both.txt"

check_classifier_gitmode "non-ascii docs-only" "images_required=false"

gh_out="$work/github_output.txt"
gh_files="$work/gh-files.txt"
printf '%s\n' "README.md" > "$gh_files"
gh_out_content="$(GITHUB_OUTPUT="$gh_out" bash "$classifier" --files-from "$gh_files" 2>/dev/null)" && gh_rc=0 || gh_rc=$?
if [ "$gh_rc" -eq 0 ] && grep -qx 'images_required=false' "$gh_out"; then
  report PASS "classifier GITHUB_OUTPUT append"
else
  report FAIL "classifier GITHUB_OUTPUT append" "rc=$gh_rc out=[$gh_out_content]"
fi

check_aggregator "docs-only, all skipped" 0 success false skipped skipped skipped
check_aggregator "required, all succeeded" 0 success true success success success
check_aggregator "classify failed, required" 1 failure true skipped skipped skipped
check_aggregator "classify failed, not required" 1 failure false skipped skipped skipped
check_aggregator "coordinator failed" 1 success true success failure success
check_aggregator "coordinator-go skipped" 1 success true success success skipped
check_aggregator "manager ran, skip expected" 1 success false skipped success skipped
check_aggregator "jobs ran, skip expected" 1 success false success success success
check_aggregator "empty images_required" 1 success "" skipped skipped skipped
check_aggregator "classify cancelled" 1 cancelled true skipped skipped skipped
check_aggregator "classify skipped" 1 skipped false skipped skipped skipped
check_aggregator "required, job neutral" 1 success true success neutral success

agg_rc=0
bash "$aggregator" success true success success >/dev/null 2>&1 || agg_rc=$?
if [ "$agg_rc" -ne 0 ]; then
  report PASS "aggregator: wrong arg count"
else
  report FAIL "aggregator: wrong arg count" "rc=$agg_rc"
fi

echo
echo "total: $((pass + fail)) passed: $pass failed: $fail"
if [ "$fail" -gt 0 ]; then
  exit 1
fi
