#!/bin/bash

set -euo pipefail

export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1

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

git_setup_code() {
  local repo="$1"
  git init -q "$repo"
  git -C "$repo" config user.email "ci@localhost"
  git -C "$repo" config user.name "ci"
  mkdir -p "$repo/docs"
  printf 'readme\n' > "$repo/README.md"
  printf 'ok\n' > "$repo/docs/ok.md"
  git -C "$repo" add -A
  git -C "$repo" commit -q -m "docs baseline"
  printf 'package main\n' > "$repo/main.go"
  git -C "$repo" add -A
  git -C "$repo" commit -q -m "code change"
  git -C "$repo" rev-parse HEAD~1
}

check_classifier_gitmode() {
  local desc="$1" expected="$2" setup="${3:-git_setup}"
  case_n=$((case_n + 1))
  local repo base out rc
  repo="$work/gitrepo-$case_n"
  base="$("$setup" "$repo" 2>"$work/err-gitmode-$case_n.log")" && rc=0 || rc=$?
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
check_classifier "nested non-README markdown" "images_required=true" "internal/prompt/system.md"
check_classifier "top-level markdown" "images_required=false" "DESIGN.md"
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
check_classifier "blank line fails closed" "images_required=true" "docs/ok.md

"
check_classifier "path with space" "images_required=false" "docs/my notes.md"

check_classifier_error "unreadable files-from" --files-from /nonexistent/missing
check_classifier_error "no mode flag"
check_classifier_error "unresolvable base-ref" --base-ref refs/heads/nope-nope
check_classifier_error "both mode flags" --base-ref main --files-from "$work/nonrepo/both.txt"

check_classifier_gitmode "non-ascii docs-only" "images_required=false"
check_classifier_gitmode "code change fails closed" "images_required=true" git_setup_code

gh_out="$work/github_output.txt"
gh_files="$work/gh-files.txt"
printf '%s\n' "README.md" > "$gh_files"
gh_out_content="$(GITHUB_OUTPUT="$gh_out" bash "$classifier" --files-from "$gh_files" 2>/dev/null)" && gh_rc=0 || gh_rc=$?
if [ "$gh_rc" -eq 0 ] && grep -qx 'images_required=false' "$gh_out"; then
  report PASS "classifier GITHUB_OUTPUT append"
else
  report FAIL "classifier GITHUB_OUTPUT append" "rc=$gh_rc out=[$gh_out_content]"
fi

check_aggregator "docs-only, all skipped" 0 success false skipped skipped skipped skipped
check_aggregator "required, all succeeded" 0 success true success success success success
check_aggregator "classify failed, required" 1 failure true skipped skipped skipped skipped
check_aggregator "classify failed, not required" 1 failure false skipped skipped skipped skipped
check_aggregator "coordinator failed" 1 success true success failure success success
check_aggregator "coordinator-go skipped" 1 success true success success skipped success
check_aggregator "manager ran, skip expected" 1 success false skipped success skipped skipped
check_aggregator "jobs ran, skip expected" 1 success false success success success skipped
check_aggregator "not required, job neutral" 1 success false neutral skipped skipped skipped
check_aggregator "not required, job cancelled" 1 success false skipped cancelled skipped skipped
check_aggregator "empty images_required" 1 success "" skipped skipped skipped skipped
check_aggregator "unknown images_required value" 1 success TRUE skipped skipped skipped skipped
check_aggregator "nonsense images_required value" 1 success yes skipped skipped skipped skipped
check_aggregator "classify cancelled" 1 cancelled true skipped skipped skipped skipped
check_aggregator "classify skipped" 1 skipped false skipped skipped skipped skipped
check_aggregator "required, job neutral" 1 success true success neutral success success

for result in failure skipped cancelled neutral unknown ""; do
  check_aggregator "required, harness [$result]" 1 success true success success success "$result"
done
for result in success failure cancelled neutral unknown ""; do
  check_aggregator "docs-only, harness [$result]" 1 success false skipped skipped skipped "$result"
done

workflow="${1:-$script_dir/../.github/workflows/ci.yaml}"
harness_job="$work/harness-job.yaml"
awk '/^  image-harness:$/ {inside=1; next} inside && /^  [^[:space:]].*:/ {exit} inside {print}' "$workflow" > "$harness_job"
if grep -Fqx '    needs: images-classify' "$harness_job" &&
  grep -Fqx "    if: needs.images-classify.outputs.images_required == 'true'" "$harness_job"; then
  report PASS "workflow: harness uses the image classification gate"
else
  report FAIL "workflow: harness uses the image classification gate"
fi
images_job="$work/images-job.yaml"
awk '/^  images:$/ {inside=1; next} inside && /^  [^[:space:]].*:/ {exit} inside {print}' "$workflow" > "$images_job"
if grep -Fqx '    needs: [images-classify, image-manager, image-coordinator, image-coordinator-go, image-harness]' "$images_job"; then
  report PASS "workflow: images waits for all image jobs"
else
  report FAIL "workflow: images waits for all image jobs"
fi
if grep -Fqx '    if: ${{ !cancelled() }}' "$images_job"; then
  report PASS "workflow: aggregate retains cancellation semantics"
else
  report FAIL "workflow: aggregate retains cancellation semantics"
fi
awk '/^          bash hack\/ci-images-aggregate.sh/ {inside=1} inside && /^[[:space:]]*$/ {exit} inside {print}' "$images_job" > "$work/actual-command"
cat > "$work/expected-command" <<'EOF'
          bash hack/ci-images-aggregate.sh \
            '${{ needs.images-classify.result }}' \
            '${{ needs.images-classify.outputs.images_required }}' \
            '${{ needs.image-manager.result }}' \
            '${{ needs.image-coordinator.result }}' \
            '${{ needs.image-coordinator-go.result }}' \
            '${{ needs.image-harness.result }}'
EOF
if diff -u "$work/expected-command" "$work/actual-command"; then
  report PASS "workflow: aggregate receives all results in argument order"
else
  report FAIL "workflow: aggregate receives all results in argument order"
fi

agg_rc=0
bash "$aggregator" success true success success success >/dev/null 2>&1 || agg_rc=$?
if [ "$agg_rc" -eq 2 ]; then
  report PASS "aggregator: wrong arg count"
else
  report FAIL "aggregator: wrong arg count" "rc=$agg_rc"
fi

agg_rc=0
bash "$aggregator" success true success success success success success >/dev/null 2>&1 || agg_rc=$?
if [ "$agg_rc" -eq 2 ]; then
  report PASS "aggregator: too many args"
else
  report FAIL "aggregator: too many args" "rc=$agg_rc"
fi
# Every image job builds with `context: .`, so .dockerignore decides what the
# builder sees. The invariant worth enforcing is that no path the Dockerfile
# reads out of the context may be excluded, because that breaks every image
# build. `.git` is asserted excluded as well, since shipping repo history to the
# builder is the cost this file exists to avoid, and an all-empty ignore file
# would otherwise satisfy every other case below.
#
# This deliberately does NOT pin the full rule list. The set of non-build paths
# is repo layout, it changes as the tree changes, and ci-change-classifier.sh
# already forces the real image builds to run on any .dockerignore change -- so
# the expensive non-inputs are covered by those builds, not by asserting here
# that a particular directory name is still present.
dockerignore="$script_dir/../.dockerignore"
dockerfile="$script_dir/../Dockerfile"

# Paths every COPY/ADD reads from the build context, one per line, sorted and
# deduplicated. Line continuations are joined first (an unwrapped COPY loses
# sources silently), --flags are dropped rather than mistaken for sources, and
# the trailing destination is discarded. Sources are normalised to paths with no
# trailing slash so a directory source compares equal to what the ignore rules
# match against.
context_sources() {
  awk '
    {
      line = $0
      while (line ~ /\\[[:space:]]*$/) {
        sub(/\\[[:space:]]*$/, "", line)
        if ((getline nxt) <= 0) break
        line = line " " nxt
      }
      sub(/^[[:space:]]+/, "", line)
      if (line !~ /^(COPY|ADD|copy|add)[[:space:]]/) next
      # --from= reads another stage, so it contributes no context source.
      if (line ~ /--from=/) next
      sub(/^(COPY|ADD|copy|add)[[:space:]]+/, "", line)
      n = split(line, field, /[[:space:]]+/)
      m = 0
      for (i = 1; i <= n; i++) {
        if (field[i] == "" || field[i] ~ /^--/) continue
        gsub(/^["\047]|["\047]$/, "", field[i])
        src[++m] = field[i]
      }
      for (i = 1; i < m; i++) {
        s = src[i]
        sub(/\/+$/, "", s)
        print s
      }
    }
  ' "$1" | sort -u
}

# True when .dockerignore would exclude the given context path, modelling
# Docker's rules: * and ? stop at "/", a pattern matches at any depth, and
# matching a directory excludes everything beneath it. Kept deliberately
# minimal -- it understands no `**`, matching Docker closely enough for the
# single-star patterns this repository uses and failing closed below if a COPY
# source appears that it cannot reason about.
dockerignore_drops() {
  awk -v df="$dockerignore" -v target="$1" '
    function to_ere(p,   out, i, c) {
      out = ""
      for (i = 1; i <= length(p); i++) {
        c = substr(p, i, 1)
        if (c == "*") out = out "[^/]*"
        else if (c == "?") out = out "[^/]"
        else if (index("\\^$.[]|()", c) > 0) out = out "\\" c
        else out = out c
      }
      return out
    }
    FILENAME == df {
      if ($0 ~ /^[[:space:]]*(#|$)/) next
      if ($0 ~ /^[[:space:]]*!/) next
      p = $0
      sub(/^[[:space:]]+/, "", p)
      sub(/^\/+/, "", p)
      sub(/\/+$/, "", p)
      if (p == "") next
      ere[++np] = "^" to_ere(p) "$"
      next
    }
    END {
      if (target == "" || target == ".") { print "unanalysable"; exit }
      for (i = 1; i <= np; i++) {
        probe = target
        while (1) {
          if (probe ~ ere[i]) { print "drops"; exit }
          if (probe !~ /\//) break
          sub(/\/[^/]*$/, "", probe)
        }
      }
      print "keeps"
    }
  ' "$dockerignore"
}

if [ ! -r "$dockerignore" ]; then
  report FAIL "dockerignore: present" "missing $(basename "$dockerignore")"
elif [ ! -r "$dockerfile" ]; then
  report FAIL "dockerignore: reads Dockerfile" "missing $(basename "$dockerfile")"
else
  report PASS "dockerignore: present"
  case_n=$((case_n + 1))
  if [ "$(dockerignore_drops .git)" = "drops" ]; then
    report PASS "dockerignore: excludes .git"
  else
    report FAIL "dockerignore: excludes .git" "repo history would reach the builder"
  fi
  sources_file="$work/context-sources.txt"
  context_sources "$dockerfile" > "$sources_file"
  if [ ! -s "$sources_file" ]; then
    report FAIL "dockerignore: reads Dockerfile context sources" "parsed none"
  else
    while IFS= read -r source; do
      case_n=$((case_n + 1))
      case "$(dockerignore_drops "$source")" in
        drops)
          report FAIL "dockerignore: keeps Dockerfile input $source" "excluded by .dockerignore"
          ;;
        unanalysable)
          report FAIL "dockerignore: keeps Dockerfile input $source" "cannot analyse this source"
          ;;
        *)
          report PASS "dockerignore: keeps Dockerfile input $source"
          ;;
      esac
    done < "$sources_file"
  fi
fi
echo
echo "total: $((pass + fail)) passed: $pass failed: $fail"
if [ "$fail" -gt 0 ]; then
  exit 1
fi
