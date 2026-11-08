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

# Mise pin drift (#768): the pinned launcher version must agree across
# hack/bootstrap.sh, Dockerfile, and .mise/config.toml (the keep-together
# contract in bootstrap.sh's bump comment), and the linux-x64 digest must
# match between bootstrap.sh and the Dockerfile so a mise bump cannot land
# half-applied with one file still carrying the previous release's digest.
# Extraction is fail-closed: an unreadable, missing, duplicated, malformed,
# or commented-out pin reports FAIL. A version string that is not a mise
# release number (mise releases are CalVer: three dot-separated numbers)
# fails even when all three pins agree, and the binary pins must carry the
# "v" prefix, so a consistently formatted corruption cannot pass as
# aligned. Each bootstrap case branch must pin exactly one well-formed
# digest bound to the pinned version's release asset name, matching what
# resolve_mise_digest hands to the runtime checksum check; a bare count of
# well-formed digest lines could not tell which release a digest belongs
# to. Assignment accounting guards the pre-case initializers: a non-empty
# assignment before resolve_mise_digest runs would make it return early
# with an unverified digest. The asset binding is verified for real: all
# four branch digests are compared against the SHASUMS256.txt of the pinned
# mise release, fetched from github.com/jdx/mise. Without that comparison a
# stale digest carried over from another mise release (or a digest of a
# different asset in the same release) is still well-formed and would pass.
# The fetch is fail-closed: an unreachable release or a checksum file that
# does not name an asset is a FAIL, not a pass, because a digest that
# cannot be verified provides no provenance. The regression mutations are
# checked for effect as well: a mutation that leaves the pinned digest in
# place would verify clean, so such a case is a FAIL rather than a green
# regression.
bootstrap_sh="$script_dir/bootstrap.sh"
mise_config="$script_dir/../.mise/config.toml"
bs_version="$(sed -n 's/^MISE_VERSION=//p' "$bootstrap_sh" 2>/dev/null)" || bs_version=
docker_version="$(sed -n 's/^ARG MISE_VERSION=//p' "$dockerfile" 2>/dev/null)" || docker_version=
min_version="$(sed -n 's/^min_version = "\(.*\)"$/\1/p' "$mise_config" 2>/dev/null)" || min_version=
mise_release_number() {
  printf '%s' "$1" | grep -qE '^[0-9]+\.[0-9]+\.[0-9]+$'
}
case_n=$((case_n + 1))
if mise_release_number "${bs_version#v}" && mise_release_number "${docker_version#v}" &&
  mise_release_number "$min_version" &&
  [ "$bs_version" = "v$min_version" ] && [ "$docker_version" = "v$min_version" ]; then
  report PASS "mise pin: version aligned and well-formed across bootstrap.sh, Dockerfile, config.toml ($bs_version)"
else
  report FAIL "mise pin: version aligned and well-formed across bootstrap.sh, Dockerfile, config.toml" \
    "bootstrap.sh=[$bs_version] Dockerfile=[$docker_version] config.toml=[$min_version]"
fi
mise_branch_check() {
  local label="$1" suffix="$2" desc block asset digests
  desc="mise pin: $label branch pins one digest for mise-\${MISE_VERSION}-$suffix"
  block="$(sed -n "\\|$label)|,/;;/p" "$bootstrap_sh" 2>/dev/null)" || block=
  asset="$(printf '%s\n' "$block" | sed -n 's/^[[:space:]]*mise_asset="\(.*\)"$/\1/p')"
  digests="$(printf '%s\n' "$block" | sed -n 's/^[[:space:]]*mise_sha256=//p')"
  case_n=$((case_n + 1))
  if [ "$asset" = 'mise-${MISE_VERSION}-'"$suffix" ] &&
    [ "$(printf '%s\n' "$digests" | grep -c . || true)" -eq 1 ] &&
    [ "$(printf '%s\n' "$digests" | grep -cE '^[0-9a-f]{64}$' || true)" -eq 1 ]; then
    report PASS "$desc"
  else
    report FAIL "$desc" "asset=[$asset] digests=[$digests]"
  fi
}
mise_branch_check 'Linux/x86_64' linux-x64
mise_branch_check 'Linux/aarch64' linux-arm64
mise_branch_check 'Darwin/arm64' macos-arm64
mise_branch_check 'Darwin/x86_64' macos-x64
case_n=$((case_n + 1))
bs_sha_total="$(grep -cE '^[[:space:]]*mise_sha256=' "$bootstrap_sh" || true)"
bs_sha_empty="$(grep -cE '^[[:space:]]*mise_sha256=$' "$bootstrap_sh" || true)"
bs_asset_total="$(grep -cE '^[[:space:]]*mise_asset=' "$bootstrap_sh" || true)"
bs_asset_empty="$(grep -cE '^[[:space:]]*mise_asset=$' "$bootstrap_sh" || true)"
if [ "$bs_sha_total" -eq 5 ] && [ "$bs_sha_empty" -eq 1 ] &&
  [ "$bs_asset_total" -eq 5 ] && [ "$bs_asset_empty" -eq 1 ]; then
  report PASS "mise pin: bootstrap.sh asset and digest assignments accounted for (4 branches + 1 empty initializer each)"
else
  report FAIL "mise pin: bootstrap.sh asset and digest assignments accounted for (4 branches + 1 empty initializer each)" \
    "digests=$bs_sha_total (empty: $bs_sha_empty) assets=$bs_asset_total (empty: $bs_asset_empty)"
fi
bs_x64="$(sed -n '\|Linux/x86_64)|,/;;/s/^[[:space:]]*mise_sha256=//p' "$bootstrap_sh" 2>/dev/null)" || bs_x64=
docker_x64="$(grep -oE '[0-9a-f]{64}[[:space:]]+/tmp/mise' "$dockerfile" | cut -d' ' -f1 || true)"
case_n=$((case_n + 1))
if [ "$(printf '%s\n' "$bs_x64" | grep -cE '^[0-9a-f]{64}$' || true)" -eq 1 ] &&
  [ -n "$docker_x64" ] && [ "$bs_x64" = "$docker_x64" ]; then
  report PASS "mise pin: linux-x64 digest aligned between bootstrap.sh and Dockerfile"
else
  report FAIL "mise pin: linux-x64 digest aligned between bootstrap.sh and Dockerfile" \
    "bootstrap.sh=[$bs_x64] Dockerfile=[$docker_x64]"
fi

# The downloader preference mirrors hack/bootstrap.sh so a host that can
# run the bootstrap can also run this check. One retry softens a transient
# failure; the check itself stays fail-closed.
fetch_url() {
  local url="$1" dest="$2" attempt
  for attempt in 1 2; do
    if command -v curl >/dev/null 2>&1; then
      curl -fsSL -o "$dest" "$url" && return 0
    elif command -v wget >/dev/null 2>&1; then
      wget -nv -O "$dest" "$url" && return 0
    elif command -v node >/dev/null 2>&1; then
      node -e '
const [url, dest] = process.argv.slice(1);
fetch(url).then((r) => {
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.arrayBuffer();
}).then((b) => require("fs").writeFileSync(dest, Buffer.from(b))
).catch((e) => { console.error(`download failed: ${e.message}`); process.exit(1); });
' "$url" "$dest" && return 0
    else
      return 127
    fi
  done
  return 1
}

# Returns every checksum in the release checksum file $1 that names the
# mise asset $2 (the version, "v" prefixed) for the platform suffix $3.
# An empty result means the release does not publish that asset.
mise_asset_digests() {
  local shasums="$1" version="$2" suffix="$3"
  [ -r "$shasums" ] || return 0
  grep -E "^[0-9a-f]{64}[[:space:]]+\./mise-${version}-${suffix}\$" "$shasums" 2>/dev/null | cut -d' ' -f1 || true
}

# The first checksum the release file lists for the asset, or an empty
# string when the release does not publish it.
mise_first_digest() {
  local all
  all="$(mise_asset_digests "$1" "$2" "$3")"
  printf '%s\n' "$all" | sed -n '1p'
}

# Verifies every branch digest in bootstrap.sh $1 against the release
# checksum file $2. Prints a reason and returns 1 on the first mismatch;
# prints nothing and returns 0 when each digest matches exactly one
# checksum line for its release asset. The asset name is rebuilt from the
# version pinned in the file under test, so a version bump whose digests
# were not updated fails on the asset lookup, and a digest carried over
# from another release or asset fails on the comparison.
mise_verify_branch_digests() {
  local bs="$1" shasums="$2" version pair label suffix want got
  [ -r "$shasums" ] || { printf 'release checksum file unreadable\n'; return 1; }
  version="$(sed -n 's/^MISE_VERSION=//p' "$bs" 2>/dev/null)" || version=
  if ! mise_release_number "${version#v}" || [ "${version#v}" = "$version" ]; then
    printf 'bootstrap.sh pins malformed MISE_VERSION [%s]\n' "$version"; return 1
  fi
  for pair in \
    'Linux/x86_64:linux-x64' \
    'Linux/aarch64:linux-arm64' \
    'Darwin/arm64:macos-arm64' \
    'Darwin/x86_64:macos-x64'; do
    label="${pair%%:*}"
    suffix="${pair##*:}"
    want="$(sed -n "\\|$label)|,/;;/s/^[[:space:]]*mise_sha256=//p" "$bs" 2>/dev/null |
      sed -n 's/^\([0-9a-f]\{64\}\)[[:space:]]*$/\1/p')"
    if [ "$(printf '%s\n' "$want" | grep -c . || true)" -ne 1 ]; then
      printf '%s branch does not pin exactly one well-formed digest\n' "$label"; return 1
    fi
    got="$(mise_asset_digests "$shasums" "$version" "$suffix")"
    if [ "$(printf '%s\n' "$got" | grep -c . || true)" -ne 1 ]; then
      printf 'mise-%s-%s appears %s times in SHASUMS256.txt\n' \
        "$version" "$suffix" "$(printf '%s\n' "$got" | grep -c . || true)"; return 1
    fi
    if [ "$want" != "$got" ]; then
      printf '%s digest %s does not match the %s release digest %s for mise-%s-%s\n' \
        "$label" "$want" "$version" "$got" "$version" "$suffix"; return 1
    fi
  done
}

case_n=$((case_n + 1))
mise_shasums="$work/mise-shasums.txt"
mise_shasums_url="https://github.com/jdx/mise/releases/download/${bs_version}/SHASUMS256.txt"
if fetch_url "$mise_shasums_url" "$mise_shasums"; then
  mise_reason="$(mise_verify_branch_digests "$bootstrap_sh" "$mise_shasums")" || true
  if [ -z "$mise_reason" ]; then
    report PASS "mise pin: all four branch digests match the ${bs_version} release SHASUMS256.txt"
  else
    report FAIL "mise pin: all four branch digests match the ${bs_version} release SHASUMS256.txt" "$mise_reason"
  fi
else
  report FAIL "mise pin: all four branch digests match the ${bs_version} release SHASUMS256.txt" \
    "could not fetch $mise_shasums_url (the digest verification is fail-closed)"
fi

# Regression cases run the same verification against mutated copies of
# bootstrap.sh, each of which must be rejected. The digest mutations are
# well-formed 64-hex digests, so only the asset binding can catch them:
# one is the linux-x64 digest copied into the linux-arm64 branch, the
# other is the digest of a different asset in the same release. The
# version mutation keeps the digests and moves the pin, so the asset
# lookup itself must fail.
#
# Every mutation must actually change the value it claims to change: a
# mutation that rewrote a branch with the release's own digest would
# verify clean and would pass for the wrong reason, so such a case fails
# loudly instead of reporting a green regression.
mise_digest_reject_case() {
  local desc="$1" bs="$2" label="$3" suffix="$4" reason expected actual
  case_n=$((case_n + 1))
  expected="$(mise_first_digest "$mise_shasums" "$bs_version" "$suffix")"
  actual="$(sed -n "\\|$label)|,/;;/s/^[[:space:]]*mise_sha256=//p" "$bs" 2>/dev/null |
    sed -n 's/^\([0-9a-f]\{64\}\)[[:space:]]*$/\1/p')"
  if [ -z "$expected" ]; then
    report FAIL "mise pin fail-closed: $desc" \
      "the release checksum file does not publish the $suffix asset, so the mutation cannot be checked"
    return
  fi
  if [ "$actual" = "$expected" ]; then
    report FAIL "mise pin fail-closed: $desc" \
      "the mutation did not replace the $suffix digest (mutated=[$actual] release=[$expected])"
    return
  fi
  reason="$(mise_verify_branch_digests "$bs" "$mise_shasums")" || true
  if [ -n "$reason" ]; then
    report PASS "mise pin fail-closed: $desc" "$reason"
  else
    report FAIL "mise pin fail-closed: $desc" "mutated bootstrap.sh passed the digest verification"
  fi
}

mise_version_reject_case() {
  local desc="$1" bs="$2" reason pinned
  case_n=$((case_n + 1))
  pinned="$(sed -n 's/^MISE_VERSION=//p' "$bs" 2>/dev/null)" || pinned=
  if [ "$pinned" = "$bs_version" ]; then
    report FAIL "mise pin fail-closed: $desc" "the mutation did not change MISE_VERSION"
    return
  fi
  reason="$(mise_verify_branch_digests "$bs" "$mise_shasums")" || true
  if [ -n "$reason" ]; then
    report PASS "mise pin fail-closed: $desc" "$reason"
  else
    report FAIL "mise pin fail-closed: $desc" "mutated bootstrap.sh passed the digest verification"
  fi
}

bs_x64_digest="$(printf '%s\n' "$bs_x64" | sed -n '1p')"
sed '\|Linux/aarch64)|,/;;/s/^[[:space:]]*mise_sha256=.*/\tmise_sha256='"$bs_x64_digest"'/' \
  "$bootstrap_sh" > "$work/bootstrap-stale-arm64.sh"
mise_digest_reject_case "stale linux-arm64 digest (carries the linux-x64 digest)" \
  "$work/bootstrap-stale-arm64.sh" 'Linux/aarch64' linux-arm64

# The macos-arm64 mutation carries the digest of a different asset in the
# same release, so only the asset binding can catch it. The preferred
# asset is the linux-x64-musl binary; when a release stops publishing
# it, the digest of another published asset of the same release is used
# (linux-arm64, then macos-x64). Every candidate is a real asset of the
# pinned release, so the case stays a wrong-digest case instead of
# degrading into a copy of the linux-arm64 mutation. The last resort, a
# corrupted copy of the macos-arm64 digest, is still a well-formed digest
# that the verification has to reject.
mutation_digest=""
mutation_asset=""
for candidate in linux-x64-musl linux-arm64 macos-x64; do
  candidate_digest="$(mise_first_digest "$mise_shasums" "$bs_version" "$candidate")"
  if [ -z "$candidate_digest" ]; then
    continue
  fi
  mutation_digest="$candidate_digest"
  mutation_asset="mise-${bs_version}-${candidate}"
  break
done
if [ -z "$mutation_digest" ]; then
  expected_macos="$(mise_first_digest "$mise_shasums" "$bs_version" macos-arm64)"
  last_char="$(printf '%s' "$expected_macos" | sed 's/.*\(.\)$/\1/')"
  if [ "$last_char" = "0" ]; then
    mutation_digest="${expected_macos%?}1"
  else
    mutation_digest="${expected_macos%?}0"
  fi
  mutation_asset="a corrupted copy of the macos-arm64 digest"
fi
sed '\|Darwin/arm64)|,/;;/s/^[[:space:]]*mise_sha256=.*/\tmise_sha256='"$mutation_digest"'/' \
  "$bootstrap_sh" > "$work/bootstrap-stale-macos-arm64.sh"
mise_digest_reject_case "stale macos-arm64 digest (digest of a different release asset: $mutation_asset)" \
  "$work/bootstrap-stale-macos-arm64.sh" 'Darwin/arm64' macos-arm64

# The version mutation keeps the digests and moves the pin, so the asset
# lookup itself must fail. The stale version is picked so it differs from
# the pinned one on every release instead of naming one fixed release.
stale_version=""
for candidate in v2026.10.13 v2026.9.0 v2020.1.1; do
  if [ "$candidate" = "$bs_version" ]; then
    continue
  fi
  stale_version="$candidate"
  break
done
if [ -z "$stale_version" ]; then
  case_n=$((case_n + 1))
  report FAIL "mise pin fail-closed: pinned version changed without updating the digests" \
    "no stale version candidate differs from the pinned $bs_version"
else
  sed "s/^MISE_VERSION=.*/MISE_VERSION=$stale_version/" \
    "$bootstrap_sh" > "$work/bootstrap-stale-version.sh"
  mise_version_reject_case "pinned version changed without updating the digests ($stale_version)" \
    "$work/bootstrap-stale-version.sh"
fi

echo
echo "total: $((pass + fail)) passed: $pass failed: $fail"
if [ "$fail" -gt 0 ]; then
  exit 1
fi
