#!/usr/bin/env bash
# Hermes Agent bootstrap: clone, acquire uv/Python, then hand the checkout to
# the same completion an update runs -- command publication, product builds and
# post-build maintenance -- so a fresh install and a finished update land in one
# state. Heavy dependencies (tool binaries, browsers, node) are pm's job:
# `hermes pm install`.
#
# Stage protocol kept for Hermes-Setup:
#   --manifest            print the stage list as JSON
#   --stage NAME [--json] run one stage
#   --non-interactive     skip stages that need input
#   --include-desktop     build the desktop app too (products stage)
#   --verbose             stream every child command's output (the default
#                         off a terminal and in CI)
set -u

# Prevent uv from discovering config files (uv.toml, pyproject.toml) from the
# wrong user's home directory when running under sudo -u <user>.  See #21269.
# pm's own venv sync re-isolates (pm/environment.py), so this bootstrap
# hygiene can't break the locked sync the way it used to before pm owned it.
export UV_NO_CONFIG=1

REPO_URL="${HERMES_REPO_URL:-https://github.com/NousResearch/hermes-agent.git}"
BRANCH="main"
INSTALL_COMMIT=""
INSTALL_DIR="${HERMES_INSTALL_DIR:-}"
HERMES_HOME="${HERMES_HOME:-$HOME/.hermes}"
STAGE=""
WANT_MANIFEST=false
JSON=false
NON_INTERACTIVE=false
INCLUDE_DESKTOP=false
VERBOSE=false
SKIP_BROWSER=false
SKIP_COMPUTER_USE=false

while [ $# -gt 0 ]; do
    case "$1" in
        --branch|-Branch|--commit|-Commit|--dir|--hermes-home|-HermesHome|--stage|-Stage)
            option="$1"
            if [ $# -lt 2 ] || [ -z "$2" ] || [[ "$2" == -* ]]; then
                printf '%s needs a value\n' "$option" >&2
                exit 2
            fi
            case "$option" in
                --branch|-Branch) BRANCH="$2" ;;
                --commit|-Commit) INSTALL_COMMIT="$2" ;;
                --dir) INSTALL_DIR="$2" ;;
                --hermes-home|-HermesHome) HERMES_HOME="$2" ;;
                --stage|-Stage) STAGE="$2" ;;
            esac
            shift 2 ;;
        --manifest|-Manifest) WANT_MANIFEST=true; shift ;;
        --json|-Json) JSON=true; shift ;;
        --non-interactive|-NonInteractive) NON_INTERACTIVE=true; shift ;;
        --skip-setup) NON_INTERACTIVE=true; shift ;;
        --skip-browser|--no-playwright|-SkipBrowser) SKIP_BROWSER=true; shift ;;
        --skip-computer-use|-SkipComputerUse) SKIP_COMPUTER_USE=true; shift ;;
        --include-desktop|-IncludeDesktop) INCLUDE_DESKTOP=true; shift ;;
        --verbose|-Verbose) VERBOSE=true; shift ;;
        -h|--help)
            echo "Usage: install.sh [--branch NAME] [--commit SHA] [--dir PATH]"
            echo "                  [--hermes-home PATH]"
            echo "                  [--manifest] [--stage NAME] [--json]"
            echo "                  [--non-interactive] [--include-desktop] [--verbose]"
            echo "                  [--skip-browser] [--skip-computer-use]"
            echo
            echo "  --skip-browser  Do not install the browser tools (agent-browser + Chromium)."
            echo "                  Alias: --no-playwright. Remembered by later"
            echo "                  installs and 'hermes update'; undo with 'hermes pm install agent-browser'."
            echo "  --skip-computer-use"
            echo "                  Do not install the computer-use driver (cua-driver). Remembered"
            echo "                  the same way; undo with 'hermes pm install cua-driver'."
            exit 0 ;;
        *) echo "unknown option: $1" >&2; exit 1 ;;
    esac
done

INSTALL_DIR="${INSTALL_DIR:-$HERMES_HOME/hermes-agent}"
export HERMES_HOME

INSTALL_LOG="$HERMES_HOME/logs/install.log"

# Same glyphs as the pre-pm installer. Colour only on a terminal, so CI
# transcripts and the Hermes-Setup driver read plain text.
if [ -t 1 ] && [ -z "${NO_COLOR:-}" ]; then
    C_RED=$'\033[0;31m' C_GREEN=$'\033[0;32m' C_YELLOW=$'\033[0;33m'
    C_CYAN=$'\033[0;36m' C_MAGENTA=$'\033[0;35m' C_BOLD=$'\033[1m'
    C_DIM=$'\033[2m' C_NC=$'\033[0m'
else
    C_RED="" C_GREEN="" C_YELLOW="" C_CYAN="" C_MAGENTA="" C_BOLD="" C_DIM="" C_NC=""
fi

log() { printf '%s→%s %s\n' "$C_CYAN" "$C_NC" "$1"; }
log_success() { printf '%s✓%s %s\n' "$C_GREEN" "$C_NC" "$1"; }
log_warn() { printf '%s⚠%s %s\n' "$C_YELLOW" "$C_NC" "$1"; }
log_error() { printf '%s✗%s %s\n' "$C_RED" "$C_NC" "$1" >&2; }
fail() { STAGE_REASON="$1"; log_error "$1"; exit 1; }

print_banner() {
    printf '\n%s%s' "$C_MAGENTA" "$C_BOLD"
    printf '%s\n' "┌─────────────────────────────────────────────────────────┐"
    printf '%s\n' "│             ☤ Hermes Agent Installer                    │"
    printf '%s\n' "├─────────────────────────────────────────────────────────┤"
    printf '%s\n' "│  An open source AI agent by Nous Research.              │"
    printf '%s\n' "└─────────────────────────────────────────────────────────┘"
    printf '%s\n' "$C_NC"
}

# Interactive runs collapse child-process output (git, uv, pm, the builds)
# into one status line. CI, --verbose and a non-terminal stdout -- the
# Hermes-Setup --json driver, E2E transcripts -- keep the full stream those
# readers parse.
quiet_output() {
    [ "$VERBOSE" = true ] && return 1
    if [ -n "${CI:-}" ] || [ -n "${GITHUB_ACTIONS:-}" ] || [ -n "${HERMES_INSTALL_VERBOSE:-}" ]; then
        return 1
    fi
    [ -t 1 ]
}

status_line() {
    local text="  $1" width=$(( $2 - 1 ))
    [ "${#text}" -le "$width" ] || text="${text:0:width}"
    printf '\r\033[K%s%s%s' "$C_DIM" "$text" "$C_NC"
}

# run_logged [--may-fail] LABEL CMD...: run CMD. Quiet mode shows LABEL with
# CMD's latest output line rewritten in place, appends everything to
# $INSTALL_LOG and, on failure, prints the tail and the log path; --may-fail
# is for probes whose failure the caller handles (no report). Otherwise LABEL
# is logged and the output streams untouched. Returns CMD's exit status.
run_logged() {
    local may_fail=false
    if [ "$1" = --may-fail ]; then may_fail=true; shift; fi
    local label="$1"; shift
    if ! quiet_output || ! { mkdir -p "${INSTALL_LOG%/*}" && : >> "$INSTALL_LOG"; } 2>/dev/null; then
        log "$label"
        "$@"
        return
    fi
    local start cols rc line shown
    start=$(( $(wc -l < "$INSTALL_LOG") + 1 ))
    cols="$(tput cols 2>/dev/null)" || cols=80
    [ "${cols:-0}" -gt 20 ] 2>/dev/null || cols=80
    printf '==> %s (%s)\n' "$label" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$INSTALL_LOG"
    status_line "$label" "$cols"
    # stdin closed: under `curl | bash` it is the script itself, and nothing
    # run here may prompt behind a status line.
    "$@" </dev/null 2>&1 | {
        while IFS= read -r line || [ -n "$line" ]; do
            line="${line%$'\r'}"
            printf '%s\n' "$line" >&3
            # git and uv redraw progress with bare CRs; show the newest.
            shown="${line##*$'\r'}"
            [ -z "$shown" ] || status_line "$label: $shown" "$cols"
        done
    } 3>>"$INSTALL_LOG"
    rc=${PIPESTATUS[0]}
    printf '\r\033[K'
    if [ "$rc" -ne 0 ] && [ "$may_fail" = false ]; then
        log_error "$label failed (exit $rc). Last output:"
        tail -n +"$(( start + 1 ))" "$INSTALL_LOG" | tail -n 20 | sed 's/^/    /' >&2
        printf '    full log: %s\n' "$INSTALL_LOG" >&2
    fi
    return "$rc"
}

# --- BEGIN GENERATED: bootstrap pins (scripts/gen-bootstrap-pins.py) ---
# Derived from pm/lock.json. DO NOT EDIT BY HAND:
# run scripts/gen-bootstrap-pins.py after a pin bump.
UV_PIN_VERSION="0.12.3"

# Sets UV_PIN_URL + UV_PIN_SHA256 for a <os>-<arch> target key.
uv_bootstrap_pin() {
    case "$1" in
        linux-x64)
            UV_PIN_URL="https://github.com/astral-sh/uv/releases/download/0.12.3/uv-x86_64-unknown-linux-gnu.tar.gz"
            UV_PIN_MIRROR="https://hermes-assets.nousresearch.com/upstream/sha256/600cf9a742aca00d292673b16b5acffaa7b8c269a364ad0c2e79498dcb1fe101"
            UV_PIN_SHA256="600cf9a742aca00d292673b16b5acffaa7b8c269a364ad0c2e79498dcb1fe101"
            ;;
        linux-arm64)
            UV_PIN_URL="https://github.com/astral-sh/uv/releases/download/0.12.3/uv-aarch64-unknown-linux-gnu.tar.gz"
            UV_PIN_MIRROR="https://hermes-assets.nousresearch.com/upstream/sha256/bb66cb52e7b1823aed1183630d8d8e5c958840d584a4c55ec10a4cfc168dcca2"
            UV_PIN_SHA256="bb66cb52e7b1823aed1183630d8d8e5c958840d584a4c55ec10a4cfc168dcca2"
            ;;
        linux-x64-musl)
            UV_PIN_URL="https://github.com/astral-sh/uv/releases/download/0.12.3/uv-x86_64-unknown-linux-musl.tar.gz"
            UV_PIN_MIRROR="https://hermes-assets.nousresearch.com/upstream/sha256/0643b9fb8c9fb27458e709ce6ff939695013c41975ff7b02d3f3b138d8d4bdb3"
            UV_PIN_SHA256="0643b9fb8c9fb27458e709ce6ff939695013c41975ff7b02d3f3b138d8d4bdb3"
            ;;
        linux-arm64-musl)
            UV_PIN_URL="https://github.com/astral-sh/uv/releases/download/0.12.3/uv-aarch64-unknown-linux-musl.tar.gz"
            UV_PIN_MIRROR="https://hermes-assets.nousresearch.com/upstream/sha256/fa513fca1eb2913334c944fe9adbdd410274a1cbe8dd05d03699a9eb85311d4e"
            UV_PIN_SHA256="fa513fca1eb2913334c944fe9adbdd410274a1cbe8dd05d03699a9eb85311d4e"
            ;;
        darwin-x64)
            UV_PIN_URL="https://github.com/astral-sh/uv/releases/download/0.12.3/uv-x86_64-apple-darwin.tar.gz"
            UV_PIN_MIRROR="https://hermes-assets.nousresearch.com/upstream/sha256/4c9f52262a14da336e4a42ed24992d12d0c956acde87619e4611d321dffa602b"
            UV_PIN_SHA256="4c9f52262a14da336e4a42ed24992d12d0c956acde87619e4611d321dffa602b"
            ;;
        darwin-arm64)
            UV_PIN_URL="https://github.com/astral-sh/uv/releases/download/0.12.3/uv-aarch64-apple-darwin.tar.gz"
            UV_PIN_MIRROR="https://hermes-assets.nousresearch.com/upstream/sha256/546f7f8a6c70ff13a3a9d2bc958db3427298cebf3e0cb756f9177133b7068843"
            UV_PIN_SHA256="546f7f8a6c70ff13a3a9d2bc958db3427298cebf3e0cb756f9177133b7068843"
            ;;
        *)
            UV_PIN_URL=""
            UV_PIN_SHA256=""
            return 1
            ;;
    esac
}
# --- END GENERATED: bootstrap pins ---

uv_bootstrap_target() {
    # Map this host to a pm/lock.json target key (<os>-<arch>).
    local _arch
    case "$(uname -m)" in
        arm64|aarch64) _arch="arm64" ;;
        x86_64|amd64)  _arch="x64" ;;
        *) return 1 ;;
    esac
    case "$(uname -s)" in
        Linux)
            # Same precedence as pm/store.py::_is_musl_libc: the native
            # userland's ELF interpreter decides; ldd and a musl loader on
            # disk are fallbacks only (a glibc host may carry musl as a
            # secondary toolchain, and minimal musl roots may lack ldd).
            local _libc="" _probe _head
            for _probe in /bin/sh /bin/ls; do
                _head="$(head -c 8192 "$_probe" 2>/dev/null | LC_ALL=C tr -d '\000')" || continue
                [[ "$_head" == $'\x7f'ELF* ]] || continue
                case "$_head" in
                    *ld-musl-*) _libc="musl"; break ;;
                    *ld-linux*) _libc="glibc"; break ;;
                esac
            done
            if [[ -z "$_libc" ]]; then
                _libc="$(ldd --version 2>&1 || true)"
                _libc="${_libc,,}"
            fi
            if [[ "$_libc" == *musl* ]]; then
                echo "linux-$_arch-musl"
            elif [[ "$_libc" == *glibc* || "$_libc" == *"gnu libc"* || "$_libc" == *"gnu c library"* ]]; then
                echo "linux-$_arch"
            elif compgen -G '/lib/ld-musl-*.so.1' >/dev/null; then
                echo "linux-$_arch-musl"
            else
                echo "linux-$_arch"
            fi
            ;;
        Darwin) echo "darwin-$_arch" ;;
        *) return 1 ;;
    esac
}

# Provision uv for this host from the pinned pm/lock.json artifact. Stages
# the EXACT artifact pm itself uses into the same store slot
# (<store>/uv-<version>-<target>/, the store pm's store_root() resolves),
# sha256-verified, so the byte authority is pm/lock.json - no astral-latest,
# no curl|sh.
UV_CMD=""
ensure_uv() {
    [ -n "$UV_CMD" ] && return 0
    # Always the pinned artifact, never a uv already on PATH: Hermes runs only
    # its own packaged toolchain.
    local _target
    if ! _target="$(uv_bootstrap_target)"; then
        fail "no pinned uv build for this platform ($(uname -s) $(uname -m)); Hermes does not support this host"
    fi
    if ! uv_bootstrap_pin "$_target"; then
        fail "no pinned uv artifact for $_target; Hermes does not support this host"
    fi
    local _store="${HERMES_RUNTIME_DIR:-$HERMES_HOME/tools}"
    local _entry="$_store/uv-$UV_PIN_VERSION-$_target"
    UV_CMD="$_entry/uv"
    if [ ! -x "$UV_CMD" ]; then
        log "Downloading uv $UV_PIN_VERSION ($_target)"
        local _tmp
        # no-tmp: ok — last-resort fallback when mktemp itself is missing
        _tmp="$(mktemp -d 2>/dev/null || echo "/tmp/hermes-uv-bootstrap.$$")"
        mkdir -p "$_tmp"
        local _fetched_from="$UV_PIN_URL"
        # Only network availability failures permit trying identical mirrored bytes.
        if curl -LsSf "$UV_PIN_URL" -o "$_tmp/uv.tar.gz"; then
            :
        else
            local _curl_status=$?
            case "$_curl_status" in
                5|6|7|18|22|28|52|55|56) ;;
                *) rm -rf "$_tmp"; fail "failed to download pinned uv from $UV_PIN_URL (curl $_curl_status)" ;;
            esac
            if [ -n "${UV_PIN_MIRROR:-}" ] && curl -LsSf "$UV_PIN_MIRROR" -o "$_tmp/uv.tar.gz"; then
                _fetched_from="$UV_PIN_MIRROR"
            else
                rm -rf "$_tmp"
                fail "failed to download pinned uv from $UV_PIN_URL or ${UV_PIN_MIRROR:-no mirror}"
            fi
        fi
        local _digest
        if command -v sha256sum >/dev/null 2>&1; then
            _digest="$(sha256sum "$_tmp/uv.tar.gz" | cut -d' ' -f1)"
        else
            _digest="$(shasum -a 256 "$_tmp/uv.tar.gz" | cut -d' ' -f1)"
        fi
        if [ "$_digest" != "$UV_PIN_SHA256" ]; then
            rm -rf "$_tmp"
            fail "uv download digest mismatch from $_fetched_from (expected $UV_PIN_SHA256, got $_digest)"
        fi
        if ! tar -xzf "$_tmp/uv.tar.gz" -C "$_tmp"; then
            rm -rf "$_tmp"
            fail "failed to extract pinned uv archive"
        fi
        local _unpacked
        _unpacked="$(find "$_tmp" -mindepth 1 -maxdepth 2 -name uv -type f | head -n1)"
        if [ -z "$_unpacked" ]; then
            rm -rf "$_tmp"
            fail "uv binary not found in the downloaded archive"
        fi
        mkdir -p "$_entry"
        mv "$_unpacked" "$UV_CMD"
        [ -f "$(dirname "$_unpacked")/uvx" ] && mv "$(dirname "$_unpacked")/uvx" "$_entry/uvx"
        chmod +x "$UV_CMD"
        chmod +x "$_entry/uvx" 2>/dev/null || true
        rm -rf "$_tmp"
    fi
    # Bootstrap keeps the installer private; only UV_CMD invokes it.
    if ! "$UV_CMD" --version >/dev/null 2>&1; then
        fail "pinned uv staged but does not run on this host"
    fi
    log_success "uv ready ($("$UV_CMD" --version 2>/dev/null))"
}

check_platform() {
    # Termux is Linux by uname, but this installer builds a glibc source
    # install the phone cannot run (no Android wheels in the lock). The
    # signed APT package is the only supported shape there.
    if [ -n "${TERMUX_VERSION:-}" ] || case "${PREFIX:-}" in *com.termux/files/usr*) true ;; *) false ;; esac; then
        fail "Termux is installed from its APT repository, not install.sh: pkg install hermes-agent (setup: https://hermes-agent.nousresearch.com/docs/getting-started/termux)"
    fi
    case "$(uname -s 2>/dev/null)" in
        Linux*) : ;;
        Darwin*) : ;;
        *) fail "unsupported platform: $(uname -s). On Windows use install.ps1." ;;
    esac
}

json_string() {
    local value="$1" code char escaped
    value="${value//\\/\\\\}"
    value="${value//\"/\\\"}"
    for ((code = 1; code < 32; code++)); do
        printf -v char '\\%03o' "$code"
        printf -v char '%b' "$char"
        printf -v escaped '\\u%04x' "$code"
        value="${value//"$char"/$escaped}"
    done
    printf '"%s"' "$value"
}

json_frame() {
    # $1 ok, $2 stage, $3 skipped, $4 reason
    if [ -n "${4:-}" ]; then
        printf '{"ok":%s,"stage":%s,"skipped":%s,"reason":%s}\n' "$1" "$(json_string "$2")" "$3" "$(json_string "$4")"
    else
        printf '{"ok":%s,"stage":%s,"skipped":%s}\n' "$1" "$(json_string "$2")" "$3"
    fi
}

stage_result() {
    local code="$1" ok=false reason="${STAGE_REASON:-}"
    if [ "$code" -eq 0 ]; then
        ok=true
    else
        reason="${reason:-stage failed (exit $code)}"
    fi
    if [ "$JSON" = true ]; then
        json_frame "$ok" "$STAGE" "${STAGE_SKIPPED:-false}" "$reason"
    fi
}

# The single authoritative stage list: emit_manifest prints it AND the
# no-flag ladder runs it. `products` is the shared completion tail -- the same
# call `hermes update` makes -- so the manifest and the run cannot disagree.
# `desktop` stays directly dispatchable via --stage for external callers, but
# is never listed: --include-desktop selects the desktop product inside
# `products` instead of adding a second build stage.
stage_names() {
    printf '%s\n' prerequisites repository venv python-deps config products setup gateway complete
}

# "title|category|needs_user_input".
products_record() {
    if [ "$INCLUDE_DESKTOP" = true ]; then
        echo "Install command and app + desktop|runtime|false"
    else
        echo "Install command and app|runtime|false"
    fi
}

# "$1" stage name -> its manifest record fields (title|category|needs_user_input).
stage_record() {
    case "$1" in
        prerequisites) echo "System prerequisites|runtime|false" ;;
        repository)    echo "Download Hermes Agent|runtime|false" ;;
        venv)          echo "Create Python environment|runtime|false" ;;
        python-deps)   echo "Install Python dependencies|runtime|false" ;;
        config)        echo "Prepare config and skills|configuration|false" ;;
        products)      products_record ;;
        setup)         echo "Configure API keys and settings|configuration|true" ;;
        gateway)       echo "Configure gateway service|configuration|true" ;;
        desktop)       echo "Build desktop app|runtime|false" ;;
        complete)      echo "Finish install|runtime|false" ;;
    esac
}

emit_manifest() {
    printf '%s' '{"protocol_version":1,"stages":['
    _sep=""
    for _s in $(stage_names); do
        IFS='|' read -r _title _category _needs <<< "$(stage_record "$_s")"
        printf '%s{"name":"%s","title":"%s","category":"%s","needs_user_input":%s}' \
            "$_sep" "$_s" "$_title" "$_category" "$_needs"
        _sep=","
    done
    printf '%s\n' ']}'
}

stage_prerequisites() {
    command -v git >/dev/null 2>&1 || fail "git is required. Install it with your system package manager."
    command -v curl >/dev/null 2>&1 || fail "curl is required. Install it with your system package manager."
    # PM's Node on musl is the unofficial-builds musl archive, which links the
    # system libstdc++; without it every node/npm stage fails verification.
    if [[ "$(uv_bootstrap_target 2>/dev/null)" == *-musl ]]; then
        local _libdir _stdcxx=""
        for _libdir in /lib /usr/lib /usr/local/lib; do
            compgen -G "$_libdir/libstdc++.so.6*" >/dev/null && { _stdcxx=yes; break; }
        done
        [ -n "$_stdcxx" ] || fail "musl host: the Node.js runtime needs the system libstdc++. Install it (Alpine: apk add libstdc++, Void: xbps-install libstdc++) and re-run."
    fi
    log_success "prerequisites ok (git, curl)"
}

stage_repository() {
    # An interrupted clone from an older installer can leave a .git with no
    # initial commit, where stash/checkout abort ("You do not have the
    # initial commit yet", #40998). Move it aside -- never delete it, it may
    # hold something the user wants -- and clone fresh below.
    if [ -d "$INSTALL_DIR/.git" ] && ! git -C "$INSTALL_DIR" rev-parse --verify HEAD >/dev/null 2>&1; then
        local broken
        broken="${INSTALL_DIR}.broken-$(date -u +%Y%m%d-%H%M%S)"
        log_warn "$INSTALL_DIR has no commits (interrupted clone); moving it aside to $broken"
        mv "$INSTALL_DIR" "$broken" || fail "cannot move $INSTALL_DIR aside"
    fi
    if [ -d "$INSTALL_DIR/.git" ]; then
        log "Updating $INSTALL_DIR ($BRANCH)"
        # An explicit HERMES_REPO_URL names the source for reruns too, not
        # just the first clone.
        if [ -n "${HERMES_REPO_URL:-}" ]; then
            git -C "$INSTALL_DIR" remote set-url origin "$REPO_URL" || fail "cannot point origin at $REPO_URL"
        fi
        # Explicit refspec: a tag-pinned --single-branch checkout from an older
        # installer maps only the tag, so a by-name fetch writes FETCH_HEAD and
        # never the origin/$BRANCH everything below resolves (#125112).
        run_logged "Fetching origin/$BRANCH" git -C "$INSTALL_DIR" fetch origin "+refs/heads/$BRANCH:refs/remotes/origin/$BRANCH" \
            || fail "git fetch failed"
        local stamp
        stamp="$(date -u +%Y%m%d-%H%M%S)"
        # Park local work BEFORE switching branches: checkout refuses a dirty
        # tree that conflicts, and the reset below would discard it. Work
        # that cannot be parked stops the install -- never overwrite it.
        if [ -n "$(git -C "$INSTALL_DIR" status --porcelain)" ]; then
            # An interrupted update can leave unmerged index entries, where
            # stash aborts ("could not write index"). Dropping only the
            # index-level conflict state keeps the working-tree changes for
            # the stash below (#4735).
            if [ -n "$(git -C "$INSTALL_DIR" ls-files --unmerged)" ]; then
                log_warn "clearing unmerged index entries from a previous conflict"
                git -C "$INSTALL_DIR" reset -q || fail "cannot clear the unmerged index in $INSTALL_DIR"
            fi
            run_logged "Stashing local changes" \
                git -C "$INSTALL_DIR" stash push --include-untracked -m "hermes-install-autostash-$stamp" \
                || fail "could not stash local changes in $INSTALL_DIR; commit or move them aside, then rerun"
            log_warn "local changes stashed as hermes-install-autostash-$stamp"
        fi
        # checkout's branch guess only sees remote refs the refspec maps, so a
        # narrow checkout (detached at its tag, no local branch) gets the branch
        # created at the fetched tip.
        if git -C "$INSTALL_DIR" show-ref --verify --quiet "refs/heads/$BRANCH"; then
            run_logged "Checking out $BRANCH" git -C "$INSTALL_DIR" checkout "$BRANCH" || fail "git checkout failed"
        else
            run_logged "Checking out $BRANCH" git -C "$INSTALL_DIR" checkout -b "$BRANCH" "origin/$BRANCH" \
                || fail "git checkout failed"
        fi
        if ! run_logged --may-fail "Fast-forwarding to origin/$BRANCH" \
            git -C "$INSTALL_DIR" merge --ff-only "origin/$BRANCH"; then
            # A release cut off the main line, a force-pushed remote, or the
            # user's own commits cannot fast-forward. Every stage below reads
            # files only the new tree has (pm/), so an install left on the old
            # tree cannot finish -- match the remote the way `hermes update`
            # does, after parking the old tip.
            # Only commits absent from origin need a rescue ref. Keep the same
            # namespace as `hermes update` so its pruning and recovery work.
            local dropped rescue_kind rescue_ref prior
            dropped="$(git -C "$INSTALL_DIR" rev-list --count "origin/$BRANCH..HEAD")" \
                || fail "cannot count commits before reset"
            if [ "$dropped" -gt 0 ]; then
                rescue_kind="diverged"
                git -C "$INSTALL_DIR" merge-base HEAD "origin/$BRANCH" >/dev/null 2>&1 \
                    || rescue_kind="orphan"
                prior="$(git -C "$INSTALL_DIR" rev-parse --short=12 HEAD)" \
                    || fail "cannot identify commits before reset"
                rescue_ref="refs/hermes-update-backups/$rescue_kind-$BRANCH-$stamp-$prior"
                git -C "$INSTALL_DIR" update-ref "$rescue_ref" HEAD \
                    || fail "cannot back up $dropped local commit(s); refusing to reset"
                log_warn "$dropped commit(s) not on origin/$BRANCH backed up to $rescue_ref"
                log "List them with: git -C \"$INSTALL_DIR\" log origin/$BRANCH..$rescue_ref"
            fi
            run_logged "Resetting to origin/$BRANCH" git -C "$INSTALL_DIR" reset --hard "origin/$BRANCH" \
                || fail "git reset failed"
            log_warn "not fast-forwardable; reset to origin/$BRANCH"
        fi
    else
        # `mv <clone> <existing dir>` nests the checkout INSIDE it as
        # <dir>/tree, so a pre-existing destination must be empty (we take
        # the empty dir over) or we refuse: whatever lives there is not ours.
        if [ -e "$INSTALL_DIR" ] || [ -L "$INSTALL_DIR" ]; then
            if [ -d "$INSTALL_DIR" ] && [ ! -L "$INSTALL_DIR" ] && [ -z "$(ls -A "$INSTALL_DIR")" ]; then
                rmdir "$INSTALL_DIR" || fail "cannot replace empty $INSTALL_DIR"
            else
                fail "$INSTALL_DIR exists and is not a Hermes git checkout. Move it aside, or install elsewhere with --dir <path>."
            fi
        fi
        mkdir -p "$(dirname "$INSTALL_DIR")"
        local staged attempt label cloned=false progress=()
        # Phase lines ("Receiving objects: 42%") feed the status line; git
        # prints none to a pipe unless asked.
        if quiet_output; then progress=(--progress); fi
        staged="$(mktemp -d "$(dirname "$INSTALL_DIR")/.hermes-clone-XXXXXX")" || fail "cannot stage clone"
        for attempt in 1 2 3; do
            # Treeless: every commit and release tag (runtime identity is the
            # nearest reachable release; --commit pins and branch switches
            # still resolve), trees and blobs fetched on demand, so the
            # download stays close to a --depth 1 clone.
            label="Cloning $REPO_URL ($BRANCH) into $INSTALL_DIR"
            [ "$attempt" = 1 ] || label="$label (attempt $attempt of 3)"
            if run_logged "$label" git clone ${progress[@]+"${progress[@]}"} \
                --filter=tree:0 --branch "$BRANCH" "$REPO_URL" "$staged/tree"; then
                cloned=true
                break
            fi
            rm -rf "$staged/tree"
            [ "$attempt" = 3 ] || sleep "$((attempt * 5))"
        done
        if [ "$cloned" = false ]; then
            # The checkout step is where throttled downloads die: clone the
            # graph alone, then retry materializing the tree separately.
            log_warn "direct clone failed; trying deferred checkout"
            if run_logged "Cloning history" git clone ${progress[@]+"${progress[@]}"} \
                --filter=tree:0 --no-checkout --branch "$BRANCH" "$REPO_URL" "$staged/tree"; then
                for attempt in 1 2; do
                    if run_logged "Checking out files (attempt $attempt of 2)" \
                        git -C "$staged/tree" reset --hard HEAD; then
                        cloned=true
                        break
                    fi
                    [ "$attempt" = 2 ] || sleep 5
                done
            fi
        fi
        if [ "$cloned" = false ]; then
            rm -rf "$staged"
            fail "git clone failed; no checkout published"
        fi
        if ! mv "$staged/tree" "$INSTALL_DIR"; then
            rm -rf "$staged"
            fail "cannot publish cloned checkout"
        fi
        rmdir "$staged"
        log_success "Hermes Agent cloned"
    fi
    if [ -n "$INSTALL_COMMIT" ]; then
<<<<<<< HEAD
        # A commit pin must never move an existing install BACKWARDS. The
        # bootstrap installer bakes its build-time commit into the binary
        # (BUILD_PIN_COMMIT) and passes it as --commit on every install-mode
        # run -- including the one the desktop's failure screen retries. An
        # installer built months ago would otherwise rewind a current checkout
        # to its build commit, stranding the user on ancient code with a
        # current venv. Only pin when the target is not already an ancestor of
        # HEAD; a fresh clone has no such ancestry and pins normally.
        if ! git cat-file -e "$INSTALL_COMMIT^{commit}" 2>/dev/null; then
            git fetch origin "$INSTALL_COMMIT" || true
        fi
        if git rev-parse --verify --quiet HEAD >/dev/null 2>&1 \
           && git merge-base --is-ancestor "$INSTALL_COMMIT" HEAD 2>/dev/null \
           && [ "$(git rev-parse "$INSTALL_COMMIT^{commit}" 2>/dev/null)" != "$(git rev-parse HEAD)" ]; then
            if [ "$FORCE_COMMIT" = true ]; then
                log_warn "--force-commit: rolling this install back to $INSTALL_COMMIT."
                git checkout --detach "$INSTALL_COMMIT"
            else
                log_warn "Ignoring --commit $INSTALL_COMMIT: the checkout is already newer."
                log_warn "Pinning to it would roll this install back. Pass --force-commit to override."
            fi
        else
            log_info "Pinning checkout to commit $INSTALL_COMMIT..."
            git checkout --detach "$INSTALL_COMMIT"
        fi
    fi

    log_success "Repository ready"
}

setup_venv() {
    if [ "$USE_VENV" = false ]; then
        log_info "Skipping virtual environment (--no-venv)"
        return 0
    fi

    if [ "$DISTRO" = "termux" ]; then
        log_info "Creating virtual environment with Termux Python..."

        if [ -d "venv" ]; then
            log_info "Virtual environment already exists, recreating..."
            rm -rf venv
        fi

        "$PYTHON_PATH" -m venv venv
        log_success "Virtual environment ready ($(./venv/bin/python --version 2>/dev/null))"
        return 0
    fi

    log_info "Creating virtual environment with Python $PYTHON_VERSION..."

    if [ -d "venv" ]; then
        log_info "Virtual environment already exists, recreating..."
        rm -rf venv
    fi

    # uv creates the venv and pins the Python version in one step
    $UV_CMD venv venv --python "$PYTHON_VERSION"

    # Neutralize any inherited UV_PYTHON (e.g. UV_PYTHON=3.14 left in the
    # user's shell env). uv honours UV_PYTHON over an existing venv for the
    # later `uv sync` / `uv pip install` tiers, so without this it would
    # silently delete this 3.11 venv and recreate it at the inherited
    # version — building Rust transitives that have no wheel for that
    # version from source via maturin, which fails. Pinning UV_PYTHON to the
    # interpreter we just created forces every subsequent uv command onto it.
    if [ -x "$INSTALL_DIR/venv/bin/python" ]; then
        export UV_PYTHON="$INSTALL_DIR/venv/bin/python"
    fi

    log_success "Virtual environment ready (Python $PYTHON_VERSION)"
}

install_deps() {
    log_info "Installing dependencies..."

    # Re-pin UV_PYTHON to the venv interpreter. setup_venv already does this,
    # but the bootstrap runs install stages (`venv`, `python-deps`) as separate
    # processes, so an export from setup_venv does NOT survive into a separate
    # python-deps invocation. Re-deriving it here covers that path. Without it,
    # an inherited UV_PYTHON=3.14 makes the uv sync/pip tiers below recreate the
    # venv at 3.14 and fail the maturin source build (no cp314 wheels yet).
    if [ "$DISTRO" != "termux" ] && [ -x "$INSTALL_DIR/venv/bin/python" ]; then
        export UV_PYTHON="$INSTALL_DIR/venv/bin/python"
    fi

    if [ "$DISTRO" = "termux" ]; then
        if [ "$USE_VENV" = true ]; then
            export VIRTUAL_ENV="$INSTALL_DIR/venv"
            PIP_PYTHON="$INSTALL_DIR/venv/bin/python"
        else
            PIP_PYTHON="$PYTHON_PATH"
        fi

        if [ -z "${ANDROID_API_LEVEL:-}" ]; then
            ANDROID_API_LEVEL="$(getprop ro.build.version.sdk 2>/dev/null || true)"
            if [ -z "$ANDROID_API_LEVEL" ]; then
                ANDROID_API_LEVEL=24
            fi
            export ANDROID_API_LEVEL
            log_info "Using ANDROID_API_LEVEL=$ANDROID_API_LEVEL for Android wheel builds"
        fi

        "$PIP_PYTHON" -m pip install --upgrade pip setuptools wheel >/dev/null

        # On Android, psutil's setup.py rejects sys.platform == 'android' before
        # it ever invokes the C build, so the next pip install would fail at
        # "platform android is not supported".  Prebuild psutil from the official
        # sdist with a one-line marker patch (Linux source path is fine on
        # Android).  Stopgap until psutil#2762 ships upstream.
        if "$PIP_PYTHON" -c 'import sys; raise SystemExit(0 if sys.platform == "android" else 1)' 2>/dev/null; then
            log_info "Android Python detected: prebuilding psutil compatibility shim..."
            if ! "$PIP_PYTHON" "$INSTALL_DIR/scripts/install_psutil_android.py" --pip "$PIP_PYTHON -m pip"; then
                log_warn "psutil Android prebuild failed — package install will likely fail next."
                log_info "Workaround: manually rerun 'python scripts/install_psutil_android.py' once your toolchain is set up."
            fi
        fi

        # Try the broad Termux profile first (best-effort "install all" for Android),
        # then fall back to the conservative Termux baseline, then base package.
        if ! "$PIP_PYTHON" -m pip install -e '.[termux-all]' -c constraints-termux.txt; then
            log_warn "Termux broad profile (.[termux-all]) failed, trying baseline Termux profile..."
            if ! "$PIP_PYTHON" -m pip install -e '.[termux]' -c constraints-termux.txt; then
                log_warn "Termux baseline profile (.[termux]) failed, trying base install..."
                if ! "$PIP_PYTHON" -m pip install -e '.' -c constraints-termux.txt; then
                    log_error "Package installation failed on Termux."
                    log_info "Ensure these packages are installed: pkg install clang rust make pkg-config libffi openssl ca-certificates curl"
                    log_info "Then re-run: cd $INSTALL_DIR && python -m pip install -e '.[termux-all]' -c constraints-termux.txt"
                    exit 1
                fi
            fi
        fi

        log_success "Main package installed"
        log_info "Termux note: matrix e2ee and local faster-whisper extras are excluded from .[termux-all] due to upstream Android wheel/toolchain blockers."
        log_info "Termux note: browser/WhatsApp tooling is not installed by default; see the Termux guide for optional follow-up steps."

        log_success "All dependencies installed"
        return 0
    fi

    if [ "$USE_VENV" = true ]; then
        # Tell uv to install into our venv (no need to activate)
        export VIRTUAL_ENV="$INSTALL_DIR/venv"
    fi

    # On Debian/Ubuntu (including WSL), some Python packages need build tools.
    # Check and offer to install them if missing.
    if [ "$DISTRO" = "ubuntu" ] || [ "$DISTRO" = "debian" ]; then
        local need_build_tools=false
        for pkg in gcc python3-dev libffi-dev; do
            if ! dpkg -s "$pkg" &>/dev/null; then
                need_build_tools=true
                break
            fi
        done
        if [ "$need_build_tools" = true ]; then
            log_info "Some build tools may be needed for Python packages..."
            if command -v sudo &> /dev/null; then
                if sudo -n true 2>/dev/null; then
                    sudo DEBIAN_FRONTEND=noninteractive NEEDRESTART_MODE=a apt-get update -qq && sudo DEBIAN_FRONTEND=noninteractive NEEDRESTART_MODE=a apt-get install -y -qq build-essential python3-dev libffi-dev >/dev/null 2>&1 || true
                    log_success "Build tools installed"
                else
                    log_info "sudo is needed ONLY to install build tools (build-essential, python3-dev, libffi-dev) via apt."
                    log_info "Hermes Agent itself does not require or retain root access."
                    if prompt_yes_no "Install build tools?" "yes"; then
                        sudo DEBIAN_FRONTEND=noninteractive NEEDRESTART_MODE=a apt-get update -qq && sudo DEBIAN_FRONTEND=noninteractive NEEDRESTART_MODE=a apt-get install -y -qq build-essential python3-dev libffi-dev >/dev/null 2>&1 || true
                        log_success "Build tools installed"
                    fi
                fi
            fi
        fi
    fi

    # Install the main package in editable mode with all extras.
    #
    # Hash-verified install (Tier 0) — when uv.lock is present, prefer
    # `uv sync --locked`. The lockfile records SHA256 hashes for every
    # transitive, so a compromised transitive (different hash than what
    # we shipped) is REJECTED by the resolver. This is the *only* path
    # that protects against the "direct dep is fine, but the dep's dep
    # got worm-poisoned overnight" failure mode. All `uv pip install`
    # tiers below re-resolve transitives fresh from PyPI without any
    # hash verification — they exist to keep installs working when the
    # lockfile is stale, missing, or out-of-sync with the current
    # extras spec, NOT because they're equivalent in posture.
    if [ -f "uv.lock" ]; then
        log_info "Trying tier: hash-verified (uv.lock) ..."
        log_info "(this resolves + downloads the curated [all] set — first run on a"
        log_info " fresh venv can take 1-5 minutes; uv prints progress below)"
        # Stream uv's progress directly to the user instead of swallowing
        # it with `2>"$(mktemp)"`.  Two reasons:
        #   1. `--extra all --locked` against a fresh venv has to pull
        #      every transitive — silencing stderr makes the install
        #      look frozen for minutes on slow networks. Users see
        #      "Trying tier: hash-verified ..." and assume it's hung.
        #   2. The previous `2>"$(mktemp)"` substituted the path at
        #      command-build time but never saved it, so on failure the
        #      uv error message was unreachable — the user just got the
        #      generic "lockfile may be stale" warning.
        #
        # Critical flag choice: `--extra all`, NOT `--all-extras`.
        #   --all-extras = every [project.optional-dependencies] key.
        #                  This bypasses the curated `[all]` extra
        #                  entirely and pulls e.g. [matrix] (which
        #                  needs python-olm + make on Windows) and
        #                  [rl] (git+https deps that fail offline).
        #   --extra all  = install just the `[all]` extra's contents.
        #                  This respects the curation in pyproject.toml.
        # uv's own progress UI handles TTY detection and downgrades
        # gracefully when stdout/stderr aren't terminals.
        if UV_PROJECT_ENVIRONMENT="$INSTALL_DIR/venv" $UV_CMD sync --extra all --locked; then
            log_success "Main package installed (hash-verified via uv.lock)"
            log_success "All dependencies installed"
            return 0
        fi
        log_warn "uv.lock sync failed (see uv output above), falling back to PyPI resolve..."
    else
        log_info "uv.lock not found — falling back to PyPI resolve (no hash verification)"
    fi

    # Multi-tier fallback. The point of the tiers is that ONE compromised
    # PyPI package (a worm-poisoned release that gets quarantined, like
    # mistralai 2.4.6 in May 2026) shouldn't be able to silently demote a
    # fresh install all the way down to "core only" — the user should keep
    # everything else they signed up for.
    #
    # Tier 1: [all] — the curated extra in pyproject.toml.
    # Tier 2: [all] minus the currently-broken extras list (_BROKEN_EXTRAS).
    #         Edit _BROKEN_EXTRAS below when something on PyPI breaks; this
    #         lets users keep the rest of [all] when one transitive is
    #         unavailable. The list of [all]'s contents is parsed from
    #         pyproject.toml at runtime — there is NO hand-mirrored copy
    #         to drift out of sync. If you want to change what [all]
    #         contains, edit pyproject.toml only.
    # Tier 3: bare `.` — last-resort so at least the core CLI launches.
    #         Skipped tiers like "PyPI-only extras (no git deps)" used to
    #         exist to dodge [rl] / [matrix] git+sdist deps; those are no
    #         longer in [all] post-2026-05-12 lazy-install migration, so
    #         a separate PyPI-only tier had no remaining content.
    local _BROKEN_EXTRAS=()  # populate when an extra becomes unresolvable

    # Parse [project.optional-dependencies].all from pyproject.toml.
    # tomllib is stdlib on Python 3.11+ which uv's bootstrap guarantees.
    # Falls back to a hand list if parse fails — defensive only.
    local _ALL_EXTRAS_CSV
    _ALL_EXTRAS_CSV="$(
        "$PYTHON_PATH" - <<'PY' 2>/dev/null
import re, sys, tomllib
try:
    with open("pyproject.toml", "rb") as fh:
        data = tomllib.load(fh)
    specs = data["project"]["optional-dependencies"]["all"]
    extras = []
    for s in specs:
        m = re.search(r"hermes-agent\[([\w-]+)\]", s)
        if m:
            extras.append(m.group(1))
    print(",".join(extras))
except Exception as e:
    print("", file=sys.stderr)
    sys.exit(1)
PY
    )"
    if [ -z "$_ALL_EXTRAS_CSV" ]; then
        log_warn "Could not parse [all] from pyproject.toml; falling back to .[all] only."
        _ALL_EXTRAS_CSV=""
    fi

    # Build "[all] minus broken" spec by filtering the parsed list.
    local _SAFE_SPEC=".[all]"
    if [ -n "$_ALL_EXTRAS_CSV" ] && [ "${#_BROKEN_EXTRAS[@]}" -gt 0 ]; then
        local _SAFE_EXTRAS=()
        local _e _b _skip
        IFS=',' read -ra _ALL_EXTRAS_ARR <<< "$_ALL_EXTRAS_CSV"
        for _e in "${_ALL_EXTRAS_ARR[@]}"; do
            _skip=false
            for _b in "${_BROKEN_EXTRAS[@]}"; do
                if [ "$_e" = "$_b" ]; then _skip=true; break; fi
            done
            if [ "$_skip" = false ]; then _SAFE_EXTRAS+=("$_e"); fi
        done
        _SAFE_SPEC=".[$(IFS=,; echo "${_SAFE_EXTRAS[*]}")]"
    fi

    ALL_INSTALL_LOG=$(mktemp)
    local _installed=false
    local _tier_name=""

    install_tier() {
        local name="$1"; local spec="$2"
        log_info "Trying tier: $name ..."
        if $UV_CMD pip install -e "$spec" 2>"$ALL_INSTALL_LOG"; then
            log_success "Main package installed ($name)"
            _installed=true
            _tier_name="$name"
            return 0
        fi
        log_warn "Tier '$name' failed. Top of pip output:"
        head -5 "$ALL_INSTALL_LOG" | sed 's/^/    /' >&2
        return 1
    }

    install_tier "all" ".[all]" \
        || install_tier "all minus known-broken (${_BROKEN_EXTRAS[*]:-none})" "$_SAFE_SPEC" \
        || install_tier "core only (no extras)" "."

    rm -f "$ALL_INSTALL_LOG"

    if [ "$_installed" = false ]; then
        log_error "Package installation failed even with no extras."
        log_info "Check that build tools are installed: sudo apt install build-essential python3-dev"
        log_info "Then re-run: cd $INSTALL_DIR && uv pip install -e '.[all]'"
        exit 1
    fi

    if [ "$_tier_name" != "all (with RL/matrix extras)" ]; then
        log_warn "Note: installed via fallback tier ($_tier_name)."
        log_info "Some optional features may be missing. After resolving any"
        log_info "PyPI/network issue, re-run: $UV_CMD pip install -e '.[all]'"
    fi

    log_success "Main package installed"

    log_success "All dependencies installed"
}

setup_path() {
    log_info "Setting up hermes command..."

    if [ "$USE_VENV" = true ]; then
        HERMES_BIN="$INSTALL_DIR/venv/bin/python"
        HERMES_ENTRYPOINT="$INSTALL_DIR/hermes"
    else
        HERMES_BIN="$(which hermes 2>/dev/null || echo "")"
        if [ -z "$HERMES_BIN" ]; then
            log_warn "hermes not found on PATH after install"
            return 0
        fi
    fi

    # Verify the interpreter and the checked-in entrypoint needed by the launcher.
    if [ ! -x "$HERMES_BIN" ] || { [ "$USE_VENV" = true ] && [ ! -f "$HERMES_ENTRYPOINT" ]; }; then
        log_warn "Hermes launcher prerequisites not found"
        log_info "This usually means the Python package install didn't complete successfully."
        if [ "$DISTRO" = "termux" ]; then
            log_info "Try: cd $INSTALL_DIR && python -m pip install -e '.[termux-all]' -c constraints-termux.txt"
        else
            log_info "Try: cd $INSTALL_DIR && uv pip install -e '.[all]'"
        fi
        return 0
    fi

    local command_link_dir
    local command_link_display_dir
    command_link_dir="$(get_command_link_dir)"
    command_link_display_dir="$(get_command_link_display_dir)"

    # Create a user-facing shim for the hermes command.
    # We intentionally clear PYTHONPATH/PYTHONHOME here so inherited env vars
    # can't make this launcher import modules from another checkout.
    mkdir -p "$command_link_dir"
    # Older installs created this path as a symlink to $HERMES_BIN. Without
    # the rm, `cat >` follows the symlink and overwrites the venv pip entry
    # point with this shim — making `exec "$HERMES_BIN"` self-recurse. (#21454)
    rm -f "$command_link_dir/hermes"
    if [ "$USE_VENV" = true ]; then
        # uv-generated console scripts resolve themselves through `realpath`,
        # which stock macOS does not provide. Run the checked-in entrypoint
        # with the venv interpreter instead, so the public launcher remains
        # independent of non-standard shell utilities.
        cat > "$command_link_dir/hermes" <<EOF
#!/usr/bin/env bash
unset PYTHONPATH
unset PYTHONHOME
exec "$HERMES_BIN" "$HERMES_ENTRYPOINT" "\$@"
EOF
    else
        cat > "$command_link_dir/hermes" <<EOF
#!/usr/bin/env bash
unset PYTHONPATH
unset PYTHONHOME
exec "$HERMES_BIN" "\$@"
EOF
    fi
    chmod +x "$command_link_dir/hermes"
    log_success "Installed hermes launcher → $command_link_display_dir/hermes"

    # Also expose `hermes-agent`. The `hermes-agent` console script declared in
    # pyproject.toml's [project.scripts] lives inside the venv, which is not on
    # the login-shell PATH. Without this launcher users can't invoke the agent
    # entrypoint directly from outside the venv. (#74819)
    rm -f "$command_link_dir/hermes-agent"
    if [ "$USE_VENV" = true ]; then
        cat > "$command_link_dir/hermes-agent" <<EOF
#!/usr/bin/env bash
unset PYTHONPATH
unset PYTHONHOME
exec "$HERMES_BIN" "$INSTALL_DIR/run_agent.py" "\$@"
EOF
    else
        cat > "$command_link_dir/hermes-agent" <<EOF
#!/usr/bin/env bash
unset PYTHONPATH
unset PYTHONHOME
exec "$HERMES_BIN" run_agent.py "\$@"
EOF
    fi
    chmod +x "$command_link_dir/hermes-agent"
    log_success "Installed hermes-agent launcher → $command_link_display_dir/hermes-agent"

    # Also expose `hermes-acp`. ACP hosts (Zed, JetBrains, Buzz) resolve the
    # agent by command name on the login-shell PATH, and the `hermes-acp`
    # console script lives inside the venv, which is not on that PATH. Without
    # this launcher those hosts report Hermes as not installed. (#21454 applies
    # here too: clear the path first so `cat >` cannot follow an old symlink
    # into the venv and overwrite the console script.)
    rm -f "$command_link_dir/hermes-acp"
    if [ "$USE_VENV" = true ]; then
        cat > "$command_link_dir/hermes-acp" <<EOF
#!/usr/bin/env bash
unset PYTHONPATH
unset PYTHONHOME
exec "$HERMES_BIN" "$HERMES_ENTRYPOINT" acp "\$@"
EOF
    else
        cat > "$command_link_dir/hermes-acp" <<EOF
#!/usr/bin/env bash
unset PYTHONPATH
unset PYTHONHOME
exec "$HERMES_BIN" acp "\$@"
EOF
    fi
    chmod +x "$command_link_dir/hermes-acp"
    log_success "Installed hermes-acp launcher → $command_link_display_dir/hermes-acp"

    if [ "$DISTRO" = "termux" ]; then
        export PATH="$command_link_dir:$PATH"
        log_info "$command_link_display_dir is the native Termux command path"
        log_success "hermes command ready"
        return 0
    fi

    # FHS layout: /usr/local/bin is normally on PATH for login shells (via
    # /etc/profile pathmunge), but on RHEL/CentOS/Rocky/Alma 8+ non-login
    # interactive root shells (su, sudo -s, tmux panes, some web terminals)
    # only source /etc/bashrc, which does NOT add /usr/local/bin — and
    # /root/.bash_profile doesn't either.  So verify with `command -v` and
    # fall back to writing a PATH guard into /root/.bashrc when needed.
    if [ "$ROOT_FHS_LAYOUT" = true ]; then
        export PATH="$command_link_dir:$PATH"
        # Probe a fresh non-login interactive bash the way the user will use it.
        # `bash -i -c` sources ~/.bashrc but NOT ~/.bash_profile or /etc/profile,
        # which is the exact scenario where RHEL root loses /usr/local/bin.
        if env -i HOME="$HOME" TERM="${TERM:-dumb}" bash -i -c 'command -v hermes' \
                >/dev/null 2>&1; then
            log_info "/usr/local/bin is already on PATH for all shells"
            log_success "hermes command ready"
            return 0
        fi

        log_info "hermes not on PATH in non-login shells (common on RHEL-family)"
        PATH_LINE='export PATH="/usr/local/bin:$PATH"'
        PATH_COMMENT='# Hermes Agent — ensure /usr/local/bin is on PATH (RHEL non-login shells)'
        for SHELL_CONFIG in "$HOME/.bashrc" "$HOME/.bash_profile"; do
            [ -f "$SHELL_CONFIG" ] || continue
            if ! grep -v '^[[:space:]]*#' "$SHELL_CONFIG" 2>/dev/null \
                    | grep -qE 'PATH=.*(/usr/local/bin|\$command_link_dir)'; then
                echo "" >> "$SHELL_CONFIG"
                echo "$PATH_COMMENT" >> "$SHELL_CONFIG"
                echo "$PATH_LINE" >> "$SHELL_CONFIG"
                log_success "Added /usr/local/bin to PATH in $SHELL_CONFIG"
            fi
        done
        log_success "hermes command ready"
        return 0
    fi

    # Check if ~/.local/bin is on PATH; if not, add it to shell config.
    # Detect the user's actual login shell (not the shell running this script,
    # which is always bash when piped from curl).
    if ! echo "$PATH" | tr ':' '\n' | grep -q "^$command_link_dir$"; then
        SHELL_CONFIGS=()
        IS_FISH=false
        LOGIN_SHELL="$(basename "${SHELL:-/bin/bash}")"
        case "$LOGIN_SHELL" in
            zsh)
                [ -f "$HOME/.zshrc" ] && SHELL_CONFIGS+=("$HOME/.zshrc")
                [ -f "$HOME/.zprofile" ] && SHELL_CONFIGS+=("$HOME/.zprofile")
                # If neither exists, create ~/.zshrc (common on fresh macOS installs)
                if [ ${#SHELL_CONFIGS[@]} -eq 0 ]; then
                    touch "$HOME/.zshrc"
                    SHELL_CONFIGS+=("$HOME/.zshrc")
                fi
                ;;
            bash)
                [ -f "$HOME/.bashrc" ] && SHELL_CONFIGS+=("$HOME/.bashrc")
                [ -f "$HOME/.bash_profile" ] && SHELL_CONFIGS+=("$HOME/.bash_profile")
                ;;
            fish)
                # fish uses ~/.config/fish/config.fish and fish_add_path — not export PATH=
                IS_FISH=true
                FISH_CONFIG="$HOME/.config/fish/config.fish"
                mkdir -p "$(dirname "$FISH_CONFIG")"
                touch "$FISH_CONFIG"
                ;;
            *)
                [ -f "$HOME/.bashrc" ] && SHELL_CONFIGS+=("$HOME/.bashrc")
                [ -f "$HOME/.zshrc" ] && SHELL_CONFIGS+=("$HOME/.zshrc")
                ;;
        esac
        # Also ensure ~/.profile has it (sourced by login shells on
        # Ubuntu/Debian/WSL even when ~/.bashrc is skipped)
        [ "$IS_FISH" = "false" ] && [ -f "$HOME/.profile" ] && SHELL_CONFIGS+=("$HOME/.profile")

        PATH_LINE='export PATH="$HOME/.local/bin:$PATH"'

        for SHELL_CONFIG in "${SHELL_CONFIGS[@]}"; do
            if ! grep -v '^[[:space:]]*#' "$SHELL_CONFIG" 2>/dev/null | grep -qE 'PATH=.*\.local/bin'; then
                echo "" >> "$SHELL_CONFIG"
                echo "# Hermes Agent — ensure ~/.local/bin is on PATH" >> "$SHELL_CONFIG"
                echo "$PATH_LINE" >> "$SHELL_CONFIG"
                log_success "Added ~/.local/bin to PATH in $SHELL_CONFIG"
            fi
        done

        # fish uses fish_add_path instead of export PATH=...
        if [ "$IS_FISH" = "true" ]; then
            if ! grep -q 'fish_add_path.*\.local/bin' "$FISH_CONFIG" 2>/dev/null; then
                echo "" >> "$FISH_CONFIG"
                echo "# Hermes Agent — ensure ~/.local/bin is on PATH" >> "$FISH_CONFIG"
                echo 'fish_add_path "$HOME/.local/bin"' >> "$FISH_CONFIG"
                log_success "Added ~/.local/bin to PATH in $FISH_CONFIG"
            fi
        fi

        if [ "$IS_FISH" = "false" ] && [ ${#SHELL_CONFIGS[@]} -eq 0 ]; then
            log_warn "Could not detect shell config file to add ~/.local/bin to PATH"
            log_info "Add manually: $PATH_LINE"
        fi
    else
        log_info "~/.local/bin already on PATH"
    fi

    # Export for current session so hermes works immediately
    export PATH="$command_link_dir:$PATH"

    log_success "hermes command ready"
}

copy_config_templates() {
    log_info "Setting up configuration files..."

    # Create ~/.hermes directory structure (config at top level, code in subdir)
    mkdir -p "$HERMES_HOME"/{cron,sessions,logs,pairing,hooks,image_cache,audio_cache,memories,skills,scripts}

    # Create .env at ~/.hermes/.env (top level, easy to find)
    if [ ! -f "$HERMES_HOME/.env" ]; then
        if [ -f "$INSTALL_DIR/.env.example" ]; then
            cp "$INSTALL_DIR/.env.example" "$HERMES_HOME/.env"
            log_success "Created ~/.hermes/.env from template"
        else
            touch "$HERMES_HOME/.env"
            log_success "Created ~/.hermes/.env"
        fi
    else
        log_info "~/.hermes/.env already exists, keeping it"
    fi
    # Restrict .env permissions — this file holds API keys and tokens.
    # 0600 ensures only the file owner can read/write, matching standard
    # practice for credential files (.netrc, .aws/credentials, .ssh/config).
    chmod 600 "$HERMES_HOME/.env"
    configure_browser_env_from_system_browser

    # Create config.yaml at ~/.hermes/config.yaml (top level, easy to find)
    if [ ! -f "$HERMES_HOME/config.yaml" ]; then
        if [ -f "$INSTALL_DIR/cli-config.yaml.example" ]; then
            cp "$INSTALL_DIR/cli-config.yaml.example" "$HERMES_HOME/config.yaml"
            log_success "Created ~/.hermes/config.yaml from template"
        fi
    else
        log_info "~/.hermes/config.yaml already exists, keeping it"
    fi

    # Create SOUL.md if it doesn't exist (global persona file).
    # This MUST match DEFAULT_SOUL_MD in hermes_cli/default_soul.py — the
    # runtime (_ensure_default_soul_md) treats the old comment-only scaffold as
    # "never customized" and upgrades it to this text on next run, so any drift
    # here is self-healing, but keep them in sync to avoid a churn on first run.
    if [ ! -f "$HERMES_HOME/SOUL.md" ]; then
        cat > "$HERMES_HOME/SOUL.md" << 'SOUL_EOF'
You are Hermes Agent, an intelligent AI assistant created by Nous Research. You are helpful, knowledgeable, and direct. You assist users with a wide range of tasks including answering questions, writing and editing code, analyzing information, creative work, and executing actions via your tools. You communicate clearly, admit uncertainty when appropriate, and prioritize being genuinely useful over being verbose unless otherwise directed below. Be targeted and efficient in your exploration and investigations.
SOUL_EOF
        log_success "Created ~/.hermes/SOUL.md (edit to customize personality)"
    fi

    log_success "Configuration directory ready: ~/.hermes/"

    # Seed bundled skills into ~/.hermes/skills/ (manifest-based, one-time per skill)
    if [ "$NO_SKILLS" = true ]; then
        # Blank-slate install: write the opt-out marker and skip seeding.
        # skills_sync.py and `hermes update` both honor this marker, so the
        # default profile stays empty across future updates too.
        printf '%s\n' \
            "This profile opted out of bundled-skill seeding (installed with --no-skills)." \
            "Delete this file to re-enable sync on the next 'hermes update'." \
            > "$HERMES_HOME/.no-bundled-skills" 2>/dev/null || true
        log_info "Skipping bundled skills (--no-skills). Wrote $HERMES_HOME/.no-bundled-skills"
        log_info "  Future 'hermes update' runs will not inject bundled skills. Delete the marker to opt back in."
    else
        log_info "Syncing bundled skills to ~/.hermes/skills/ ..."
        if "$INSTALL_DIR/venv/bin/python" "$INSTALL_DIR/tools/skills_sync.py" 2>/dev/null; then
            log_success "Skills synced to ~/.hermes/skills/"
        else
            # Fallback: simple directory copy if Python sync fails
            if [ -d "$INSTALL_DIR/skills" ] && [ ! "$(ls -A "$HERMES_HOME/skills/" 2>/dev/null | grep -v '.bundled_manifest')" ]; then
                cp -r "$INSTALL_DIR/skills/"* "$HERMES_HOME/skills/" 2>/dev/null || true
                log_success "Skills copied to ~/.hermes/skills/"
            fi
        fi
=======
        # A pin must come from the branch being installed: the complete
        # marker records both, and a commit off that branch would make the
        # next plain rerun "update" onto a different line.
        git -C "$INSTALL_DIR" merge-base --is-ancestor "$INSTALL_COMMIT" "origin/$BRANCH" 2>/dev/null \
            || fail "commit $INSTALL_COMMIT is not on branch $BRANCH"
        run_logged "Pinning $INSTALL_COMMIT" git -C "$INSTALL_DIR" checkout "$INSTALL_COMMIT" \
            || fail "could not pin commit $INSTALL_COMMIT"
>>>>>>> upstream/main
    fi
}

stage_venv() {
    # Keep the installer stage protocol; PM alone creates dependency environments.
    local boot_py
    bootstrap_python
    log_success "bootstrap Python ready; PM prepares the dependency environment"
}

# Tool-only bootstrap: acquire uv and Python before PM's own dependencies exist.
# The application dependency graph is never installed in this interpreter.
bootstrap_python() {
    ensure_uv
    local _py
    # Read packages.python.version by following object names and braces, not
    # indentation — same pre-Python reader contract as setup-hermes.sh's pin().
    _py="$(awk -F '"' '
        /^[[:space:]]*("[^"]+"[[:space:]]*:[[:space:]]*)?\{/ { path[++depth] = $2; next }
        /^[[:space:]]*\}[[:space:]]*,?[[:space:]]*$/ { delete path[depth--]; next }
        path[2] == "packages" && path[3] == "python" && $2 == "version" && depth == 3 { print $4; exit }
    ' "$INSTALL_DIR/pm/lock.json" | cut -d+ -f1 | cut -d. -f1,2)"
    [ -n "$_py" ] || _py="3.14"
    # Only base interpreters qualify: an activated app venv must not become
    # PM's bootstrap parent. Prefer the existing managed Python, then a host
    # Python of the same supported minor before attempting a download (#10778).
    # This interpreter only boots PM; PM still owns the exact runtime pin.
    if ! boot_py="$(UV_SYSTEM_PYTHON=1 UV_NO_PROJECT=1 "$UV_CMD" python find --managed-python "$_py" 2>/dev/null)" \
        && ! boot_py="$("$UV_CMD" python find --system --no-project "$_py" 2>/dev/null)"; then
        run_logged "Downloading Python $_py" "$UV_CMD" python install --no-bin --no-registry "$_py" \
            || fail "bootstrap Python installation failed"
        boot_py="$(UV_SYSTEM_PYTHON=1 UV_NO_PROJECT=1 "$UV_CMD" python find --managed-python "$_py")" || fail "bootstrap Python lookup failed"
    fi
    boot_py="${boot_py%$'\r'}"
    [ -x "$boot_py" ] && "$boot_py" --version >/dev/null 2>&1 || fail "bootstrap Python is not executable: $boot_py"
}

# uv exits before PM can replace its tool entry. pm.cli then prepares and
# enters its independently locked runtime before mutating application deps.
bootstrap_pm() {
    local boot_py
    local pm_args=(install)
    # PM records the opt-outs, so later installs and `hermes update` keep the
    # tools off until `hermes pm install <name>` opts back in.
    [ "$SKIP_BROWSER" = true ] && pm_args+=(--without agent-browser)
    [ "$SKIP_COMPUTER_USE" = true ] && pm_args+=(--without cua-driver)
    bootstrap_python
    (cd "$INSTALL_DIR" && run_logged "Installing dependencies (hash-verified via uv.lock)" \
        "$boot_py" -m pm.cli "${pm_args[@]}") \
        || fail "pm install failed"
    log_success "dependencies installed"
}

stage_python_deps() {
    bootstrap_pm
}

desktop_product_present() {
    # Does this checkout already carry a built desktop app? A plain repair or
    # upgrade rerun on a desktop install must REBUILD it rather than leave a
    # bundle built from the previous code: the app is part of that install, and
    # the artifacts live inside the tree (gitignored), so an update makes them
    # stale instead of removing them.
    # electron-builder suffixes the output dir with the arch on every non-x64
    # target (linux-arm64-unpacked, mac-arm64, win-arm64-unpacked), so the x64
    # names alone miss a desktop build on ARM64 Linux/Windows (#94703).
    local release="$INSTALL_DIR/apps/desktop/release" dir
    for dir in linux-unpacked linux-arm64-unpacked mac mac-arm64 \
               win-unpacked win-ia32-unpacked win-arm64-unpacked; do
        [ -d "$release/$dir" ] && return 0
    done
    return 1
}

# Guarded: a login shell whose ~/.bash_profile sources ~/.bashrc (Fedora, RHEL)
# reads both files, so an unconditional prepend would put ~/.local/bin on PATH twice.
SHELL_PATH_LINE='case ":$PATH:" in *":$HOME/.local/bin:"*) ;; *) export PATH="$HOME/.local/bin:$PATH" ;; esac'
SHELL_PATH_SETUP_RE='^[[:space:]]*([^#[:space:]].*)?PATH=.*\.local/bin'

append_shell_path() {
    local rc="$1" line="$2" pattern="$3"
    # Existing user PATH setup wins; never append another line on a repair run.
    if [ -f "$rc" ] && grep -E "$pattern" "$rc" >/dev/null 2>&1; then
        return 0
    fi
    mkdir -p "$(dirname "$rc")"
    printf '\n# Hermes Agent command\n%s\n' "$line" >> "$rc" || fail "cannot update PATH in $rc"
    log_success "added ~/.local/bin to PATH in $rc"
}

wire_shell_path() {
    # The launcher is published by source_completion; shell rc files belong to
    # the installer, not to PM (updates must not modify a user's shell setup).
    # Never use the installer's inherited PATH as a proxy for a *new* shell.
    local login_shell="${SHELL:-/bin/bash}"
    case "${login_shell##*/}" in
        zsh)
            append_shell_path "$HOME/.zshrc" "$SHELL_PATH_LINE" "$SHELL_PATH_SETUP_RE"
            append_shell_path "$HOME/.zprofile" "$SHELL_PATH_LINE" "$SHELL_PATH_SETUP_RE"
            ;;
        fish)
            append_shell_path "$HOME/.config/fish/config.fish" 'fish_add_path "$HOME/.local/bin"' '^[[:space:]]*fish_add_path.*\.local/bin'
            ;;
        *)
            append_shell_path "$HOME/.bashrc" "$SHELL_PATH_LINE" "$SHELL_PATH_SETUP_RE"
            append_shell_path "$HOME/.profile" "$SHELL_PATH_LINE" "$SHELL_PATH_SETUP_RE"
            # Bash prefers .bash_profile over .profile if both exist.
            if [ -f "$HOME/.bash_profile" ]; then
                append_shell_path "$HOME/.bash_profile" "$SHELL_PATH_LINE" "$SHELL_PATH_SETUP_RE"
            fi
            ;;
    esac
}

stage_products() {
    # The whole tail in one place, by calling the completion an update calls:
    # publish the commands, build the products (tui/web, plus the desktop app),
    # then run the post-build maintenance that syncs bundled skills and migrates
    # config. Node, browsers and the frontend build tools arrive through pm as
    # the build asks for them; the bootstrap interpreter itself only re-enters
    # the tree on PM's selected Python.
    local boot_py
    local args=(--source "$INSTALL_DIR")
    bootstrap_python
    if [ "$INCLUDE_DESKTOP" = true ] || desktop_product_present; then
        args+=(--desktop)
    fi
    (cd "$INSTALL_DIR" && run_logged "Building the hermes command and apps" \
        "$boot_py" -I -B -X utf8 hermes_cli/source_completion.py "${args[@]}") \
        || fail "app products or command publication failed"
    wire_shell_path
    log_success "app products and hermes command ready"
}

stage_desktop() {
    # External-caller contract: `--stage desktop` stays dispatchable on its own
    # (the manifest never lists it now -- --include-desktop selects the desktop
    # product inside `products`). Same completion call, desktop selected.
    INCLUDE_DESKTOP=true
    stage_products
}

stage_config() {
    mkdir -p "$HERMES_HOME"/cron "$HERMES_HOME"/sessions "$HERMES_HOME"/logs \
        "$HERMES_HOME"/pairing "$HERMES_HOME"/hooks "$HERMES_HOME"/image_cache \
        "$HERMES_HOME"/audio_cache "$HERMES_HOME"/memories "$HERMES_HOME"/skills
    if [ ! -f "$HERMES_HOME/.env" ]; then
        cp "$INSTALL_DIR/.env.example" "$HERMES_HOME/.env" 2>/dev/null || touch "$HERMES_HOME/.env"
    fi
    chmod 600 "$HERMES_HOME/.env"
    if [ ! -f "$HERMES_HOME/config.yaml" ] && [ -f "$INSTALL_DIR/cli-config.yaml.example" ]; then
        cp "$INSTALL_DIR/cli-config.yaml.example" "$HERMES_HOME/config.yaml"
    fi
    log_success "config prepared in $HERMES_HOME"
}

# Interactive stages read the terminal, not stdin: under `curl | bash` stdin
# IS the script. Probe by opening /dev/tty -- a Docker build has the device
# node in its mount namespace but opening it fails (ENXIO).
has_terminal() { (: </dev/tty) 2>/dev/null; }

stage_setup() {
    if [ "$NON_INTERACTIVE" = true ]; then return 0; fi
    if ! has_terminal; then
        log "setup skipped (no terminal); run 'hermes setup' after install"
        return 0
    fi
    "$INSTALL_DIR/.hermes/bin/hermes" setup </dev/tty || fail "setup failed"
}

stage_gateway() {
    if [ "$NON_INTERACTIVE" = true ]; then return 0; fi
    if ! has_terminal; then
        log "gateway setup skipped (no terminal); run 'hermes gateway install' after install"
        return 0
    fi
    # Setup installs the service when it handles the gateway; ask only if it did not.
    "$INSTALL_DIR/.hermes/bin/hermes" gateway install --if-missing </dev/tty || fail "gateway installation failed"
}

stage_complete() {
    local commit
    commit="$INSTALL_COMMIT"
    [ -n "$commit" ] || commit=$(git -C "$INSTALL_DIR" rev-parse HEAD 2>/dev/null) || commit=""
    if [ -n "$commit" ]; then
        printf '{\n  "schemaVersion": 1,\n  "pinnedCommit": "%s",\n  "pinnedBranch": "%s",\n  "completedAt": "%s"\n}\n' \
            "$commit" "$BRANCH" "$(date -u +%Y-%m-%dT%H:%M:%S.000Z)" > "$INSTALL_DIR/.hermes-bootstrap-complete.tmp"
        mv -f "$INSTALL_DIR/.hermes-bootstrap-complete.tmp" "$INSTALL_DIR/.hermes-bootstrap-complete"
    fi
    log_success "Hermes Agent install complete. Run: hermes"
}

print_path_reload_hint() {
    # The rc files only reach shells started later, and this installer is
    # always a child (`curl | bash`, `bash install.sh`) that cannot change its
    # parent's PATH. The inherited PATH is the parent's, so it says whether
    # the user can run `hermes` right away.
    case ":$PATH:" in *":$HOME/.local/bin:"*|*":$HOME/.local/bin/:"*) return 0 ;; esac
    local rc
    local login_shell="${SHELL:-}"
    case "${login_shell##*/}" in
        zsh) rc="source ~/.zshrc" ;;
        fish) rc="source ~/.config/fish/config.fish" ;;
        bash|"") rc="source ~/.bashrc" ;;
        *) rc=". ~/.profile" ;;
    esac
    log "Reload your shell to use hermes: open a new terminal, or run: $rc"
}

run_stage() (
    # Keep failure handling out of conditional calls, which disable errexit.
    set -e
    STAGE="$1"
    STAGE_REASON=""
    STAGE_SKIPPED=false
    trap 'stage_result "$?"' EXIT
    if [ "$NON_INTERACTIVE" = true ] && { [ "$STAGE" = setup ] || [ "$STAGE" = gateway ]; }; then
        STAGE_SKIPPED=true
        STAGE_REASON="needs user input"
        exit 0
    fi
    case "$1" in
        prerequisites) stage_prerequisites ;;
        repository) stage_repository ;;
        venv) stage_venv ;;
        python-deps) stage_python_deps ;;
        config) stage_config ;;
        products) stage_products ;;
        setup) stage_setup ;;
        gateway) stage_gateway ;;
        desktop) stage_desktop ;;
        complete) stage_complete ;;
        *) STAGE_REASON="unknown stage: $1"; printf '%s\n' "$STAGE_REASON" >&2; exit 2 ;;
    esac
)

# Main. Guarded so the script can be SOURCED for its functions (the
# installer-test harness sources it with --manifest, which must define
# the functions and stop before main). Under `curl | bash` BASH_SOURCE is
# empty, and `set -u` would abort on the bare expansion.
if [ "${BASH_SOURCE[0]:-$0}" = "$0" ]; then
    if [ "$WANT_MANIFEST" = true ]; then
        emit_manifest
        exit 0
    fi

    if [ -n "$STAGE" ] && [ "$JSON" = true ]; then
        trap 'stage_result "$?"' EXIT
    fi
    check_platform
    trap - EXIT

    if [ -n "$STAGE" ]; then
        run_stage "$STAGE"
        exit "$?"
    fi

    # No --stage: run the whole ladder — the same authoritative list the
    # manifest prints, so --include-desktop inserts desktop here too.
    print_banner
    for s in $(stage_names); do
        run_stage "$s"
        rc=$?
        [ "$rc" -eq 0 ] || exit "$rc"
    done
    print_path_reload_hint
fi
