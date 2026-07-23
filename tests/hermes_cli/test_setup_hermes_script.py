from pathlib import Path
import re
import subprocess


REPO_ROOT = Path(__file__).resolve().parents[2]
SETUP_SCRIPT = REPO_ROOT / "setup-hermes.sh"
INSTALLER_SCRIPT = REPO_ROOT / "installer" / "setup-hermes.sh"
OPS_DIR = REPO_ROOT / "ops"


def test_setup_hermes_script_is_valid_shell():
    result = subprocess.run(["bash", "-n", str(SETUP_SCRIPT)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_installer_script_is_valid_shell():
    result = subprocess.run(["bash", "-n", str(INSTALLER_SCRIPT)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_installer_gateway_install_is_noninteractive():
    content = INSTALLER_SCRIPT.read_text(encoding="utf-8")

    assert '"$HERMES_BIN" gateway install --system \\' in content
    assert "--no-start-now --no-start-on-login" in content


def test_installer_manages_scripts_as_state_symlink():
    content = INSTALLER_SCRIPT.read_text(encoding="utf-8")

    assert "==> wiring state symlinks (skills, memories, cron, scripts)" in content
    assert "for d in skills memories cron scripts; do" in content
    assert 'if [[ ! -e "$STATE_TARGET/$d" ]]; then' in content
    assert 'install -d -o "$AGENT_USER" -g "$AGENT_USER" -m 755 "$STATE_TARGET/$d"' in content
    assert 'Move its contents into $STATE_TARGET/$d/ and retry' in content


def test_setup_hermes_script_has_termux_path():
    content = SETUP_SCRIPT.read_text(encoding="utf-8")

    assert "is_termux()" in content
    assert ".[termux]" in content
    assert "constraints-termux.txt" in content
    assert "$PREFIX/bin" in content
    assert "tested Android bundle" in content


def test_installer_unsets_stale_insteadof_for_quay_entries(tmp_path):
    """Re-running the installer must clear any url.insteadOf rewrite a
    pre-consolidation install wrote for a now-quay-managed github entry.
    Without this, git applies the rewrite before credential lookup and
    silently keeps pushing via the deploy key — the App helper is shadowed.
    """
    gitconfig = tmp_path / ".gitconfig"
    repo_url = "https://github.com/InverterNetwork/test-factory-code"
    ssh_url = "git@github.com:InverterNetwork/test-factory-code.git"

    subprocess.run(
        ["git", "config", "--file", str(gitconfig),
         f"url.{ssh_url}.insteadOf", repo_url],
        check=True,
    )
    assert subprocess.run(
        ["git", "config", "--file", str(gitconfig), "--get",
         f"url.{ssh_url}.insteadOf"],
        capture_output=True, text=True,
    ).stdout.strip() == repo_url

    # Two runs: first removes the stale entry; second must be a no-op
    # (exit 5 = key absent) so the installer's `|| true` swallow is correct.
    for _ in range(2):
        result = subprocess.run(
            ["git", "config", "--file", str(gitconfig),
             "--unset-all", f"url.{ssh_url}.insteadOf"],
            capture_output=True, text=True,
        )
        assert result.returncode in (0, 5), result.stderr

    assert subprocess.run(
        ["git", "config", "--file", str(gitconfig), "--get",
         f"url.{ssh_url}.insteadOf"],
        capture_output=True, text=True,
    ).returncode == 1


def test_stale_insteadof_can_be_removed_by_value_for_alias_host(tmp_path):
    """A stale deploy-key rewrite may use an arbitrary SSH host alias.

    The installer cannot reconstruct keys like `github-frontends` from
    deploy.values.yaml, so the upgrade path must remove rewrites by matching
    the `insteadOf` value for the quay-managed repo.
    """
    gitconfig = tmp_path / ".gitconfig"
    key = "url.git@github-frontends:InverterNetwork/iTRY-frontends.insteadOf"
    value = "git@github.com:InverterNetwork/iTRY-frontends.git"

    subprocess.run(
        ["git", "config", "--file", str(gitconfig), "--add", key, value],
        check=True,
    )

    listed = subprocess.run(
        ["git", "config", "--file", str(gitconfig), "--get-regexp",
         r"^url\..*\.insteadof$"],
        capture_output=True, text=True, check=True,
    ).stdout.strip().split(None, 1)
    assert listed == [key.replace("insteadOf", "insteadof"), value]

    subprocess.run(
        ["git", "config", "--file", str(gitconfig),
         "--unset-all", listed[0], listed[1]],
        check=True,
    )
    assert subprocess.run(
        ["git", "config", "--file", str(gitconfig), "--get", key],
        capture_output=True, text=True,
    ).returncode == 1


def test_installer_unsets_url_insteadof_somewhere():
    """Guards the upgrade-path fix: setup-hermes.sh must contain a
    `git config --unset-all` call on a `url.*.insteadOf` key. Loose regex
    so reformatting (whitespace, quoting style) doesn't fail the test for
    the wrong reason — the only thing being asserted is that the unset
    logic is still in the script.
    """
    content = INSTALLER_SCRIPT.read_text(encoding="utf-8")
    assert "--get-regexp '^url\\..*\\.insteadof$'" in content
    assert "stale_list_rc != 0 && stale_list_rc != 1" in content
    assert "listing stale url.insteadOf entries" in content
    assert re.search(r'--unset-all\s+"\$stale_key"\s+"\$stale_value"', content), \
        "installer must clear stale url.insteadOf entries by matched value"


def test_git_config_unset_all_keys_are_suffix_sensitive(tmp_path):
    """Documents the git semantics the installer's suffix-iterating loop
    relies on: `git config --unset-all` matches the stored key VERBATIM,
    so unsetting `url.<ssh>.insteadOf` with `.git` does NOT touch a key
    stored without `.git` (and vice versa) — that mismatch returns exit 5
    and leaves the stale rewrite in place. This is the failure mode that
    bit pre-consolidation hosts when the installer constructed the lookup
    key with the wrong suffix.

    Note: this test does not invoke the installer. The static guard below
    + the installer-smoke per-repo probe cover that the script actually
    iterates both shapes. This one pins the underlying git behavior, so a
    future git release that quietly normalized suffixes (which would make
    the loop redundant) would surface here.
    """
    gitconfig = tmp_path / ".gitconfig"
    org = "InverterNetwork"
    repo_short = "test-factory-code"
    repo_url = f"https://github.com/{org}/{repo_short}"
    no_suffix_key = f"url.git@github.com:{org}/{repo_short}.insteadOf"
    dot_git_key = f"url.git@github.com:{org}/{repo_short}.git.insteadOf"

    subprocess.run(
        ["git", "config", "--file", str(gitconfig), no_suffix_key, repo_url],
        check=True,
    )

    # Cross-suffix unset must NOT clear the no-suffix entry — that
    # silent exit-5 is the failure mode the suffix-iterating loop fixes.
    cross = subprocess.run(
        ["git", "config", "--file", str(gitconfig), "--unset-all", dot_git_key],
        capture_output=True, text=True,
    )
    assert cross.returncode == 5, (
        f"expected exit 5 (key absent) for cross-suffix unset, got {cross.returncode}"
    )
    assert subprocess.run(
        ["git", "config", "--file", str(gitconfig), "--get", no_suffix_key],
        capture_output=True, text=True,
    ).stdout.strip() == repo_url, "no-suffix entry must survive a .git-suffix unset"

    # Same-suffix unset clears it; a follow-up unset is a no-op (exit 5).
    same = subprocess.run(
        ["git", "config", "--file", str(gitconfig), "--unset-all", no_suffix_key],
        capture_output=True, text=True,
    )
    assert same.returncode == 0, f"same-suffix unset failed: {same.stderr}"
    assert subprocess.run(
        ["git", "config", "--file", str(gitconfig), "--get", no_suffix_key],
        capture_output=True, text=True,
    ).returncode == 1


def test_installer_creates_quay_config_once_then_preserves_it():
    """Boundary guard: setup-hermes.sh bootstraps quay/config.toml only when
    missing. Once present, Quay owns the whole file and installer reruns must
    not re-render selected runtime blocks or force-overwrite operator/Quay
    changes.
    """
    content = INSTALLER_SCRIPT.read_text(encoding="utf-8")
    assert 'if [[ ! -e "$QUAY_CONFIG_OUT" ]]; then' in content
    assert 'echo "==> preserving existing $QUAY_CONFIG_OUT"' in content
    assert '--reference-repos-root "$TARGET_DIR/code"' in content
    block = re.search(
        r'render-quay-config --out "\$QUAY_CONFIG_OUT"(?P<body>.*?)\n    chown ',
        content,
        re.S,
    )
    assert block is not None
    assert "--force" not in block.group("body")


def test_installer_reconciles_existing_quay_repo_metadata():
    """Existing Quay repo rows must be declaratively reconciled from
    deploy.values.yaml, not merely preserved after the first `repo add`.
    """
    content = INSTALLER_SCRIPT.read_text(encoding="utf-8")

    assert "==> reconciling quay repo $repo_id metadata" in content
    assert 'get-repo-config "$repo_id" --mode update' in content
    assert re.search(
        r'"\$QUAY_BIN_DST"\s+repo\s+update\s+\\\s*\n\s+--id "\$repo_id"\s+\\\s*\n\s+--input "\$repo_config_json"',
        content,
    )
    assert 'get-repo-config "$repo_id" --mode add' in content
    assert re.search(
        r'"\$QUAY_BIN_DST"\s+repo\s+add\s+\\\s*\n\s+--input "\$repo_config_json"',
        content,
    )
    assert "already registered (preserving)" not in content


def test_installer_translates_legacy_orchestrator_env_keys():
    """Legacy /etc/default/brix-orchestrator overrides must keep winning
    after migration. The new service sets QUAY_ORCHESTRATOR_CONFIG by default,
    so copied BRIX_* keys have to be renamed in /etc/default/quay-orchestrator.
    """
    content = INSTALLER_SCRIPT.read_text(encoding="utf-8")
    assert "/etc/default/brix-orchestrator /etc/default/quay-orchestrator" in content
    assert "sed -i \\" in content
    for name in (
        "CONFIG",
        "PYTHON",
        "SCRIPT",
        "ENABLED",
        "LOCK",
        "PROVIDER",
        "WORKER_ID",
    ):
        assert (
            f"s/BRIX_ORCHESTRATOR_{name}=/QUAY_ORCHESTRATOR_{name}=/g"
            in content
        )


def test_installer_provisions_agent_clis_for_quay():
    content = INSTALLER_SCRIPT.read_text(encoding="utf-8")
    assert "active-agent-invocations" not in content
    assert (
        "sudo -u \"$AGENT_USER\" -H bash -c "
        "'curl -fsSL https://claude.ai/install.sh | bash'"
    ) in content
    assert 'CLAUDE_AGENT_BIN="$AGENT_HOME/.local/bin/claude"' in content
    assert 'ln -sf "$CLAUDE_AGENT_BIN" /usr/local/bin/claude' in content
    assert (
        'ensure-codex --values "$VALUES_FILE" --agent-user "$AGENT_USER" --required'
        in content
    )
    assert 'if [[ "$QUAY_ENABLED" -eq 1 ]]; then' in content
    assert "quay.agent_invocation references 'claude'" not in content


def test_quay_tick_runtime_path_includes_managed_bins():
    content = (OPS_DIR / "quay-tick.service").read_text(encoding="utf-8")

    assert (
        "Environment=PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
        in content
    )


def test_installer_persists_quay_expected_sha_for_verify():
    """The installer must leave verify a SHA source of truth for the quay
    binary instead of forcing verify to trust `quay --version` output."""
    content = INSTALLER_SCRIPT.read_text(encoding="utf-8")
    assert "QUAY_EXPECTED_SHA" in content
    assert "SHA256SUM.expected" in content
    assert 'QUAY_EXPECTED_SHA_DIR="$TARGET_DIR/hermes-agent/installer/.state/quay"' in content
    assert 'QUAY_EXPECTED_SHA_DST="$QUAY_EXPECTED_SHA_DIR/SHA256SUM.expected"' in content
    assert 'install -d -o root -g root -m 0755 "$QUAY_EXPECTED_SHA_DIR"' in content
    assert re.search(
        r"install\s+-o\s+root\s+-g\s+root\s+-m\s+0644\s+/dev/stdin\s+\"\$QUAY_EXPECTED_SHA_DST\"",
        content,
    )
    assert '"$TARGET_DIR/quay/SHA256SUM.expected"' not in content


def test_installer_installs_dashboard_web_extra_when_quay_admin_public_url_is_set():
    """Hosted Quay Admin needs the dashboard runtime deps in the managed venv.

    Regression guard for the release deploy where the host-level dashboard
    service failed because the installer had only installed the [slack] extra.
    """
    content = INSTALLER_SCRIPT.read_text(encoding="utf-8")

    assert "QUAY_ADMIN_PUBLIC_BASE_URL" in content
    assert 'HERMES_INSTALL_EXTRAS="slack"' in content
    assert 'HERMES_INSTALL_EXTRAS="slack,web"' in content
    assert '"$TARGET_DIR/hermes-agent[$HERMES_INSTALL_EXTRAS]"' in content
    assert "hermes-dashboard.service" in content
    assert "quay.admin.public_base_url" in content
    assert "http://127.0.0.1:9119/quay/admin/" in content
    assert "hermes-dashboard.service did not return HTTP 401" in content
    assert "systemctl enable hermes-dashboard.service" in content
    assert "systemctl restart hermes-dashboard.service" in content
    assert "systemctl is-active --quiet hermes-dashboard.service" in content


def test_hermes_dashboard_service_is_loopback_quay_admin_proxy():
    service = OPS_DIR / "hermes-dashboard.service"
    content = service.read_text(encoding="utf-8")

    assert "After=network-online.target quay-serve.service" in content
    assert "Wants=network-online.target quay-serve.service" in content
    assert "Environment=HERMES_HOME=__TARGET_DIR__" in content
    assert "Environment=HERMES_WEB_DIST=__TARGET_DIR__/hermes-agent/hermes_cli/web_dist" in content
    assert "Environment=QUAY_ADMIN_BASE_URL=http://127.0.0.1:9731" in content
    assert "EnvironmentFile=__TARGET_DIR__/auth/quay.env" in content
    assert "EnvironmentFile=-__TARGET_DIR__/auth/gateway-runtime.env" in content
    assert re.search(
        r"ExecStart=.*\bhermes\s+dashboard\s+--host\s+127\.0\.0\.1\s+--port\s+9119\s+--no-open",
        content,
    )


def test_installer_uses_configured_quay_release_repo():
    content = INSTALLER_SCRIPT.read_text(encoding="utf-8")

    assert "lafawnduh1966/quay" not in content
    assert "get quay.release_repo" in content
    assert "QUAY_RELEASE_REPO" in content
    assert "https://github.com/${QUAY_RELEASE_REPO}/releases/download/${QUAY_VERSION}" in content
    assert "quay.release_repo must be a GitHub owner/repo slug" in content


def test_atlas_release_download_uses_authenticated_gh_path():
    content = INSTALLER_SCRIPT.read_text(encoding="utf-8")

    assert "get atlas.release_repo" in content
    assert "ATLAS_RELEASE_REPO" in content
    assert "gh release download \"$ATLAS_VERSION\"" in content
    assert "--repo \"$ATLAS_RELEASE_REPO\"" in content
    assert "GH_TOKEN=\"$ATLAS_RELEASE_TOKEN\"" in content
    assert "github.com/${ATLAS_RELEASE_REPO}/releases/download" not in content


def test_atlas_gws_install_is_version_and_checksum_pinned():
    content = INSTALLER_SCRIPT.read_text(encoding="utf-8")
    release_installer = (
        REPO_ROOT / "installer" / "install-gws-release"
    ).read_text(encoding="utf-8")
    values = (REPO_ROOT / "deploy.values.yaml").read_text(encoding="utf-8")

    assert 'GWS_VERSION" == "0.22.5"' in content
    assert "atlas.google_docs.minimum_atlas_version" in content
    assert 'python3 "$FORK_DIR/installer/check_atlas_gws_version.py"' in content
    assert 'ASSET="google-workspace-cli-${ARCH}-unknown-linux-gnu.tar.gz"' in release_installer
    assert "GWS_EXPECTED_SHA" in content
    assert 'bash "$FORK_DIR/installer/install-gws-release"' in content
    assert 'GWS_EXPECTED_SHA_DIR="$TARGET_DIR/hermes-agent/installer/.state/gws"' in content
    assert 'GWS_EXPECTED_SHA_DST="$GWS_EXPECTED_SHA_DIR/SHA256SUM.expected"' in content
    assert "os.O_RDONLY | os.O_NOFOLLOW" in content
    assert "os.fchown(credential.fileno(), 0," in content
    assert "os.fchmod(credential.fileno(), 0o640)" in content
    assert 'must resolve under $AUTH_DIR' in content
    assert '[[ -f "$ATLAS_GWS_CREDENTIALS_FILE" && ! -L "$ATLAS_GWS_CREDENTIALS_FILE" ]]' in content
    assert 'install -d -o "$AGENT_USER" -g "$AGENT_USER" -m 0755 -- "$TARGET_DIR/cache" "$ATLAS_CACHE_DIR"' in content
    assert 'sudo -u "$AGENT_USER" install -d -m 0700 -- "$ATLAS_GWS_CACHE_DIR"' in content
    assert '[[ -d "$ATLAS_GWS_CACHE_DIR" && ! -L "$ATLAS_GWS_CACHE_DIR" ]]' in content
    assert "https://github.com/${RELEASE_REPO}/releases/download/v${VERSION}/${ASSET}" in release_installer
    assert "gws_sha256:" in values
    assert "x86_64_linux_gnu:" in values
    assert "aarch64_linux_gnu:" in values
    assert "94490295d9580e1e88574e715a0a162991747d12d62f8c7b8dcc8268b6c1cea0" in values
    assert 'minimum_atlas_version: "0.1.16"' in values


def test_atlas_gitbook_source_sync_is_configured():
    values = (REPO_ROOT / "deploy.values.yaml").read_text(encoding="utf-8")
    installer = INSTALLER_SCRIPT.read_text(encoding="utf-8")
    service = (OPS_DIR / "atlas-source-sync.service").read_text(encoding="utf-8")
    runner = (OPS_DIR / "atlas-source-sync-runner").read_text(encoding="utf-8")
    wrapper = (OPS_DIR / "atlas-as-hermes").read_text(encoding="utf-8")
    profile = (OPS_DIR / "profile.d" / "atlas-env.sh").read_text(encoding="utf-8")

    assert "google_docs:" in values
    assert 'gws_version: "0.22.5"' in values
    assert "credentials_file: auth/atlas-google-authorized-user.json" in values
    assert "cache_dir: cache/atlas-gws" in values
    assert "de78ecdbd2f1a84cca0063a7ecbc440240fc14b6ebccbb17f4646b792a8c5c1f" in values
    assert "source_names:" in values
    assert "- emusd-docs" in values
    assert "- brix-product-docs" in values
    assert "- slack-channels" in values
    assert "brix-product-docs:" in values
    assert "type: gitbook" in values
    assert "space_id: JZwK8SE9GDTka1EcS5dD" in values
    assert "token_env: GITBOOK_API_TOKEN" in values
    # Slack scheduled sync (F1): the source is defined and scheduled, and the
    # token is referenced by env-var name only (never a value).
    assert "slack-channels:" in values
    assert "type: slack" in values
    assert "token_env: SLACK_ATLAS_TOKEN" in values

    assert "get atlas.source_sync.source_names" in installer
    assert "atlas.sources.$atlas_source_name.type" in installer
    assert "type must be github, gitbook, or slack" in installer
    assert "atlas.sources.$atlas_source_name.space_id" in installer
    assert "atlas.sources.$atlas_source_name.token_env" in installer
    assert "atlas.sources.$atlas_source_name.channel_ids" in installer
    assert 'ATLAS_SECRET_ENV="$AUTH_DIR/atlas.env"' in installer
    assert 'ATLAS_RUNTIME_ENV="$AUTH_DIR/atlas-runtime.env"' in installer
    assert "GOOGLE_WORKSPACE_CLI_CREDENTIALS_FILE" in installer
    assert "GOOGLE_WORKSPACE_CLI_CONFIG_DIR" in installer
    assert "ATLAS_GOOGLE_SERVICE_ACCOUNT_FILE" not in installer
    assert "service_account_file:" not in values
    assert "ATLAS_GOOGLE_SERVICE_ACCOUNT_FILE" not in wrapper
    assert 'realpath -m -- "$ATLAS_GWS_CREDENTIALS_FILE"' in installer
    assert 'realpath -m -- "$ATLAS_GWS_CACHE_DIR"' in installer

    assert "Environment=ATLAS_SYNC_SOURCE_NAMES=__ATLAS_SYNC_SOURCE_NAMES__" in service
    assert "EnvironmentFile=-__TARGET_DIR__/auth/atlas-runtime.env" in service
    assert "EnvironmentFile=-__TARGET_DIR__/auth/atlas.env" in service
    assert "ATLAS_SYNC_SOURCE_NAMES" in runner
    assert 'for source_name in "${source_names[@]}"' in runner
    assert 'sync source "$source_name"' in runner
    assert "__TARGET_DIR__/auth/atlas-runtime.env" in wrapper
    assert "__TARGET_DIR__/auth/atlas.env" in wrapper
    assert "__TARGET_DIR__/auth/atlas-runtime.env" in profile
    assert "__TARGET_DIR__/auth/atlas.env" in profile


def test_atlas_source_sync_full_reconciliation_is_scheduled():
    installer = INSTALLER_SCRIPT.read_text(encoding="utf-8")
    full_service = (OPS_DIR / "atlas-source-sync-full.service").read_text(encoding="utf-8")
    full_timer = (OPS_DIR / "atlas-source-sync-full.timer").read_text(encoding="utf-8")

    # Daily full reconciliation passes --full to the runner and carries a long
    # timeout for the whole-channel re-fetch; the hourly sync stays incremental.
    assert "ExecStart=/usr/local/sbin/atlas-source-sync-runner --full" in full_service
    assert "TimeoutStartSec=10800" in full_service
    assert "OnFailure=atlas-source-sync-failure.service" in full_service
    assert "OnCalendar=daily" in full_timer
    assert "Unit=atlas-source-sync-full.service" in full_timer

    # The installer renders + installs + enables the daily full timer.
    assert "atlas-source-sync-full.service" in installer
    assert "atlas-source-sync-full.timer" in installer
    assert "systemctl enable --now atlas-source-sync-full.timer" in installer


def test_atlas_hub_service_is_loopback_and_key_protected():
    values = (REPO_ROOT / "deploy.values.yaml").read_text(encoding="utf-8")
    installer = INSTALLER_SCRIPT.read_text(encoding="utf-8")
    service = (OPS_DIR / "atlas-hub.service").read_text(encoding="utf-8")

    assert "hub:" in values
    assert "enabled: true" in values
    assert "host: 127.0.0.1" in values
    assert "port: 8765" in values
    assert "public_base_url: https://didier.brix.fyi" in values
    assert "query_concurrency: 4" in values

    assert "ExecStart=/usr/local/bin/atlas --config __ATLAS_CONFIG__ serve --host __ATLAS_HUB_HOST__ --port __ATLAS_HUB_PORT__" in service
    assert "Environment=ATLAS_CONFIG=__ATLAS_CONFIG__" in service
    assert "Environment=ATLAS_KB_ROOT=__ATLAS_KB_ROOT__" in service
    assert "EnvironmentFile=-__TARGET_DIR__/auth/atlas.env" in service
    assert "EnvironmentFile=-__TARGET_DIR__/auth/atlas-runtime.env" in service
    assert "EnvironmentFile=-/etc/default/atlas-hub" in service

    assert 'ATLAS_HUB_AUTH_FILE="$AUTH_DIR/atlas-hub-auth.json"' in installer
    assert 'ATLAS_HUB_CLIENT_API_KEY_FILE="$AUTH_DIR/atlas-hub-client-api-key"' in installer
    assert '"$ATLAS_BIN_DST" hub keygen --scope v1:*' in installer
    assert "atlas.hub.host must be 127.0.0.1 or localhost" in installer
    assert "atlas-hub.service" in installer
    assert "systemctl enable --now atlas-hub.service" in installer
    assert "systemctl try-restart atlas-hub.service" in installer
    assert "http://$ATLAS_HUB_HOST:$ATLAS_HUB_PORT/v1/health" in installer
    assert "ensure-caddy-atlas-hub-route" in installer
    assert "caddy validate --config /etc/caddy/Caddyfile" in installer
    assert "$ATLAS_HUB_PUBLIC_BASE_URL/v1/health" in installer
    assert "Authorization: Bearer $atlas_hub_key" in installer


def test_quay_tick_service_carries_reviewer_token_minting_env():
    service = (OPS_DIR / "quay-tick.service").read_text(encoding="utf-8")
    runner = (OPS_DIR / "quay-tick-runner").read_text(encoding="utf-8")
    installer = INSTALLER_SCRIPT.read_text(encoding="utf-8")

    assert "Environment=HERMES_REVIEWER_GH_CONFIG=/etc/hermes/reviewer.env" in service
    assert "EnvironmentFile=-/etc/default/quay-worker-env" in service
    assert "quay-worker-env" in installer
    assert "RPC_URL_4326" in installer
    assert "RuntimeDirectory=hermes" in service
    assert "QUAY_REVIEWER_GH_TOKEN" in runner
    assert "/etc/hermes/reviewer.env" in runner
    assert "QUAY_GITHUB_AUTH_SRC=\"$OPS_DIR/quay-github-auth\"" in installer
    assert "install -o \"$AGENT_USER\" -g \"$AGENT_USER\" -m 0755 \"$QUAY_GITHUB_AUTH_SRC\" \"$QUAY_GITHUB_AUTH_DST\"" in installer


def test_quay_orchestrator_runner_prepares_github_auth_before_parked_polls():
    runner = (OPS_DIR / "quay-orchestrator-runner").read_text(encoding="utf-8")

    assert "quay-github-auth" in runner
    assert "quay_prepare_worker_github_auth" in runner
    assert "--park-human-waits" in runner


def test_quay_serve_service_is_localhost_and_token_protected():
    service = (OPS_DIR / "quay-serve.service").read_text(encoding="utf-8")
    installer = INSTALLER_SCRIPT.read_text(encoding="utf-8")
    stage = (REPO_ROOT / "stage-secrets.sh").read_text(encoding="utf-8")

    assert "ExecStart=/usr/local/bin/quay serve --host 127.0.0.1 --port 9731" in service
    assert "Environment=QUAY_DATA_DIR=__TARGET_DIR__/quay" in service
    assert "EnvironmentFile=-__TARGET_DIR__/auth/gateway-runtime.env" in service
    assert "EnvironmentFile=__TARGET_DIR__/auth/quay.env" in service
    assert "QUAY_ADMIN_TOKEN" in service
    assert "quay-serve.service" in installer
    assert '"$QUAY_BIN_DST" serve --help' in installer
    assert "QUAY_SERVE_PROBE_DIR" in installer
    assert "QUAY_DATA_DIR=$QUAY_SERVE_PROBE_DIR" in installer
    assert "QUAY_DATA_DIR=$TARGET_DIR/quay\" \"$QUAY_BIN_DST\" serve --help" not in installer
    assert "QUAY_SERVE_SUPPORTED" in installer
    assert "--enable-admin-auth" in installer
    assert "ensure_quay_admin_token" in installer
    assert "secrets.token_urlsafe(48)" in installer
    assert '[[ "$QUAY_ENABLED" -eq 1 && "$QUAY_SERVE_SUPPORTED" -eq 1' in installer
    assert "quay_serve_was_active=0" in installer
    assert "systemctl is-active quay-serve.service" in installer
    assert "systemctl enable --now quay-serve.service" in installer
    assert "systemctl try-restart quay-serve.service" in installer
    assert "QUAY_ADMIN_TOKEN=${existing_quay_admin_token}" in stage
    assert "API_SERVER_KEY=${existing_api_server_key}" in stage
    assert "QUAY_HERMES_API_KEY=${existing_api_server_key}" in stage


def test_installer_removes_legacy_reviewer_token_timer():
    content = INSTALLER_SCRIPT.read_text(encoding="utf-8")
    assert "systemctl disable --now hermes-reviewer-token.timer" in content
    assert "systemctl stop hermes-reviewer-token.service" in content
    assert "/etc/systemd/system/hermes-reviewer-token.service" in content
    assert "/etc/systemd/system/hermes-reviewer-token.timer" in content
    assert "systemctl enable --now hermes-reviewer-token.timer" not in content
    assert "systemctl start hermes-reviewer-token.service" not in content


def test_installer_stale_insteadof_values_cover_suffixes_and_ssh_shapes():
    """Static guard for the quay-managed stale-rewrite upgrade path."""
    content = INSTALLER_SCRIPT.read_text(encoding="utf-8")
    assert '"$repo_url"' in content
    assert '"${repo_url}.git"' in content
    assert '"git@github.com:${org}/${repo_short}"' in content
    assert '"git@github.com:${org}/${repo_short}.git"' in content
    assert '"ssh://git@github.com/${org}/${repo_short}"' in content
    assert '"ssh://git@github.com/${org}/${repo_short}.git"' in content


def test_installer_supports_local_hermes_state_quay_fixture():
    """CI uses --state without GitHub App auth for the hermes-state fixture."""
    installer = INSTALLER_SCRIPT.read_text(encoding="utf-8")
    verify = (REPO_ROOT / "installer" / "hermes_installer" / "verify.py").read_text(
        encoding="utf-8"
    )
    verify_cli = (
        REPO_ROOT / "installer" / "hermes_installer" / "__main__.py"
    ).read_text(encoding="utf-8")

    assert "_is_legacy_state_repo_fixture" in installer
    assert "_use_local_state_repo_source_for_quay" in installer
    assert "STATE_ORIGIN_URL" in installer
    assert "_alternate_clone_origin_urls" in installer
    assert 'if _use_local_state_repo_source_for_quay "$repo_id" "$repo_url"; then' in installer
    assert 'mapfile -t alternate_clone_origins' in installer
    assert '"${alternate_clone_origins[@]}"' in installer
    assert '"$repo_id" == "hermes-state"' in installer
    assert '"$repo_url" == "https://github.com/InverterNetwork/hermes-state"' in installer
    assert 'verify_args+=( --state "$STATE_DIR" )' in installer
    assert "_effective_repos_tsv(args, repos_tsv)" in verify
    assert 'repo_id == "hermes-state"' in verify
    assert 'repo_url == "https://github.com/InverterNetwork/hermes-state"' in verify
    assert '"--state"' in verify_cli
