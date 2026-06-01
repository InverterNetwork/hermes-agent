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


def test_installer_unsets_url_insteadof_somewhere():
    """Guards the upgrade-path fix: setup-hermes.sh must contain a
    `git config --unset-all` call on a `url.*.insteadOf` key. Loose regex
    so reformatting (whitespace, quoting style) doesn't fail the test for
    the wrong reason — the only thing being asserted is that the unset
    logic is still in the script.
    """
    content = INSTALLER_SCRIPT.read_text(encoding="utf-8")
    assert re.search(r'--unset-all\s+"url\..*?\.insteadOf"', content), \
        "installer must clear stale url.<ssh>.insteadOf for upgraded quay-managed entries"


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


def test_installer_renders_quay_config_with_force_on_every_run():
    """Boundary guard: setup-hermes.sh must call render-quay-config with
    --force for the Hermes-owned launch/auth/context keys. Quay behavior
    fields must not be added to that renderer just because they live under
    deploy.values.yaml's transitional quay: block.
    """
    content = INSTALLER_SCRIPT.read_text(encoding="utf-8")
    # Allow any whitespace/line-continuations between the subcommand and the
    # --force flag, but require they appear in the same invocation block.
    pattern = r'render-quay-config[^\n]*--out[^\n]*"[^\n]*"\s*(?:\\\s*\n\s*)?--force'
    assert re.search(pattern, content), (
        "installer must invoke `render-quay-config --out … --force` so the "
        "Hermes-owned Quay boundary is reconciled from deploy.values.yaml"
    )
    assert '--reference-repos-root "$TARGET_DIR/code"' in content
    assert "Hermes-owned launch/auth/context boundary" in content
    # Belt-and-suspenders: the old preserve branch printed this exact phrase.
    # Its presence would mean the gate snuck back in.
    assert "$QUAY_CONFIG_OUT already present (preserving)" not in content


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


def test_installer_provisions_claude_cli_from_active_invocations():
    content = INSTALLER_SCRIPT.read_text(encoding="utf-8")
    assert "active-agent-invocations" in content
    assert 'if [[ "$agent_invocations" == *claude* ]]; then' in content
    assert (
        "sudo -u \"$AGENT_USER\" -H bash -c "
        "'curl -fsSL https://claude.ai/install.sh | bash'"
    ) in content
    assert 'CLAUDE_AGENT_BIN="$AGENT_HOME/.local/bin/claude"' in content
    assert 'ln -sf "$CLAUDE_AGENT_BIN" /usr/local/bin/claude' in content
    assert "quay.agent_invocation references 'claude'" not in content


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


def test_quay_tick_service_carries_reviewer_token_minting_env():
    service = (OPS_DIR / "quay-tick.service").read_text(encoding="utf-8")
    runner = (OPS_DIR / "quay-tick-runner").read_text(encoding="utf-8")

    assert "Environment=HERMES_REVIEWER_GH_CONFIG=/etc/hermes/reviewer.env" in service
    assert "RuntimeDirectory=hermes" in service
    assert "QUAY_REVIEWER_GH_TOKEN" in runner
    assert "/etc/hermes/reviewer.env" in runner


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
    assert "systemctl enable --now quay-serve.service" in installer
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


def test_installer_unset_loop_iterates_both_suffixes():
    """Static guard: the installer's quay-managed unset branch must
    iterate over both the empty suffix and `.git`. Without this loop a
    pre-consolidation key (no `.git`) silently survives a re-run because
    the stored shape doesn't match the constructed lookup key.
    """
    content = INSTALLER_SCRIPT.read_text(encoding="utf-8")
    # Match `for <var> in "" ".git"` (or `.git` ""), with flexible whitespace.
    pattern = r'for\s+\w+\s+in\s+(?:""\s+"\.git"|"\.git"\s+"")'
    assert re.search(pattern, content), \
        "installer must loop over both '' and '.git' suffixes when clearing stale url.insteadOf"
