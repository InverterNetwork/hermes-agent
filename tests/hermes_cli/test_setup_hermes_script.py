from pathlib import Path
import re
import subprocess


REPO_ROOT = Path(__file__).resolve().parents[2]
SETUP_SCRIPT = REPO_ROOT / "setup-hermes.sh"
INSTALLER_SCRIPT = REPO_ROOT / "installer" / "setup-hermes.sh"


def test_setup_hermes_script_is_valid_shell():
    result = subprocess.run(["bash", "-n", str(SETUP_SCRIPT)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_installer_script_is_valid_shell():
    result = subprocess.run(["bash", "-n", str(INSTALLER_SCRIPT)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_setup_hermes_script_has_termux_path():
    content = SETUP_SCRIPT.read_text(encoding="utf-8")

    assert "is_termux()" in content
    assert ".[termux]" in content
    assert "constraints-termux.txt" in content
    assert "$PREFIX/bin" in content
    assert "Skipping tinker-atropos on Termux" in content


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
    """Configs-as-code guard: setup-hermes.sh must call render-quay-config
    with --force (no first-install gate) so quay.agent_invocation and the
    rest of the quay.* block stay in sync with deploy.values.yaml on every
    install. The earlier gate let updated invocations silently drift on
    re-runs — workers kept running the seeded-on-first-install command.
    """
    content = INSTALLER_SCRIPT.read_text(encoding="utf-8")
    # Allow any whitespace/line-continuations between the subcommand and the
    # --force flag, but require they appear in the same invocation block.
    pattern = r'render-quay-config[^\n]*--out[^\n]*"[^\n]*"\s*(?:\\\s*\n\s*)?--force'
    assert re.search(pattern, content), (
        "installer must invoke `render-quay-config --out … --force` so the "
        "quay config reconciles from deploy.values.yaml on every install"
    )
    # Belt-and-suspenders: the old preserve branch printed this exact phrase.
    # Its presence would mean the gate snuck back in.
    assert "$QUAY_CONFIG_OUT already present (preserving)" not in content


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
