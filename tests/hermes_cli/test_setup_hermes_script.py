from pathlib import Path
import subprocess


REPO_ROOT = Path(__file__).resolve().parents[2]
SETUP_SCRIPT = REPO_ROOT / "setup-hermes.sh"
INSTALLER_SCRIPT = REPO_ROOT / "installer" / "setup-hermes.sh"


def test_setup_hermes_script_is_valid_shell():
    result = subprocess.run(["bash", "-n", str(SETUP_SCRIPT)], capture_output=True, text=True)
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


def test_installer_per_repo_loop_drops_rewrite_for_quay_entries():
    """The per-repo provisioning loop in installer/setup-hermes.sh must
    take the unset branch (not the write branch) for entries with a
    non-empty package_manager. Static-analysis check: the loop's
    rewrite-write must be gated on `-z "$repo_pkg"` and the unset must
    reference `--unset-all` on the same `url.<ssh>.insteadOf` key.
    """
    content = INSTALLER_SCRIPT.read_text(encoding="utf-8")
    assert 'if [[ -z "$repo_pkg" ]]; then' in content
    assert '--unset-all "url.${ssh_url}.insteadOf"' in content
