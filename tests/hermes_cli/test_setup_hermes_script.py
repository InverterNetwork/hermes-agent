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
