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


def test_installer_unset_covers_both_suffix_variants(tmp_path):
    """Pre-consolidation installs wrote the rewrite key WITHOUT a .git
    suffix; the current write branch uses .git. The unset path must clear
    both shapes — otherwise the suffix mismatch silently no-ops (exit 5)
    and the stale rewrite keeps shadowing the App credential helper. This
    test runs the same suffix-iterating loop the installer uses against a
    no-suffix fixture (the krustentier upgrade shape).
    """
    gitconfig = tmp_path / ".gitconfig"
    org = "InverterNetwork"
    repo_short = "test-factory-code"
    repo_url = f"https://github.com/{org}/{repo_short}"
    no_suffix_key = f"url.git@github.com:{org}/{repo_short}.insteadOf"

    subprocess.run(
        ["git", "config", "--file", str(gitconfig), no_suffix_key, repo_url],
        check=True,
    )

    # Mirror the installer's loop: try both suffix variants, treating
    # exit 5 (key absent) as success.
    for suffix in ("", ".git"):
        ssh_url = f"git@github.com:{org}/{repo_short}{suffix}"
        result = subprocess.run(
            ["git", "config", "--file", str(gitconfig),
             "--unset-all", f"url.{ssh_url}.insteadOf"],
            capture_output=True, text=True,
        )
        assert result.returncode in (0, 5), (
            f"unset for suffix {suffix!r} returned {result.returncode}: {result.stderr}"
        )

    # Stale rewrite must be gone after the loop — proves the no-suffix
    # variant of the unset call is the one that did the work.
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
