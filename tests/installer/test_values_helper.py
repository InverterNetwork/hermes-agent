"""Tests for installer/values_helper.py — the YAML→shell helper invoked by
setup-hermes.sh. Covers edge cases the installer relies on: missing keys
must exit non-zero, unresolved manifest placeholders must be reported, and
render-runtime-config must always create a file (so setup-hermes.sh can
chown/chmod it without race conditions on no-op runs)."""

import json
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
HELPER = REPO_ROOT / "installer" / "values_helper.py"


def _run(values_path: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(HELPER), "--values", str(values_path), *args],
        capture_output=True,
        text=True,
    )


@pytest.fixture
def values_file(tmp_path: Path) -> Path:
    p = tmp_path / "deploy.values.yaml"
    p.write_text(
        "org:\n"
        "  name: TestOrg\n"
        "  agent_identity_name: alice\n"
        "slack:\n"
        "  app:\n"
        "    display_name: TestBot\n"
        "    description: testing\n"
        "    background_color: \"#000000\"\n"
        "    slash_command_name: testcmd\n"
        "    slash_command_description: Test description\n"
        "  runtime:\n"
        "    allowed_channels:\n"
        "      - C_TEST\n"
        "    require_mention: true\n"
        "    home_channel: C_HOME\n"
        "    channel_prompts: {}\n",
        encoding="utf-8",
    )
    return p


class TestGet:
    def test_scalar(self, values_file: Path):
        r = _run(values_file, "get", "org.agent_identity_name")
        assert r.returncode == 0
        assert r.stdout == "alice"

    def test_list_csv(self, values_file: Path):
        r = _run(values_file, "get", "slack.runtime.allowed_channels")
        assert r.returncode == 0
        assert r.stdout == "C_TEST"

    def test_bool(self, values_file: Path):
        r = _run(values_file, "get", "slack.runtime.require_mention")
        assert r.returncode == 0
        assert r.stdout == "true"

    def test_missing_key_exits_nonzero_no_stdout(self, values_file: Path):
        r = _run(values_file, "get", "does.not.exist")
        assert r.returncode == 1
        assert r.stdout == ""
        assert "key not found: does.not.exist" in r.stderr


class TestRenderManifest:
    def _tmpl(self, tmp_path: Path, body: str) -> Path:
        p = tmp_path / "manifest.tmpl"
        p.write_text(body, encoding="utf-8")
        return p

    def test_substitutes_slack_app_placeholders(self, values_file: Path, tmp_path: Path):
        tmpl = self._tmpl(
            tmp_path,
            '{"name": "${slack.app.display_name}", '
            '"cmd": "/${slack.app.slash_command_name}"}',
        )
        out = tmp_path / "manifest.json"
        r = _run(values_file, "render-manifest", "--in", str(tmpl), "--out", str(out))
        assert r.returncode == 0, r.stderr
        rendered = json.loads(out.read_text())
        assert rendered == {"name": "TestBot", "cmd": "/testcmd"}

    def test_unresolved_placeholder_exits_nonzero(
        self, values_file: Path, tmp_path: Path
    ):
        tmpl = self._tmpl(tmp_path, '{"x": "${slack.app.does_not_exist}"}')
        out = tmp_path / "manifest.json"
        r = _run(values_file, "render-manifest", "--in", str(tmpl), "--out", str(out))
        assert r.returncode == 1
        assert "unresolved placeholders" in r.stderr
        assert "${slack.app.does_not_exist}" in r.stderr
        assert not out.exists()

    def test_only_slack_app_keys_substitute(self, values_file: Path, tmp_path: Path):
        # org.* is in values.yaml but the manifest contract is slack.app.* only.
        tmpl = self._tmpl(tmp_path, '{"x": "${org.agent_identity_name}"}')
        out = tmp_path / "manifest.json"
        r = _run(values_file, "render-manifest", "--in", str(tmpl), "--out", str(out))
        assert r.returncode == 1
        assert "unresolved placeholders" in r.stderr

    def test_quote_in_value_is_json_escaped(self, tmp_path: Path):
        values = tmp_path / "values.yaml"
        values.write_text(
            "slack:\n"
            "  app:\n"
            '    description: \'I said "hi"\'\n',
            encoding="utf-8",
        )
        tmpl = self._tmpl(tmp_path, '{"d": "${slack.app.description}"}')
        out = tmp_path / "manifest.json"
        r = _run(values, "render-manifest", "--in", str(tmpl), "--out", str(out))
        assert r.returncode == 0, r.stderr
        # If quotes weren't escaped, json.loads would fail.
        assert json.loads(out.read_text()) == {"d": 'I said "hi"'}


class TestRenderRuntimeConfig:
    def test_writes_slack_block_with_allowed_channels(
        self, values_file: Path, tmp_path: Path
    ):
        out = tmp_path / "config.yaml"
        r = _run(values_file, "render-runtime-config", "--out", str(out))
        assert r.returncode == 0, r.stderr
        text = out.read_text()
        assert "allowed_channels:" in text
        assert "C_TEST" in text
        assert "require_mention: true" in text
        assert "home_channel: C_HOME" in text

    def test_preserves_existing_file_without_force(
        self, values_file: Path, tmp_path: Path
    ):
        out = tmp_path / "config.yaml"
        out.write_text("# operator hand-edits\nfoo: bar\n", encoding="utf-8")

        r = _run(values_file, "render-runtime-config", "--out", str(out))
        assert r.returncode == 0
        assert "preserved" in r.stdout
        assert out.read_text() == "# operator hand-edits\nfoo: bar\n"

    def test_force_overwrites_existing_file(self, values_file: Path, tmp_path: Path):
        out = tmp_path / "config.yaml"
        out.write_text("# stale\nfoo: bar\n", encoding="utf-8")

        r = _run(
            values_file, "render-runtime-config", "--out", str(out), "--force"
        )
        assert r.returncode == 0, r.stderr
        text = out.read_text()
        assert "foo: bar" not in text
        assert "C_TEST" in text

    def test_empty_allowed_channels_emits_empty_list(self, tmp_path: Path):
        values = tmp_path / "values.yaml"
        values.write_text(
            "slack:\n  runtime:\n    allowed_channels: []\n",
            encoding="utf-8",
        )
        out = tmp_path / "config.yaml"
        r = _run(values, "render-runtime-config", "--out", str(out))
        assert r.returncode == 0, r.stderr
        text = out.read_text()
        assert "allowed_channels: []" in text

    def test_empty_runtime_still_creates_file(self, tmp_path: Path):
        """The installer chowns/chmods the output path right after; the helper
        must always create a file, even when there's nothing to seed."""
        values = tmp_path / "values.yaml"
        values.write_text("slack:\n  runtime: {}\n", encoding="utf-8")
        out = tmp_path / "config.yaml"
        r = _run(values, "render-runtime-config", "--out", str(out))
        assert r.returncode == 0, r.stderr
        assert out.exists()
        text = out.read_text()
        assert text.startswith("# Seeded by setup-hermes.sh")

    def test_missing_runtime_section_still_creates_file(self, tmp_path: Path):
        values = tmp_path / "values.yaml"
        values.write_text("org:\n  name: X\n", encoding="utf-8")
        out = tmp_path / "config.yaml"
        r = _run(values, "render-runtime-config", "--out", str(out))
        assert r.returncode == 0, r.stderr
        assert out.exists()


class TestRenderQuayConfig:
    @pytest.fixture
    def quay_values(self, tmp_path: Path) -> Path:
        p = tmp_path / "values.yaml"
        p.write_text(
            "quay:\n"
            "  version: v0.1.0\n"
            '  agent_invocation: "claude < {prompt_file}"\n'
            "  adapters:\n"
            "    linear:\n"
            "      enabled: true\n"
            "      api_key_env: LINEAR_API_KEY\n"
            "    slack:\n"
            "      enabled: false\n",
            encoding="utf-8",
        )
        return p

    def test_writes_agent_invocation_and_linear_block(
        self, quay_values: Path, tmp_path: Path
    ):
        out = tmp_path / "config.toml"
        r = _run(quay_values, "render-quay-config", "--out", str(out))
        assert r.returncode == 0, r.stderr
        text = out.read_text()
        assert 'agent_invocation = "claude < {prompt_file}"' in text
        assert "[adapters.linear]" in text
        assert "enabled = true" in text
        assert 'api_key_env = "LINEAR_API_KEY"' in text

    def test_omits_data_dir_repos_root_and_version(
        self, quay_values: Path, tmp_path: Path
    ):
        out = tmp_path / "config.toml"
        r = _run(quay_values, "render-quay-config", "--out", str(out))
        assert r.returncode == 0, r.stderr
        text = out.read_text()
        # data_dir is set via QUAY_DATA_DIR in the systemd unit; repos_root
        # defaults to ${data_dir}/repos; version is consumed by the installer.
        assert "data_dir =" not in text
        assert "repos_root =" not in text
        assert "version =" not in text

    def test_slack_disabled_omits_section(
        self, quay_values: Path, tmp_path: Path
    ):
        out = tmp_path / "config.toml"
        r = _run(quay_values, "render-quay-config", "--out", str(out))
        assert r.returncode == 0, r.stderr
        assert "[adapters.slack]" not in out.read_text()

    def test_slack_enabled_renders_section(self, tmp_path: Path):
        values = tmp_path / "values.yaml"
        values.write_text(
            "quay:\n"
            '  agent_invocation: "claude < {prompt_file}"\n'
            "  adapters:\n"
            "    slack:\n"
            "      enabled: true\n"
            "      bot_token_env: QUAY_SLACK_TOKEN\n",
            encoding="utf-8",
        )
        out = tmp_path / "config.toml"
        r = _run(values, "render-quay-config", "--out", str(out))
        assert r.returncode == 0, r.stderr
        text = out.read_text()
        assert "[adapters.slack]" in text
        assert 'bot_token_env = "QUAY_SLACK_TOKEN"' in text

    def test_missing_agent_invocation_exits_nonzero(self, tmp_path: Path):
        values = tmp_path / "values.yaml"
        values.write_text("quay:\n  adapters: {}\n", encoding="utf-8")
        out = tmp_path / "config.toml"
        r = _run(values, "render-quay-config", "--out", str(out))
        assert r.returncode == 1
        assert "agent_invocation is required" in r.stderr
        assert not out.exists()

    def test_preserves_existing_file_without_force(
        self, quay_values: Path, tmp_path: Path
    ):
        out = tmp_path / "config.toml"
        out.write_text("# operator hand-edit\n", encoding="utf-8")
        r = _run(quay_values, "render-quay-config", "--out", str(out))
        assert r.returncode == 0
        assert "preserved" in r.stdout
        assert out.read_text() == "# operator hand-edit\n"

    def test_force_overwrites_existing_file(
        self, quay_values: Path, tmp_path: Path
    ):
        out = tmp_path / "config.toml"
        out.write_text("# stale\n", encoding="utf-8")
        r = _run(
            quay_values, "render-quay-config", "--out", str(out), "--force"
        )
        assert r.returncode == 0, r.stderr
        text = out.read_text()
        assert "# stale" not in text
        assert "agent_invocation" in text

    def test_quote_in_agent_invocation_is_escaped(self, tmp_path: Path):
        values = tmp_path / "values.yaml"
        values.write_text(
            "quay:\n"
            "  agent_invocation: 'claude --system \"be terse\" < {prompt_file}'\n",
            encoding="utf-8",
        )
        out = tmp_path / "config.toml"
        r = _run(values, "render-quay-config", "--out", str(out))
        assert r.returncode == 0, r.stderr
        # The TOML basic string must escape internal double-quotes; otherwise
        # the file is not valid TOML.
        text = out.read_text()
        assert r'\"be terse\"' in text


class TestListRepos:
    @pytest.fixture
    def repos_values(self, tmp_path: Path) -> Path:
        p = tmp_path / "values.yaml"
        p.write_text(
            "quay:\n"
            "  repos:\n"
            "    - id: alpha\n"
            "      url: https://github.com/example/alpha\n"
            "      base_branch: main\n"
            "      package_manager: bun\n"
            "      install_cmd: bun install\n"
            "    - id: beta\n"
            "      url: https://github.com/example/beta\n"
            "      base_branch: trunk\n"
            "      package_manager: npm\n"
            "      install_cmd: npm ci --no-audit\n",
            encoding="utf-8",
        )
        return p

    def test_emits_one_tsv_line_per_repo(self, repos_values: Path):
        r = _run(repos_values, "list-repos")
        assert r.returncode == 0, r.stderr
        lines = r.stdout.splitlines()
        assert lines == [
            "alpha\thttps://github.com/example/alpha\tmain\tbun\tbun install",
            "beta\thttps://github.com/example/beta\ttrunk\tnpm\tnpm ci --no-audit",
        ]

    def test_missing_repos_section_emits_nothing(self, tmp_path: Path):
        values = tmp_path / "values.yaml"
        values.write_text("quay:\n  version: v0.1.0\n", encoding="utf-8")
        r = _run(values, "list-repos")
        assert r.returncode == 0, r.stderr
        assert r.stdout == ""

    def test_missing_required_field_exits_nonzero(self, tmp_path: Path):
        # Missing install_cmd — should fail loudly so an under-specified
        # deploy.values.yaml doesn't silently skip the repo at install time.
        values = tmp_path / "values.yaml"
        values.write_text(
            "quay:\n"
            "  repos:\n"
            "    - id: alpha\n"
            "      url: https://github.com/example/alpha\n"
            "      base_branch: main\n"
            "      package_manager: bun\n",
            encoding="utf-8",
        )
        r = _run(values, "list-repos")
        assert r.returncode == 1
        assert "install_cmd is required" in r.stderr

    def test_empty_required_field_exits_nonzero(self, tmp_path: Path):
        values = tmp_path / "values.yaml"
        values.write_text(
            "quay:\n"
            "  repos:\n"
            '    - id: ""\n'
            "      url: https://github.com/example/alpha\n"
            "      base_branch: main\n"
            "      package_manager: bun\n"
            "      install_cmd: bun install\n",
            encoding="utf-8",
        )
        r = _run(values, "list-repos")
        assert r.returncode == 1
        assert "id is required" in r.stderr

    def test_tab_in_field_exits_nonzero(self, tmp_path: Path):
        # A tab inside a field would silently corrupt the bash-side parse.
        values = tmp_path / "values.yaml"
        values.write_text(
            "quay:\n"
            "  repos:\n"
            "    - id: alpha\n"
            "      url: https://github.com/example/alpha\n"
            "      base_branch: main\n"
            "      package_manager: bun\n"
            '      install_cmd: "bun\tinstall"\n',
            encoding="utf-8",
        )
        r = _run(values, "list-repos")
        assert r.returncode == 1
        assert "tab or newline" in r.stderr

    def test_repos_must_be_a_list(self, tmp_path: Path):
        values = tmp_path / "values.yaml"
        values.write_text(
            "quay:\n  repos: not-a-list\n", encoding="utf-8"
        )
        r = _run(values, "list-repos")
        assert r.returncode == 1
        assert "quay.repos must be a list" in r.stderr
