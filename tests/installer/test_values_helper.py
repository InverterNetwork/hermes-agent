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


class TestRenderGatewayRuntimeEnv:
    def test_writes_slack_allowed_users(self, tmp_path: Path):
        values = tmp_path / "values.yaml"
        values.write_text(
            "slack:\n  runtime:\n    allowed_users:\n      - U01ABC2DEF\n      - U02GHI3JKL\n",
            encoding="utf-8",
        )
        out = tmp_path / "gateway-runtime.env"
        r = _run(values, "render-gateway-runtime-env", "--out", str(out))
        assert r.returncode == 0, r.stderr
        text = out.read_text()
        assert "SLACK_ALLOWED_USERS=U01ABC2DEF,U02GHI3JKL" in text

    def test_empty_list_omits_line(self, tmp_path: Path):
        values = tmp_path / "values.yaml"
        values.write_text(
            "slack:\n  runtime:\n    allowed_users: []\n", encoding="utf-8"
        )
        out = tmp_path / "gateway-runtime.env"
        r = _run(values, "render-gateway-runtime-env", "--out", str(out))
        assert r.returncode == 0, r.stderr
        text = out.read_text()
        assert "SLACK_ALLOWED_USERS" not in text

    def test_missing_slack_section_still_creates_file(self, tmp_path: Path):
        # The installer chowns/chmods the output path right after; the helper
        # must always create a file, even with nothing to seed.
        values = tmp_path / "values.yaml"
        values.write_text("org:\n  name: X\n", encoding="utf-8")
        out = tmp_path / "gateway-runtime.env"
        r = _run(values, "render-gateway-runtime-env", "--out", str(out))
        assert r.returncode == 0, r.stderr
        assert out.exists()
        assert "SLACK_ALLOWED_USERS" not in out.read_text()

    def test_rejects_value_with_whitespace(self, tmp_path: Path):
        # Unquoted env-file lines silently truncate at whitespace; refuse to
        # emit a file the runtime would misread.
        values = tmp_path / "values.yaml"
        values.write_text(
            "slack:\n  runtime:\n    allowed_users:\n      - 'U bad has space'\n",
            encoding="utf-8",
        )
        out = tmp_path / "gateway-runtime.env"
        r = _run(values, "render-gateway-runtime-env", "--out", str(out))
        assert r.returncode == 1
        assert "whitespace" in r.stderr
        assert not out.exists()

    def test_rejects_non_list_allowed_users(self, tmp_path: Path):
        values = tmp_path / "values.yaml"
        values.write_text(
            "slack:\n  runtime:\n    allowed_users: 'U01ABC2DEF,U02GHI3JKL'\n",
            encoding="utf-8",
        )
        out = tmp_path / "gateway-runtime.env"
        r = _run(values, "render-gateway-runtime-env", "--out", str(out))
        assert r.returncode == 1
        assert "must be a list" in r.stderr

    def test_writes_linear_team_env_vars(self, tmp_path: Path):
        # linear.teams.<key> → LINEAR_TEAM_<KEY>
        values = tmp_path / "values.yaml"
        values.write_text(
            "linear:\n"
            "  teams:\n"
            "    itry: 28294e03-c1ce-4c2b-ba24-49e36179a321\n"
            "    aster: 11111111-2222-3333-4444-555555555555\n",
            encoding="utf-8",
        )
        out = tmp_path / "gateway-runtime.env"
        r = _run(values, "render-gateway-runtime-env", "--out", str(out))
        assert r.returncode == 0, r.stderr
        text = out.read_text()
        assert "LINEAR_TEAM_ITRY=28294e03-c1ce-4c2b-ba24-49e36179a321" in text
        assert "LINEAR_TEAM_ASTER=11111111-2222-3333-4444-555555555555" in text

    def test_skips_empty_team_value(self, tmp_path: Path):
        values = tmp_path / "values.yaml"
        values.write_text(
            "linear:\n  teams:\n    itry: \"\"\n    aster: ABC\n", encoding="utf-8"
        )
        out = tmp_path / "gateway-runtime.env"
        r = _run(values, "render-gateway-runtime-env", "--out", str(out))
        assert r.returncode == 0, r.stderr
        text = out.read_text()
        assert "LINEAR_TEAM_ITRY" not in text
        assert "LINEAR_TEAM_ASTER=ABC" in text

    def test_rejects_invalid_team_key(self, tmp_path: Path):
        values = tmp_path / "values.yaml"
        values.write_text(
            "linear:\n  teams:\n    '1bad': 28294e03-c1ce-4c2b-ba24-49e36179a321\n",
            encoding="utf-8",
        )
        out = tmp_path / "gateway-runtime.env"
        r = _run(values, "render-gateway-runtime-env", "--out", str(out))
        assert r.returncode == 1
        assert "is not a valid env-var suffix" in r.stderr

    def test_rejects_non_mapping_teams(self, tmp_path: Path):
        values = tmp_path / "values.yaml"
        values.write_text(
            "linear:\n  teams:\n    - itry\n    - aster\n", encoding="utf-8"
        )
        out = tmp_path / "gateway-runtime.env"
        r = _run(values, "render-gateway-runtime-env", "--out", str(out))
        assert r.returncode == 1
        assert "linear.teams must be a mapping" in r.stderr

    def test_always_rewrites_existing_file(self, tmp_path: Path):
        # Unlike render-runtime-config, this output is a reflection of
        # values.yaml; operator hand-edits don't survive.
        values = tmp_path / "values.yaml"
        values.write_text(
            "slack:\n  runtime:\n    allowed_users:\n      - U01ABC2DEF\n",
            encoding="utf-8",
        )
        out = tmp_path / "gateway-runtime.env"
        out.write_text("STALE_KEY=stale_value\n", encoding="utf-8")
        r = _run(values, "render-gateway-runtime-env", "--out", str(out))
        assert r.returncode == 0, r.stderr
        text = out.read_text()
        assert "STALE_KEY" not in text
        assert "SLACK_ALLOWED_USERS=U01ABC2DEF" in text


class TestMergeConfigModel:
    def _seed_config(self, path: Path, body: str) -> None:
        path.write_text(body, encoding="utf-8")

    def test_sets_provider_in_existing_config(self, tmp_path: Path):
        values = tmp_path / "values.yaml"
        values.write_text(
            "gateway:\n  model_provider: openai-codex\n  model_base_url: \"\"\n",
            encoding="utf-8",
        )
        config = tmp_path / "config.yaml"
        self._seed_config(
            config, "slack:\n  allowed_channels:\n    - C_HOME\n  require_mention: true\n"
        )
        r = _run(values, "merge-config-model", "--out", str(config))
        assert r.returncode == 0, r.stderr
        import yaml
        loaded = yaml.safe_load(config.read_text())
        # Other top-level keys preserved
        assert loaded["slack"]["require_mention"] is True
        assert loaded["slack"]["allowed_channels"] == ["C_HOME"]
        # model.provider set, no base_url
        assert loaded["model"]["provider"] == "openai-codex"
        assert "base_url" not in loaded["model"]

    def test_writes_base_url_when_set(self, tmp_path: Path):
        values = tmp_path / "values.yaml"
        values.write_text(
            "gateway:\n"
            "  model_provider: openai\n"
            "  model_base_url: https://proxy.example.com/v1\n",
            encoding="utf-8",
        )
        config = tmp_path / "config.yaml"
        self._seed_config(config, "{}\n")
        r = _run(values, "merge-config-model", "--out", str(config))
        assert r.returncode == 0, r.stderr
        import yaml
        loaded = yaml.safe_load(config.read_text())
        assert loaded["model"]["base_url"] == "https://proxy.example.com/v1"

    def test_drops_stale_base_url_when_values_empty(self, tmp_path: Path):
        # Operator removed the override from values.yaml — config.yaml's
        # stale base_url must drop, otherwise the gateway keeps routing
        # through the old endpoint.
        values = tmp_path / "values.yaml"
        values.write_text(
            "gateway:\n  model_provider: openai\n  model_base_url: \"\"\n",
            encoding="utf-8",
        )
        config = tmp_path / "config.yaml"
        self._seed_config(
            config,
            "model:\n"
            "  default: claude-sonnet-4-5\n"
            "  provider: stale\n"
            "  base_url: https://stale.example.com/v1\n",
        )
        r = _run(values, "merge-config-model", "--out", str(config))
        assert r.returncode == 0, r.stderr
        import yaml
        loaded = yaml.safe_load(config.read_text())
        # default preserved, provider replaced, base_url dropped
        assert loaded["model"]["default"] == "claude-sonnet-4-5"
        assert loaded["model"]["provider"] == "openai"
        assert "base_url" not in loaded["model"]

    def test_requires_gateway_model_provider(self, tmp_path: Path):
        values = tmp_path / "values.yaml"
        values.write_text("gateway:\n  model_provider: \"\"\n", encoding="utf-8")
        config = tmp_path / "config.yaml"
        self._seed_config(config, "{}\n")
        r = _run(values, "merge-config-model", "--out", str(config))
        assert r.returncode == 1
        assert "gateway.model_provider is required" in r.stderr

    def test_missing_config_is_error(self, tmp_path: Path):
        # render-runtime-config seeds the file first; merging into a
        # non-existent path would silently lose the slack block.
        values = tmp_path / "values.yaml"
        values.write_text("gateway:\n  model_provider: openai\n", encoding="utf-8")
        config = tmp_path / "config.yaml"  # never created
        r = _run(values, "merge-config-model", "--out", str(config))
        assert r.returncode == 1
        assert "does not exist" in r.stderr
        assert not config.exists()

    def test_header_re_emitted_on_merge(self, tmp_path: Path):
        # The round-trip strips comments; merge-config-model must re-emit
        # the standard header so the file's intro text doesn't disappear
        # after the first re-install (operators rely on the header to
        # know what's preserved across runs and what isn't).
        values = tmp_path / "values.yaml"
        values.write_text("gateway:\n  model_provider: openai\n", encoding="utf-8")
        config = tmp_path / "config.yaml"
        # Simulate a config.yaml that has lost its header (e.g. after a
        # prior merge or hand-edit that stripped the seeded comment).
        config.write_text("slack:\n  require_mention: true\n", encoding="utf-8")
        r = _run(values, "merge-config-model", "--out", str(config))
        assert r.returncode == 0, r.stderr
        text = config.read_text()
        assert text.startswith("# Seeded by setup-hermes.sh from deploy.values.yaml.")
        # And the honest wording stuck.
        assert "Operator-added top-level keys are preserved" in text
        assert "YAML comments are not" in text

    def test_invalid_yaml_in_config_is_error(self, tmp_path: Path):
        values = tmp_path / "values.yaml"
        values.write_text("gateway:\n  model_provider: openai\n", encoding="utf-8")
        config = tmp_path / "config.yaml"
        config.write_text("not: valid: yaml: ::\n", encoding="utf-8")
        r = _run(values, "merge-config-model", "--out", str(config))
        assert r.returncode == 1
        assert "not valid YAML" in r.stderr


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

    def test_header_advertises_every_run_reconciliation(
        self, quay_values: Path, tmp_path: Path
    ):
        # The header is the only operator-visible cue that this file is
        # configs-as-code. Pin it so a future edit that softens the wording
        # (e.g., back to "preserve this file") trips the test.
        out = tmp_path / "config.toml"
        r = _run(quay_values, "render-quay-config", "--out", str(out))
        assert r.returncode == 0, r.stderr
        text = out.read_text()
        assert "on every run" in text
        assert "reconciled away" in text

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
        # Mixed: one quay-managed entry, one code-only entry. The code-only
        # entry must emit empty package_manager + install_cmd fields so
        # setup-hermes.sh's loop can iterate a single 5-column TSV format
        # for both.
        p = tmp_path / "values.yaml"
        p.write_text(
            "repos:\n"
            "  - id: alpha\n"
            "    url: https://github.com/example/alpha\n"
            "    base_branch: main\n"
            "    quay:\n"
            "      package_manager: bun\n"
            "      install_cmd: bun install\n"
            "  - id: beta\n"
            "    url: https://github.com/example/beta\n"
            "    base_branch: trunk\n"
            "    quay:\n"
            "      package_manager: npm\n"
            "      install_cmd: npm ci --no-audit\n"
            "  - id: gamma\n"
            "    url: https://github.com/example/gamma\n"
            "    base_branch: main\n",
            encoding="utf-8",
        )
        return p

    def test_emits_one_tsv_line_per_repo(self, repos_values: Path):
        r = _run(repos_values, "list-repos")
        assert r.returncode == 0, r.stderr
        lines = r.stdout.splitlines()
        # gamma is code-only — last two columns empty.
        assert lines == [
            "alpha\thttps://github.com/example/alpha\tmain\tbun\tbun install",
            "beta\thttps://github.com/example/beta\ttrunk\tnpm\tnpm ci --no-audit",
            "gamma\thttps://github.com/example/gamma\tmain\t\t",
        ]

    def test_quay_filter_excludes_code_only_entries(self, repos_values: Path):
        r = _run(repos_values, "list-repos", "--quay")
        assert r.returncode == 0, r.stderr
        lines = r.stdout.splitlines()
        # gamma (code-only) is filtered out under --quay.
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

    def test_legacy_quay_repos_rejected_with_migration_hint(self, tmp_path: Path):
        # Legacy `quay.repos[]` schema. The helper must surface a single,
        # clearly-actionable migration error instead of silently honouring
        # the old key and skipping the new top-level repos[] — which would
        # let a stale values file install only the quay-managed entries
        # while losing every code mirror.
        values = tmp_path / "values.yaml"
        values.write_text(
            "quay:\n"
            "  version: v0.1.0\n"
            "  repos:\n"
            "    - id: alpha\n"
            "      url: https://github.com/example/alpha\n"
            "      base_branch: main\n"
            "      package_manager: bun\n"
            "      install_cmd: bun install\n",
            encoding="utf-8",
        )
        r = _run(values, "list-repos")
        assert r.returncode == 1
        assert "quay.repos[]` is no longer supported" in r.stderr
        assert "top-level `repos[]`" in r.stderr

    def test_missing_required_field_exits_nonzero(self, tmp_path: Path):
        # Missing install_cmd inside the quay: block — should fail loudly
        # so an under-specified deploy.values.yaml doesn't silently skip
        # the entry at install time.
        values = tmp_path / "values.yaml"
        values.write_text(
            "repos:\n"
            "  - id: alpha\n"
            "    url: https://github.com/example/alpha\n"
            "    base_branch: main\n"
            "    quay:\n"
            "      package_manager: bun\n",
            encoding="utf-8",
        )
        r = _run(values, "list-repos")
        assert r.returncode == 1
        assert "install_cmd is required" in r.stderr

    def test_empty_required_field_exits_nonzero(self, tmp_path: Path):
        values = tmp_path / "values.yaml"
        values.write_text(
            "repos:\n"
            '  - id: ""\n'
            "    url: https://github.com/example/alpha\n"
            "    base_branch: main\n",
            encoding="utf-8",
        )
        r = _run(values, "list-repos")
        assert r.returncode == 1
        assert "id is required" in r.stderr

    def test_quay_block_must_be_mapping_when_present(self, tmp_path: Path):
        values = tmp_path / "values.yaml"
        values.write_text(
            "repos:\n"
            "  - id: alpha\n"
            "    url: https://github.com/example/alpha\n"
            "    base_branch: main\n"
            "    quay: oops\n",
            encoding="utf-8",
        )
        r = _run(values, "list-repos")
        assert r.returncode == 1
        assert "quay must be a mapping" in r.stderr

    def test_tab_in_field_exits_nonzero(self, tmp_path: Path):
        # A tab inside a field would silently corrupt the bash-side parse.
        values = tmp_path / "values.yaml"
        values.write_text(
            "repos:\n"
            "  - id: alpha\n"
            "    url: https://github.com/example/alpha\n"
            "    base_branch: main\n"
            "    quay:\n"
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
            "repos: not-a-list\n", encoding="utf-8"
        )
        r = _run(values, "list-repos")
        assert r.returncode == 1
        assert "repos must be a list" in r.stderr

    @pytest.mark.parametrize(
        "bad_id",
        [
            "../foo",       # path traversal up
            "foo/bar",      # path separator
            "..",           # parent-dir literal
            ".hidden",      # leading dot
            "-leading",     # leading dash → would be parsed as a CLI flag
            "with space",   # whitespace
            "name@host",    # special chars
        ],
    )
    def test_id_shape_rejects_unsafe_values(self, tmp_path: Path, bad_id: str):
        # id is interpolated into a filesystem path on the bash side; the
        # helper is the validation choke point.
        values = tmp_path / "values.yaml"
        values.write_text(
            "repos:\n"
            f"  - id: {bad_id!r}\n"
            "    url: https://github.com/example/x\n"
            "    base_branch: main\n",
            encoding="utf-8",
        )
        r = _run(values, "list-repos")
        assert r.returncode == 1, r.stdout
        assert ".id=" in r.stderr
        assert "must match" in r.stderr

    @pytest.mark.parametrize(
        "good_id",
        ["alpha", "test-factory-code", "v1.2", "underscored_id", "x", "a1.b2-c3"],
    )
    def test_id_shape_accepts_typical_values(self, tmp_path: Path, good_id: str):
        values = tmp_path / "values.yaml"
        values.write_text(
            "repos:\n"
            f"  - id: {good_id}\n"
            "    url: https://github.com/example/x\n"
            "    base_branch: main\n",
            encoding="utf-8",
        )
        r = _run(values, "list-repos")
        assert r.returncode == 0, r.stderr
        assert r.stdout.split("\t")[0] == good_id

    @pytest.mark.parametrize(
        "bad_url",
        [
            "https://github.com/example/alpha.git",       # .git suffix
            "https://github.com/example/alpha/",          # trailing slash + extra segs
            "https://github.com/example",                 # missing repo segment
            "http://github.com/example/alpha",            # http (not https)
            "git@github.com:example/alpha.git",           # SCP-style SSH (empty netloc trap)
            "git@github.com:example/alpha",               # SCP-style SSH, no .git
            "ssh://git@github.com/example/alpha.git",     # explicit ssh:// scheme
        ],
    )
    def test_github_url_shape_rejected(self, tmp_path: Path, bad_url: str):
        # The per-repo url.insteadOf rewrite expects a clean
        # `https://github.com/<org>/<repo>` prefix; anything else either
        # fails to match or produces a broken rewrite. The helper is the
        # validation choke point.
        values = tmp_path / "values.yaml"
        values.write_text(
            "repos:\n"
            "  - id: alpha\n"
            f"    url: {bad_url}\n"
            "    base_branch: main\n",
            encoding="utf-8",
        )
        r = _run(values, "list-repos")
        assert r.returncode == 1, r.stdout

    @pytest.mark.parametrize(
        "passthrough_url",
        [
            "file:///srv/hermes/repos/quay-fixtures/alpha",
            "https://gitlab.com/example/alpha",
            "https://gitlab.com/example/alpha.git",
        ],
    )
    def test_non_github_urls_pass_through_unchecked(
        self, tmp_path: Path, passthrough_url: str
    ):
        # CI fixtures use file:// URLs; public mirrors elsewhere use other
        # hosts. The installer's url.insteadOf rewrite is github.com-only,
        # so the helper has nothing to validate against on these URLs.
        values = tmp_path / "values.yaml"
        values.write_text(
            "repos:\n"
            "  - id: alpha\n"
            f"    url: {passthrough_url}\n"
            "    base_branch: main\n",
            encoding="utf-8",
        )
        r = _run(values, "list-repos")
        assert r.returncode == 0, r.stderr
        assert r.stdout.split("\t")[1] == passthrough_url


class TestValidateSchema:
    """Direct coverage of `validate-schema` — setup-hermes.sh --verify
    invokes it and prints the helper's stderr verbatim, so the error
    messages are part of the public contract."""

    def test_clean_schema_silent(self, tmp_path: Path):
        values = tmp_path / "values.yaml"
        values.write_text(
            "repos:\n"
            "  - id: alpha\n"
            "    url: https://github.com/example/alpha\n"
            "    base_branch: main\n",
            encoding="utf-8",
        )
        r = _run(values, "validate-schema")
        assert r.returncode == 0
        assert r.stdout == ""
        assert r.stderr == ""

    def test_legacy_key_rejected(self, tmp_path: Path):
        values = tmp_path / "values.yaml"
        values.write_text(
            "quay:\n  repos: []\n",
            encoding="utf-8",
        )
        r = _run(values, "validate-schema")
        assert r.returncode == 1
        assert "quay.repos[]` is no longer supported" in r.stderr

    def test_missing_required_field_named(self, tmp_path: Path):
        values = tmp_path / "values.yaml"
        values.write_text(
            "repos:\n"
            "  - id: alpha\n"
            "    url: https://github.com/example/alpha\n",
            encoding="utf-8",
        )
        r = _run(values, "validate-schema")
        assert r.returncode == 1
        assert "base_branch is required" in r.stderr


class TestRenderGatewayOrgDefaults:
    """Direct coverage of `render-gateway-org-defaults`. The seed shape is
    part of the public contract — the gateway prompt builder loads this
    file verbatim into the cached system prompt."""

    def test_empty_repos_writes_empty_file(self, tmp_path: Path):
        values = tmp_path / "values.yaml"
        values.write_text("org:\n  name: X\n", encoding="utf-8")
        out = tmp_path / "gateway-org-defaults.md"
        r = _run(values, "render-gateway-org-defaults", "--out", str(out))
        assert r.returncode == 0, r.stderr
        assert out.exists()
        assert out.read_text() == ""

    def test_repos_without_issue_tracker_omit_linear_sentence(
        self, tmp_path: Path
    ):
        values = tmp_path / "values.yaml"
        values.write_text(
            "repos:\n"
            "  - id: alpha\n"
            "    url: https://github.com/example/alpha\n"
            "    base_branch: main\n",
            encoding="utf-8",
        )
        out = tmp_path / "gateway-org-defaults.md"
        r = _run(values, "render-gateway-org-defaults", "--out", str(out))
        assert r.returncode == 0, r.stderr
        text = out.read_text()
        assert "~/.hermes/code/<repo>/" in text
        assert "never `git clone`" in text
        assert "alpha(main)" in text
        assert "Linear" not in text
        assert "inverter-linear" not in text

    def test_per_repo_issue_tracker_renders_linear_suffix(self, tmp_path: Path):
        values = tmp_path / "values.yaml"
        values.write_text(
            "linear:\n"
            "  teams:\n"
            "    itry: 28294e03-c1ce-4c2b-ba24-49e36179a321\n"
            "    aster: 11111111-2222-3333-4444-555555555555\n"
            "repos:\n"
            "  - id: alpha\n"
            "    url: https://github.com/example/alpha\n"
            "    base_branch: main\n"
            "    issue_tracker:\n"
            "      linear:\n"
            "        team: itry\n"
            "  - id: beta\n"
            "    url: https://github.com/example/beta\n"
            "    base_branch: main\n"
            "    issue_tracker:\n"
            "      linear:\n"
            "        team: aster\n"
            "  - id: gamma\n"
            "    url: https://github.com/example/gamma\n"
            "    base_branch: main\n",
            encoding="utf-8",
        )
        out = tmp_path / "gateway-org-defaults.md"
        r = _run(values, "render-gateway-org-defaults", "--out", str(out))
        assert r.returncode == 0, r.stderr
        text = out.read_text()
        # Linear sentence present because at least one repo opted in
        assert "Issue tracking is Linear via the `inverter-linear` skill" in text
        assert "never `gh issue`" in text
        # Per-repo Linear suffix only on opted-in entries
        assert "alpha(main, Linear:itry)" in text
        assert "beta(main, Linear:aster)" in text
        assert "gamma(main)" in text  # no suffix when issue_tracker absent
        # No trailing newline garbage; one paragraph, one trailing \n.
        assert text.endswith(".\n")
        assert text.count("\n") == 1

    def test_token_efficient_single_paragraph(self, tmp_path: Path):
        # Guardrail: the seed must stay one paragraph (no headings, no
        # bullets, no multi-line structure) — every gateway turn pays this
        # token cost and the shape is part of the contract.
        values = tmp_path / "values.yaml"
        values.write_text(
            "linear:\n"
            "  teams:\n"
            "    itry: 28294e03-c1ce-4c2b-ba24-49e36179a321\n"
            "repos:\n"
            "  - id: alpha\n"
            "    url: https://github.com/example/alpha\n"
            "    base_branch: main\n"
            "    issue_tracker:\n"
            "      linear:\n"
            "        team: itry\n",
            encoding="utf-8",
        )
        out = tmp_path / "gateway-org-defaults.md"
        r = _run(values, "render-gateway-org-defaults", "--out", str(out))
        assert r.returncode == 0, r.stderr
        text = out.read_text()
        # Exactly one trailing newline; no internal line breaks.
        assert text.count("\n") == 1
        # No markdown headings or bullets that would inflate the seed.
        assert not any(line.startswith(("#", "-", "*")) for line in text.splitlines())
        # Sanity: well under a kilobyte for a one-repo deployment.
        assert len(text.encode("utf-8")) < 512

    def test_rejects_unknown_team_key(self, tmp_path: Path):
        values = tmp_path / "values.yaml"
        values.write_text(
            "linear:\n"
            "  teams:\n"
            "    itry: 28294e03-c1ce-4c2b-ba24-49e36179a321\n"
            "repos:\n"
            "  - id: alpha\n"
            "    url: https://github.com/example/alpha\n"
            "    base_branch: main\n"
            "    issue_tracker:\n"
            "      linear:\n"
            "        team: aster\n",
            encoding="utf-8",
        )
        out = tmp_path / "gateway-org-defaults.md"
        r = _run(values, "render-gateway-org-defaults", "--out", str(out))
        assert r.returncode == 1
        assert "does not match any key in linear.teams" in r.stderr

    def test_rejects_unknown_adapter(self, tmp_path: Path):
        values = tmp_path / "values.yaml"
        values.write_text(
            "linear:\n"
            "  teams:\n"
            "    itry: 28294e03-c1ce-4c2b-ba24-49e36179a321\n"
            "repos:\n"
            "  - id: alpha\n"
            "    url: https://github.com/example/alpha\n"
            "    base_branch: main\n"
            "    issue_tracker:\n"
            "      jira:\n"
            "        project: ABC\n",
            encoding="utf-8",
        )
        out = tmp_path / "gateway-org-defaults.md"
        r = _run(values, "render-gateway-org-defaults", "--out", str(out))
        assert r.returncode == 1
        assert "unknown adapter" in r.stderr
        assert "jira" in r.stderr

    def test_rejects_missing_linear_team(self, tmp_path: Path):
        values = tmp_path / "values.yaml"
        values.write_text(
            "linear:\n"
            "  teams:\n"
            "    itry: 28294e03-c1ce-4c2b-ba24-49e36179a321\n"
            "repos:\n"
            "  - id: alpha\n"
            "    url: https://github.com/example/alpha\n"
            "    base_branch: main\n"
            "    issue_tracker:\n"
            "      linear: {}\n",
            encoding="utf-8",
        )
        out = tmp_path / "gateway-org-defaults.md"
        r = _run(values, "render-gateway-org-defaults", "--out", str(out))
        assert r.returncode == 1
        assert "linear.team is required" in r.stderr

    def test_render_rejects_invalid_repo_id(self, tmp_path: Path):
        # validate-schema is gated on --verify; the render path must
        # enforce id/url shape itself so a typo can't reach the seed.
        values = tmp_path / "values.yaml"
        values.write_text(
            "repos:\n"
            "  - id: bad/slash\n"
            "    url: https://github.com/example/foo\n"
            "    base_branch: main\n",
            encoding="utf-8",
        )
        out = tmp_path / "gateway-org-defaults.md"
        r = _run(values, "render-gateway-org-defaults", "--out", str(out))
        assert r.returncode == 1
        assert "bad/slash" in r.stderr

    def test_render_rejects_invalid_repo_url(self, tmp_path: Path):
        values = tmp_path / "values.yaml"
        values.write_text(
            "repos:\n"
            "  - id: alpha\n"
            "    url: https://github.com/example/alpha.git\n"
            "    base_branch: main\n",
            encoding="utf-8",
        )
        out = tmp_path / "gateway-org-defaults.md"
        r = _run(values, "render-gateway-org-defaults", "--out", str(out))
        assert r.returncode == 1
        assert ".git" in r.stderr

    def test_validate_schema_also_catches_unknown_team(self, tmp_path: Path):
        values = tmp_path / "values.yaml"
        values.write_text(
            "linear:\n"
            "  teams:\n"
            "    itry: 28294e03-c1ce-4c2b-ba24-49e36179a321\n"
            "repos:\n"
            "  - id: alpha\n"
            "    url: https://github.com/example/alpha\n"
            "    base_branch: main\n"
            "    issue_tracker:\n"
            "      linear:\n"
            "        team: aster\n",
            encoding="utf-8",
        )
        r = _run(values, "validate-schema")
        assert r.returncode == 1
        assert "does not match any key in linear.teams" in r.stderr


class TestParseRepoListIds:
    """Direct coverage of `parse-repo-list-ids` — the subcommand both the
    install-time and verify-time paths in setup-hermes.sh pipe `quay repo
    list` output into. The field-name detail (real binary keys entries by
    `repo_id`, not `id`) used to live duplicated across two bash heredocs;
    centralising it here means a future format drift surfaces as a unit
    test failure instead of a CI integration bug."""

    def _run_parse(self, stdin: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, str(HELPER), "parse-repo-list-ids"],
            input=stdin,
            capture_output=True,
            text=True,
        )

    def test_extracts_repo_ids(self):
        payload = json.dumps([
            {"repo_id": "alpha", "repo_url": "https://example.com/a"},
            {"repo_id": "beta", "repo_url": "https://example.com/b"},
        ])
        r = self._run_parse(payload)
        assert r.returncode == 0, r.stderr
        assert r.stdout.splitlines() == ["alpha", "beta"]

    def test_empty_array_is_clean_exit(self):
        r = self._run_parse("[]")
        assert r.returncode == 0, r.stderr
        assert r.stdout == ""

    def test_skips_entries_without_repo_id(self):
        payload = json.dumps([
            {"repo_id": "ok"},
            {"name": "no-repo-id-key"},
            {"repo_id": ""},
            "not-a-dict",
        ])
        r = self._run_parse(payload)
        assert r.returncode == 0, r.stderr
        assert r.stdout.splitlines() == ["ok"]

    def test_non_json_input_exits_2(self):
        r = self._run_parse("garbage not json")
        assert r.returncode == 2
        assert "not valid JSON" in r.stderr

    def test_non_list_top_level_exits_2(self):
        r = self._run_parse('{"repo_id": "single"}')
        assert r.returncode == 2
        assert "expected JSON array" in r.stderr

    def test_rejects_id_field_not_repo_id(self):
        """The bug PR #23 fixed: a parser that read `id` would silently
        emit nothing on real binary output. Guard against regression by
        asserting an `id`-keyed payload yields no rows."""
        payload = json.dumps([{"id": "alpha", "url": "https://example.com/a"}])
        r = self._run_parse(payload)
        assert r.returncode == 0, r.stderr
        assert r.stdout == ""


class TestParseTaskListCount:
    """Direct coverage of `parse-task-list-count` — gates the stale
    ~/.quay/ refusal in setup-hermes.sh on whether operator state is
    present in the stale DB. Same exit-code contract as
    parse-repo-list-ids: 2 on un-probeable input lets the bash caller
    refuse-on-uncertainty without a sentinel value."""

    def _run_parse(self, stdin: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, str(HELPER), "parse-task-list-count"],
            input=stdin,
            capture_output=True,
            text=True,
        )

    def test_counts_entries(self):
        r = self._run_parse(json.dumps([{"task_id": "a"}, {"task_id": "b"}]))
        assert r.returncode == 0, r.stderr
        assert r.stdout.strip() == "2"

    def test_empty_array_yields_zero(self):
        r = self._run_parse("[]")
        assert r.returncode == 0, r.stderr
        assert r.stdout.strip() == "0"

    def test_non_json_input_exits_2(self):
        r = self._run_parse("garbage not json")
        assert r.returncode == 2
        assert "not valid JSON" in r.stderr

    def test_non_list_top_level_exits_2(self):
        r = self._run_parse('{"task_id": "single"}')
        assert r.returncode == 2
        assert "expected JSON array" in r.stderr


# ---------------------------------------------------------------------------
# Tag vocab — per-repo `tags:` and deployment `tag_namespaces:` blocks
# ---------------------------------------------------------------------------


def _values_with_repo(tmp_path: Path, body: str) -> Path:
    """Minimal values.yaml carrying one quay-managed repo plus ``body`` at
    top level so the new tag-vocab block can be exercised in isolation
    without dragging in the whole gateway/slack schema."""
    p = tmp_path / "values.yaml"
    p.write_text(
        "repos:\n"
        "  - id: alpha\n"
        "    url: https://github.com/example/alpha\n"
        "    base_branch: main\n"
        "    quay:\n"
        "      package_manager: bun\n"
        "      install_cmd: \"bun install\"\n"
        + body,
        encoding="utf-8",
    )
    return p


class TestGetRepoTags:
    """`get-repo-tags <repo_id>` — the install-time emitter piped into
    `quay repo apply-tags --from -`. The JSON shape is the public contract
    with quay's apply payload: missing/empty `tags:` MUST emit
    `{"namespaces": {}}` so the strict-reconciliation path actually
    clears the repo's vocab on removal."""

    def test_bare_list_wrapped_with_required_false(self, tmp_path: Path):
        values = _values_with_repo(
            tmp_path,
            "      tags:\n"
            "        area: [vesting, bonding-curve]\n"
            "        risk: [reentrancy]\n",
        )
        r = _run(values, "get-repo-tags", "alpha")
        assert r.returncode == 0, r.stderr
        out = json.loads(r.stdout)
        assert out == {
            "namespaces": {
                "area": {"required": False, "values": ["bonding-curve", "vesting"]},
                "risk": {"required": False, "values": ["reentrancy"]},
            }
        }

    def test_absent_tags_emits_empty_clear_payload(self, tmp_path: Path):
        # Strict reconciliation: dropping the `tags:` block from values
        # MUST flow through to "clear the repo's vocab in quay" — that's
        # the explicit-clear payload upstream's apply-tags accepts.
        values = _values_with_repo(tmp_path, "")
        r = _run(values, "get-repo-tags", "alpha")
        assert r.returncode == 0, r.stderr
        assert json.loads(r.stdout) == {"namespaces": {}}

    def test_empty_tags_block_emits_empty_clear_payload(self, tmp_path: Path):
        values = _values_with_repo(tmp_path, "      tags: {}\n")
        r = _run(values, "get-repo-tags", "alpha")
        assert r.returncode == 0, r.stderr
        assert json.loads(r.stdout) == {"namespaces": {}}

    def test_unknown_repo_id_exits_1(self, tmp_path: Path):
        values = _values_with_repo(tmp_path, "")
        r = _run(values, "get-repo-tags", "no-such-repo")
        assert r.returncode == 1
        assert "no-such-repo" in r.stderr

    def test_namespace_with_dash_rejected(self, tmp_path: Path):
        # Quay's validator splits ticket tags on the first `-`, so a
        # dashed namespace would be unaddressable. The helper rejects
        # at validate-time so a typo doesn't reach `apply-tags`.
        values = _values_with_repo(
            tmp_path,
            "      tags:\n"
            "        task-type: [bugfix]\n",
        )
        r = _run(values, "get-repo-tags", "alpha")
        assert r.returncode == 1
        assert "namespace must match" in r.stderr

    def test_value_with_uppercase_rejected(self, tmp_path: Path):
        values = _values_with_repo(
            tmp_path,
            "      tags:\n"
            "        area: [Bonding-Curve]\n",
        )
        r = _run(values, "get-repo-tags", "alpha")
        assert r.returncode == 1
        assert "must match" in r.stderr

    def test_duplicate_value_rejected(self, tmp_path: Path):
        values = _values_with_repo(
            tmp_path,
            "      tags:\n"
            "        area: [bonding-curve, bonding-curve]\n",
        )
        r = _run(values, "get-repo-tags", "alpha")
        assert r.returncode == 1
        assert "duplicate" in r.stderr

    def test_non_list_value_rejected(self, tmp_path: Path):
        values = _values_with_repo(
            tmp_path,
            "      tags:\n"
            "        area: not-a-list\n",
        )
        r = _run(values, "get-repo-tags", "alpha")
        assert r.returncode == 1
        assert "must be a list" in r.stderr

    def test_output_is_deterministic_across_runs(self, tmp_path: Path):
        # verify-path drift is a string equality check vs `quay repo
        # get-tags` (which itself sorts). Re-running the helper must
        # produce byte-identical output, otherwise even a no-op install
        # would surface as drift.
        values = _values_with_repo(
            tmp_path,
            "      tags:\n"
            "        risk: [reentrancy, access-control]\n"
            "        area: [vesting, bonding-curve]\n",
        )
        first = _run(values, "get-repo-tags", "alpha").stdout
        second = _run(values, "get-repo-tags", "alpha").stdout
        assert first == second
        # Sorted form: namespaces alpha-sorted, values within alpha-sorted.
        assert first.index('"area"') < first.index('"risk"')
        assert first.index('"access-control"') < first.index('"reentrancy"')


class TestGetDeploymentTags:
    """`get-deployment-tags` — emitter for the deployment-level apply.
    Same contract as get-repo-tags except it consumes the full envelope
    (`{required, values}`) so deployment-required namespaces flow
    through values.yaml into quay."""

    def _values(self, tmp_path: Path, body: str) -> Path:
        p = tmp_path / "values.yaml"
        p.write_text(body, encoding="utf-8")
        return p

    def test_envelope_passes_required_through(self, tmp_path: Path):
        values = self._values(
            tmp_path,
            "quay:\n"
            "  tag_namespaces:\n"
            "    tasktype:\n"
            "      required: true\n"
            "      values: [bugfix, refactor]\n"
            "    risk:\n"
            "      values: [pii, money-handling]\n",
        )
        r = _run(values, "get-deployment-tags")
        assert r.returncode == 0, r.stderr
        out = json.loads(r.stdout)
        assert out == {
            "namespaces": {
                "risk": {"required": False, "values": ["money-handling", "pii"]},
                "tasktype": {"required": True, "values": ["bugfix", "refactor"]},
            }
        }

    def test_required_omitted_defaults_to_false(self, tmp_path: Path):
        values = self._values(
            tmp_path,
            "quay:\n"
            "  tag_namespaces:\n"
            "    risk:\n"
            "      values: [pii]\n",
        )
        r = _run(values, "get-deployment-tags")
        assert r.returncode == 0, r.stderr
        out = json.loads(r.stdout)
        assert out["namespaces"]["risk"]["required"] is False

    def test_required_true_with_empty_values_rejected(self, tmp_path: Path):
        # Mirrors the upstream rule (apply-deployment refuses
        # `{values: [], required: true}` because every validation would
        # emit TAG_REQUIRED_MISSING with no satisfying tag possible).
        # Surfacing this at values-helper time gives a single error line
        # instead of a partial install.
        values = self._values(
            tmp_path,
            "quay:\n"
            "  tag_namespaces:\n"
            "    risk:\n"
            "      required: true\n"
            "      values: []\n",
        )
        r = _run(values, "get-deployment-tags")
        assert r.returncode == 1
        assert "required=true with no values" in r.stderr

    def test_unknown_key_rejected(self, tmp_path: Path):
        values = self._values(
            tmp_path,
            "quay:\n"
            "  tag_namespaces:\n"
            "    risk:\n"
            "      values: [pii]\n"
            "      typo_field: true\n",
        )
        r = _run(values, "get-deployment-tags")
        assert r.returncode == 1
        assert "unknown key" in r.stderr

    def test_required_non_bool_rejected(self, tmp_path: Path):
        values = self._values(
            tmp_path,
            "quay:\n"
            "  tag_namespaces:\n"
            "    risk:\n"
            "      required: \"yes\"\n"
            "      values: [pii]\n",
        )
        r = _run(values, "get-deployment-tags")
        assert r.returncode == 1
        assert "must be a bool" in r.stderr

    def test_absent_block_emits_empty_clear_payload(self, tmp_path: Path):
        values = self._values(tmp_path, "quay:\n  version: \"v0.2.0\"\n")
        r = _run(values, "get-deployment-tags")
        assert r.returncode == 0, r.stderr
        assert json.loads(r.stdout) == {"namespaces": {}}

    def test_quay_block_absent_is_empty_clear(self, tmp_path: Path):
        # Pre-quay-enabled forks have no `quay:` block at all; the
        # helper must still emit the explicit-clear payload so the
        # installer can pipe it unconditionally without an extra guard.
        values = self._values(tmp_path, "org:\n  name: T\n")
        r = _run(values, "get-deployment-tags")
        assert r.returncode == 0, r.stderr
        assert json.loads(r.stdout) == {"namespaces": {}}


class TestValidateSchemaTagVocab:
    """`validate-schema` walks the new tag blocks too — drift in the
    values file should surface at verify time, not on the next install
    when half the reconciliation has already run."""

    def test_per_repo_tag_error_surfaces(self, tmp_path: Path):
        values = _values_with_repo(
            tmp_path,
            "      tags:\n"
            "        area: [INVALID-CASE]\n",
        )
        r = _run(values, "validate-schema")
        assert r.returncode == 1
        assert "must match" in r.stderr

    def test_deployment_tag_error_surfaces(self, tmp_path: Path):
        values = tmp_path / "values.yaml"
        values.write_text(
            "quay:\n"
            "  tag_namespaces:\n"
            "    risk:\n"
            "      required: true\n"
            "      values: []\n",
            encoding="utf-8",
        )
        r = _run(values, "validate-schema")
        assert r.returncode == 1
        assert "required=true" in r.stderr

    def test_clean_tag_blocks_silent(self, tmp_path: Path):
        values = _values_with_repo(
            tmp_path,
            "      tags:\n"
            "        area: [bonding-curve]\n"
            "quay:\n"
            "  tag_namespaces:\n"
            "    tasktype:\n"
            "      required: true\n"
            "      values: [bugfix]\n",
        )
        r = _run(values, "validate-schema")
        assert r.returncode == 0
        assert r.stderr == ""


