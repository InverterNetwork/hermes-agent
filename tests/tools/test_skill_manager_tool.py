"""Tests for tools/skill_manager_tool.py — skill creation, editing, and deletion."""

import json
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import pytest

from tools.skill_manager_tool import (
    _validate_name,
    _validate_category,
    _validate_frontmatter,
    _validate_file_path,
    _create_skill,
    _edit_skill,
    _patch_skill,
    _delete_skill,
    _write_file,
    _remove_file,
    skill_manage,
)
from agent.skill_utils import (
    extract_skill_description,
    parse_frontmatter,
    SKILL_PROMPT_DESC_LIMIT,
)


@contextmanager
def _skill_dir(tmp_path):
    """Patch both SKILLS_DIR and get_all_skills_dirs so _find_skill searches
    only the temp directory — not the real ~/.hermes/skills/."""
    with patch("tools.skill_manager_tool.SKILLS_DIR", tmp_path), \
         patch("agent.skill_utils.get_all_skills_dirs", return_value=[tmp_path]):
        yield


VALID_SKILL_CONTENT = """\
---
name: test-skill
description: A test skill for unit testing.
---

# Test Skill

Step 1: Do the thing.
"""

VALID_SKILL_CONTENT_2 = """\
---
name: test-skill
description: Updated description.
---

# Test Skill v2

Step 1: Do the new thing.
"""

LONG_DESC_CONTENT = """\
---
name: long-desc
description: Use when deploying multi-region Kubernetes clusters with custom CNI plugins and service mesh.
---

# Long Desc Skill

Step 1.
"""


# ---------------------------------------------------------------------------
# _validate_name
# ---------------------------------------------------------------------------


class TestValidateName:
    def test_valid_names(self):
        assert _validate_name("my-skill") is None
        assert _validate_name("skill123") is None
        assert _validate_name("my_skill.v2") is None
        assert _validate_name("a") is None

    def test_special_chars_rejected(self):
        err = _validate_name("skill/name")
        assert "Invalid skill name 'skill/name'" in err
        err = _validate_name("skill name")
        assert "Invalid skill name 'skill name'" in err
        err = _validate_name("skill@name")
        assert "Invalid skill name 'skill@name'" in err


class TestValidateCategory:
    def test_path_traversal_rejected(self):
        err = _validate_category("../escape")
        assert "Invalid category '../escape'" in err

    def test_absolute_path_rejected(self):
        err = _validate_category("/tmp/escape")
        assert "Invalid category '/tmp/escape'" in err


# ---------------------------------------------------------------------------
# _validate_frontmatter
# ---------------------------------------------------------------------------


class TestValidateFrontmatter:
    def test_no_frontmatter(self):
        err = _validate_frontmatter("# Just a heading\nSome content.\n")
        assert err == "SKILL.md must start with YAML frontmatter (---). See existing skills for format."

    def test_invalid_yaml(self):
        content = "---\n: invalid: yaml: {{{\n---\n\nBody.\n"
        assert "YAML frontmatter parse error" in _validate_frontmatter(content)


# ---------------------------------------------------------------------------
# _validate_file_path — path traversal prevention
# ---------------------------------------------------------------------------


class TestValidateFilePath:
    def test_valid_paths(self):
        assert _validate_file_path("references/api.md") is None
        assert _validate_file_path("templates/config.yaml") is None
        assert _validate_file_path("scripts/train.py") is None
        assert _validate_file_path("assets/image.png") is None

    def test_path_traversal_blocked(self):
        err = _validate_file_path("references/../../../etc/passwd")
        assert err == "Path traversal ('..') is not allowed."


    def test_skill_md_traversal_still_rejected(self):
        # The SKILL.md exception must not weaken the traversal guard.
        err = _validate_file_path("../SKILL.md")
        assert err == "Path traversal ('..') is not allowed."

    def test_other_root_md_still_rejected(self):
        # Only SKILL.md gets the root-level exception, not arbitrary files.
        err = _validate_file_path("README.md")
        assert "File must be under one of:" in err


# ---------------------------------------------------------------------------
# CRUD operations
# ---------------------------------------------------------------------------


class TestCreateSkill:
    def test_create_skill(self, tmp_path):
        with _skill_dir(tmp_path):
            result = _create_skill("my-skill", VALID_SKILL_CONTENT)
        assert result["success"] is True
        assert (tmp_path / "my-skill" / "SKILL.md").exists()

    def test_create_duplicate_blocked(self, tmp_path):
        with _skill_dir(tmp_path):
            _create_skill("my-skill", VALID_SKILL_CONTENT)
            result = _create_skill("my-skill", VALID_SKILL_CONTENT)
        assert result["success"] is False
        assert "already exists" in result["error"]

    def test_create_rejects_category_traversal(self, tmp_path):
        skills_dir = tmp_path / "skills"
        skills_dir.mkdir()

        with patch("tools.skill_manager_tool.SKILLS_DIR", skills_dir), \
             patch("agent.skill_utils.get_all_skills_dirs", return_value=[skills_dir]):
            result = _create_skill("my-skill", VALID_SKILL_CONTENT, category="../escape")

        assert result["success"] is False
        assert "Invalid category '../escape'" in result["error"]
        assert not (tmp_path / "escape").exists()


    def test_edit_long_desc_still_allowed_with_preview(self, tmp_path):
        """Edit/patch paths stay permissive so existing over-limit skills
        remain maintainable — they warn via system_prompt_preview instead."""
        with _skill_dir(tmp_path):
            _create_skill("my-skill", VALID_SKILL_CONTENT)
            result = _edit_skill("my-skill", LONG_DESC_CONTENT)
        assert result["success"] is True
        assert "system_prompt_preview" in result
        assert "System prompt will show" in result["system_prompt_preview"]
        fm, _ = parse_frontmatter(LONG_DESC_CONTENT)
        assert extract_skill_description(fm) in result["system_prompt_preview"]


class TestEditSkill:
    def test_edit_existing_skill(self, tmp_path):
        with _skill_dir(tmp_path):
            _create_skill("my-skill", VALID_SKILL_CONTENT)
            result = _edit_skill("my-skill", VALID_SKILL_CONTENT_2)
        assert result["success"] is True
        content = (tmp_path / "my-skill" / "SKILL.md").read_text()
        assert "Updated description" in content


    def test_edit_invalid_content_rejected(self, tmp_path):
        with _skill_dir(tmp_path):
            _create_skill("my-skill", VALID_SKILL_CONTENT)
            result = _edit_skill("my-skill", "no frontmatter")
        assert result["success"] is False
        # Original content should be preserved
        content = (tmp_path / "my-skill" / "SKILL.md").read_text()
        assert "A test skill" in content

class TestPatchSkill:
    def test_patch_unique_match(self, tmp_path):
        with _skill_dir(tmp_path):
            _create_skill("my-skill", VALID_SKILL_CONTENT)
            result = _patch_skill("my-skill", "Do the thing.", "Do the new thing.")
        assert result["success"] is True
        content = (tmp_path / "my-skill" / "SKILL.md").read_text()
        assert "Do the new thing." in content


    def test_patch_ambiguous_match_rejected(self, tmp_path):
        content = """\
---
name: test-skill
description: A test skill.
---

# Test

word word
"""
        with _skill_dir(tmp_path):
            _create_skill("my-skill", content)
            result = _patch_skill("my-skill", "word", "replaced")
        assert result["success"] is False
        assert "match" in result["error"].lower()

    def test_patch_supporting_file_symlink_escape_blocked(self, tmp_path):
        outside_file = tmp_path / "outside.txt"
        outside_file.write_text("old text here")

        with _skill_dir(tmp_path):
            _create_skill("my-skill", VALID_SKILL_CONTENT)
            link = tmp_path / "my-skill" / "references" / "evil.md"
            link.parent.mkdir(parents=True, exist_ok=True)
            try:
                link.symlink_to(outside_file)
            except OSError:
                pytest.skip("Symlinks not supported")

            result = _patch_skill("my-skill", "old text", "new text", file_path="references/evil.md")

        assert result["success"] is False
        assert "escapes" in result["error"].lower()
        assert outside_file.read_text() == "old text here"


class TestDeleteSkill:
    def test_delete_cleans_empty_category_dir(self, tmp_path):
        with _skill_dir(tmp_path):
            _create_skill("my-skill", VALID_SKILL_CONTENT, category="devops")
            _delete_skill("my-skill")
        assert not (tmp_path / "devops").exists()


    def test_delete_with_absorbed_into_equals_self_rejected(self, tmp_path):
        with _skill_dir(tmp_path):
            _create_skill("narrow", VALID_SKILL_CONTENT)
            result = _delete_skill("narrow", absorbed_into="narrow")
        assert result["success"] is False
        assert "cannot equal" in result["error"]
        assert (tmp_path / "narrow").exists()

# ---------------------------------------------------------------------------
# write_file / remove_file
# ---------------------------------------------------------------------------


class TestWriteFile:
    def test_write_reference_file(self, tmp_path):
        with _skill_dir(tmp_path):
            _create_skill("my-skill", VALID_SKILL_CONTENT)
            result = _write_file("my-skill", "references/api.md", "# API\nEndpoint docs.")
        assert result["success"] is True
        assert (tmp_path / "my-skill" / "references" / "api.md").exists()

    def test_write_symlink_escape_blocked(self, tmp_path):
        outside_dir = tmp_path / "outside"
        outside_dir.mkdir()

        with _skill_dir(tmp_path):
            _create_skill("my-skill", VALID_SKILL_CONTENT)
            link = tmp_path / "my-skill" / "references" / "escape"
            link.parent.mkdir(parents=True, exist_ok=True)
            try:
                link.symlink_to(outside_dir, target_is_directory=True)
            except OSError:
                pytest.skip("Symlinks not supported")

            result = _write_file("my-skill", "references/escape/owned.md", "malicious")

        assert result["success"] is False
        assert "escapes" in result["error"].lower()
        assert not (outside_dir / "owned.md").exists()


class TestRemoveFile:
    def test_remove_existing_file(self, tmp_path):
        with _skill_dir(tmp_path):
            _create_skill("my-skill", VALID_SKILL_CONTENT)
            _write_file("my-skill", "references/api.md", "content")
            result = _remove_file("my-skill", "references/api.md")
        assert result["success"] is True
        assert not (tmp_path / "my-skill" / "references" / "api.md").exists()

    def test_remove_symlink_escape_blocked(self, tmp_path):
        outside_dir = tmp_path / "outside"
        outside_dir.mkdir()
        outside_file = outside_dir / "keep.txt"
        outside_file.write_text("content")

        with _skill_dir(tmp_path):
            _create_skill("my-skill", VALID_SKILL_CONTENT)
            link = tmp_path / "my-skill" / "references" / "escape"
            link.parent.mkdir(parents=True, exist_ok=True)
            try:
                link.symlink_to(outside_dir, target_is_directory=True)
            except OSError:
                pytest.skip("Symlinks not supported")

            result = _remove_file("my-skill", "references/escape/keep.txt")

        assert result["success"] is False
        assert "escapes" in result["error"].lower()
        assert outside_file.exists()


# ---------------------------------------------------------------------------
# skill_manage dispatcher
# ---------------------------------------------------------------------------


class TestSkillManageDispatcher:
    def test_full_create_via_dispatcher(self, tmp_path):
        """Foreground create does NOT mark the skill as agent-created.

        Skills created by user-directed foreground turns belong to the user;
        only the background self-improvement review fork should mark its
        own sediment as agent-created (so the curator can later consolidate
        or prune it).
        """
        with _skill_dir(tmp_path):
            raw = skill_manage(action="create", name="test-skill", content=VALID_SKILL_CONTENT)
            from tools.skill_usage import load_usage
            usage = load_usage()
        result = json.loads(raw)
        assert result["success"] is True
        # No provenance marker on a foreground create — record either missing
        # entirely (telemetry best-effort) or present with created_by unset.
        rec = usage.get("test-skill") or {}
        assert rec.get("created_by") in {None, "", False}

    def test_successful_mutations_emit_lifecycle_with_correlation(self, tmp_path):
        with (
            _skill_dir(tmp_path),
            patch("tools.skill_provenance.is_background_review", return_value=False),
            patch("tools.skill_usage.record_created") as record_created,
            patch("tools.skill_usage.bump_patch") as bump_patch,
        ):
            created = json.loads(skill_manage(
                action="create",
                name="test-skill",
                content=VALID_SKILL_CONTENT,
                task_id="task-mutation",
                session_id="session-mutation",
            ))
            patched = json.loads(skill_manage(
                action="patch",
                name="test-skill",
                old_string="Step 1: Do the thing.",
                new_string="Step 1: Do the thing safely.",
                task_id="task-mutation",
                session_id="session-mutation",
            ))
            edited = json.loads(skill_manage(
                action="edit",
                name="test-skill",
                content=VALID_SKILL_CONTENT_2,
                task_id="task-mutation",
                session_id="session-mutation",
            ))

        assert created["success"] is True
        assert patched["success"] is True
        assert edited["success"] is True
        record_created.assert_called_once_with(
            "test-skill",
            agent_created=False,
            task_id="task-mutation",
            session_id="session-mutation",
        )
        assert [call.kwargs for call in bump_patch.call_args_list] == [
            {
                "action": "patch",
                "task_id": "task-mutation",
                "session_id": "session-mutation",
            },
            {
                "action": "edit",
                "task_id": "task-mutation",
                "session_id": "session-mutation",
            },
        ]
        assert all(call.args == ("test-skill",) for call in bump_patch.call_args_list)

    def test_failed_mutations_do_not_emit_lifecycle(self, tmp_path):
        with (
            _skill_dir(tmp_path),
            patch("tools.skill_usage.record_created") as record_created,
            patch("tools.skill_usage.bump_patch") as bump_patch,
        ):
            create_result = json.loads(skill_manage(
                action="create",
                name="test-skill",
            ))
            patch_result = json.loads(skill_manage(
                action="patch",
                name="test-skill",
            ))

        assert create_result["success"] is False
        assert patch_result["success"] is False
        record_created.assert_not_called()
        bump_patch.assert_not_called()


    def test_background_review_delete_refuses_bundled_even_with_absorbed_into(self, tmp_path):
        from tools.skill_provenance import (
            BACKGROUND_REVIEW,
            reset_current_write_origin,
            set_current_write_origin,
        )

        token = set_current_write_origin(BACKGROUND_REVIEW)
        try:
            with _skill_dir(tmp_path), \
                 patch("tools.skill_usage.is_protected_builtin", return_value=False), \
                 patch("tools.skill_usage.is_hub_installed", return_value=False), \
                 patch("tools.skill_usage.is_bundled",
                       side_effect=lambda skill_name: skill_name == "bundled"):
                skill_manage(action="create", name="umbrella", content=VALID_SKILL_CONTENT)
                skill_manage(action="create", name="bundled", content=VALID_SKILL_CONTENT)
                raw = skill_manage(
                    action="delete",
                    name="bundled",
                    absorbed_into="umbrella",
                )
        finally:
            reset_current_write_origin(token)

        result = json.loads(raw)
        assert result["success"] is False
        assert "bundled" in result["error"].lower()
        assert (tmp_path / "bundled" / "SKILL.md").exists()


class TestSecurityScanGate:
    """_security_scan_skill is gated by skills.guard_agent_created config flag."""

    def test_scan_noop_when_flag_off(self, tmp_path):
        """Default config (flag off) short-circuits before running scan_skill."""
        from tools.skill_manager_tool import _security_scan_skill

        with patch("tools.skill_manager_tool._guard_agent_created_enabled", return_value=False), \
             patch("tools.skill_manager_tool.scan_skill") as mock_scan:
            result = _security_scan_skill(tmp_path)

        assert result is None
        mock_scan.assert_not_called()  # scan never ran

    def test_scan_blocks_dangerous_when_flag_on(self, tmp_path):
        """Dangerous verdict + flag on → returns an error string for the agent."""
        from tools.skill_manager_tool import _security_scan_skill
        from tools.skills_guard import ScanResult, Finding

        finding = Finding(
            pattern_id="test", severity="critical", category="exfiltration",
            file="SKILL.md", line=1, match="curl $TOKEN", description="test",
        )
        fake_result = ScanResult(
            skill_name="test",
            source="agent-created",
            trust_level="agent-created",
            verdict="dangerous",
            findings=[finding],
            summary="dangerous",
        )
        with patch("tools.skill_manager_tool._guard_agent_created_enabled", return_value=True), \
             patch("tools.skill_manager_tool.scan_skill", return_value=fake_result):
            result = _security_scan_skill(tmp_path)

        assert result is not None
        assert "Security scan blocked" in result

    def test_guard_flag_handles_config_error(self):
        """If load_config raises, _guard_agent_created_enabled defaults to False (fail-safe off)."""
        from tools.skill_manager_tool import _guard_agent_created_enabled

        with patch("hermes_cli.config.load_config", side_effect=RuntimeError("boom")):
            assert _guard_agent_created_enabled() is False

    def test_guard_flag_quoted_false_stays_disabled(self):
        """Quoted 'false' from YAML edits must not enable the guard."""
        from tools.skill_manager_tool import _guard_agent_created_enabled

        for quoted in ("false", "False", "0", "no", "off"):
            with patch("hermes_cli.config.load_config",
                       return_value={"skills": {"guard_agent_created": quoted}}):
                assert _guard_agent_created_enabled() is False, \
                    f"guard_agent_created={quoted!r} must coerce to False"


# ---------------------------------------------------------------------------
# External skills directories (skills.external_dirs) — mutations in place
# ---------------------------------------------------------------------------


@contextmanager
def _two_roots(local_dir: Path, external_dir: Path):
    """Patch the skill manager so local SKILLS_DIR = local_dir and
    get_all_skills_dirs() returns [local_dir, external_dir] in order."""
    with patch("tools.skill_manager_tool.SKILLS_DIR", local_dir), \
         patch("agent.skill_utils.get_all_skills_dirs",
               return_value=[local_dir, external_dir]):
        yield


def _write_external_skill(external_dir: Path, name: str = "ext-skill") -> Path:
    skill_dir = external_dir / name
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        f"---\nname: {name}\ndescription: An external skill.\n---\n\n"
        "# External\n\nBody with OLD_MARKER here.\n"
    )
    return skill_dir


class TestExternalSkillMutations:
    """Verify skill_manage can patch/edit/write/remove/delete skills that live
    under skills.external_dirs — in place, without duplicating to local.

    Regression for issues #4759 and #4381: the read-only gate used to refuse
    with 'Skill X is in an external directory and cannot be modified', which
    caused agents to create duplicate copies in ~/.hermes/skills/ as a
    workaround.
    """

    def test_patch_external_skill_writes_in_place(self, tmp_path):
        local = tmp_path / "local"
        external = tmp_path / "vault"
        local.mkdir(); external.mkdir()
        skill_dir = _write_external_skill(external)

        with _two_roots(local, external):
            result = _patch_skill("ext-skill", "OLD_MARKER", "NEW_MARKER")

        assert result["success"] is True, result
        assert "NEW_MARKER" in (skill_dir / "SKILL.md").read_text()
        # No duplicate in local
        assert not (local / "ext-skill").exists()


    def test_background_review_refuses_to_patch_pinned_skill(self, tmp_path):
        """Autonomous background maintenance must not modify pinned skills."""
        from tools.skill_provenance import (
            BACKGROUND_REVIEW,
            reset_current_write_origin,
            set_current_write_origin,
        )

        def _fake_get_record(skill_name):
            return {"pinned": True} if skill_name == "my-skill" else {"pinned": False}

        with _skill_dir(tmp_path):
            _create_skill("my-skill", VALID_SKILL_CONTENT)
            token = set_current_write_origin(BACKGROUND_REVIEW)
            try:
                with patch("tools.skill_usage.get_record", side_effect=_fake_get_record):
                    raw = skill_manage(
                        action="patch",
                        name="my-skill",
                        old_string="Do the thing.",
                        new_string="Do the new thing.",
                    )
            finally:
                reset_current_write_origin(token)

        result = json.loads(raw)
        assert result["success"] is False
        assert "pinned" in result["error"].lower()

<<<<<<< HEAD
    def test_background_review_unpinned_skill_not_blocked_by_pin_guard(self, tmp_path):
        """The pin guard must not over-block background writes to unpinned skills."""
        from tools.skill_provenance import (
            BACKGROUND_REVIEW,
            reset_current_write_origin,
            set_current_write_origin,
        )

        with _skill_dir(tmp_path):
            _create_skill("my-skill", VALID_SKILL_CONTENT)
            token = set_current_write_origin(BACKGROUND_REVIEW)
            try:
                from tools.skill_manager_tool import mark_background_review_skill_read

                mark_background_review_skill_read(tmp_path / "my-skill" / "SKILL.md")
                with patch(
                    "tools.skill_usage.get_record",
                    side_effect=lambda n: {"pinned": False},
                ), patch(
                    # Ownership runs before the pin guard; mark the skill
                    # curator-managed so this test still isolates the PIN guard
                    # (since #67140 an unmarked skill fails closed on ownership).
                    "tools.skill_usage.load_usage",
                    return_value={"my-skill": {"created_by": "agent"}},
                ):
                    raw = skill_manage(
                        action="patch",
                        name="my-skill",
                        old_string="Do the thing.",
                        new_string="Do the new thing.",
                    )
            finally:
                reset_current_write_origin(token)

        result = json.loads(raw)
        assert result["success"] is True

    def test_background_review_refuses_manually_authored_skill(self, tmp_path):
        """The curator must not archive/edit skills the user placed manually
        (created_by=None). Only agent-created skills are eligible for
        autonomous curation."""
        from tools.skill_provenance import (
            BACKGROUND_REVIEW,
            reset_current_write_origin,
            set_current_write_origin,
        )

        with _skill_dir(tmp_path):
            _create_skill("manual-skill", VALID_SKILL_CONTENT)
            token = set_current_write_origin(BACKGROUND_REVIEW)
            try:
                from tools.skill_manager_tool import mark_background_review_skill_read

                mark_background_review_skill_read(tmp_path / "manual-skill" / "SKILL.md")
                with patch(
                    "tools.skill_usage.load_usage",
                    return_value={"manual-skill": {"created_by": None, "use_count": 50}},
                ), patch(
                    "tools.skill_usage.get_record",
                    side_effect=lambda n: {"created_by": None, "use_count": 50} if n == "manual-skill" else {},
                ):
                    raw = skill_manage(
                        action="delete",
                        name="manual-skill",
                    )
            finally:
                reset_current_write_origin(token)

        result = json.loads(raw)
        assert result["success"] is False
        # Refusal must name the ownership reason and point at the supported way
        # in (`hermes curator adopt`), not just say "no".
        assert "not curator-managed" in result["error"].lower()
        assert "curator adopt" in result["error"]

    @pytest.mark.parametrize(
        ("action", "kwargs"),
        [
            ("patch", {"old_string": "Do the thing.", "new_string": "Changed."}),
            ("edit", {"content": VALID_SKILL_CONTENT_2}),
            ("delete", {}),
            (
                "write_file",
                {"file_path": "references/new.md", "file_content": "new"},
            ),
            ("remove_file", {"file_path": "references/existing.md"}),
        ],
    )
    def test_background_review_fails_closed_without_agent_ownership_record(
        self, tmp_path, action, kwargs
    ):
        """Every autonomous mutation requires positive agent ownership proof."""
        from tools.skill_provenance import (
            BACKGROUND_REVIEW,
            reset_current_write_origin,
            set_current_write_origin,
        )

        with _skill_dir(tmp_path):
            _create_skill("manual-skill", VALID_SKILL_CONTENT)
            support = tmp_path / "manual-skill" / "references" / "existing.md"
            support.parent.mkdir(parents=True)
            support.write_text("keep", encoding="utf-8")
            before = {
                path.relative_to(tmp_path): path.read_bytes()
                for path in tmp_path.rglob("*")
                if path.is_file()
            }

            token = set_current_write_origin(BACKGROUND_REVIEW)
            try:
                with patch("tools.skill_usage.load_usage", return_value={}):
                    raw = skill_manage(action=action, name="manual-skill", **kwargs)
            finally:
                reset_current_write_origin(token)

            after = {
                path.relative_to(tmp_path): path.read_bytes()
                for path in tmp_path.rglob("*")
                if path.is_file()
            }

        result = json.loads(raw)
        assert result["success"] is False
        # Wording landed as "not curator-managed" (#67140) rather than
        # "ownership"; the contract asserted here is the refusal + zero writes.
        assert "not curator-managed" in result["error"].lower()
        assert before == after
=======
>>>>>>> upstream/main

    def test_background_review_fails_closed_when_ownership_lookup_errors(self, tmp_path):
        from tools.skill_provenance import (
            BACKGROUND_REVIEW,
            reset_current_write_origin,
            set_current_write_origin,
        )

        with _skill_dir(tmp_path):
            _create_skill("manual-skill", VALID_SKILL_CONTENT)
            token = set_current_write_origin(BACKGROUND_REVIEW)
            try:
                with patch(
                    "tools.skill_usage.load_usage",
                    side_effect=ValueError("corrupt usage data"),
                ):
                    raw = skill_manage(
                        action="patch",
                        name="manual-skill",
                        old_string="Do the thing.",
                        new_string="Changed.",
                    )
            finally:
                reset_current_write_origin(token)

        result = json.loads(raw)
        assert result["success"] is False
        assert "ownership" in result["error"].lower()
        assert "Do the thing." in (
            tmp_path / "manual-skill" / "SKILL.md"
        ).read_text(encoding="utf-8")

class TestBackgroundOwnershipPolicyConsistency:
    """The autonomous write policy must not depend on its own side effects.

    Issue #67140: the ownership guard keyed on ``isinstance(usage_rec, dict)``,
    so a local skill with NO usage record passed. The successful write then
    called ``bump_patch()``, creating a ``created_by: null`` record — and the
    identical write was refused from then on. "Allowed exactly once" is a race
    with our own bookkeeping, not a policy.
    """

    @staticmethod
    def _bg_patch(tmp_path, name, old, new):
        from tools.skill_manager_tool import mark_background_review_skill_read
        from tools.skill_provenance import (
            BACKGROUND_REVIEW,
            reset_current_write_origin,
            set_current_write_origin,
        )

        token = set_current_write_origin(BACKGROUND_REVIEW)
        try:
            mark_background_review_skill_read(tmp_path / name / "SKILL.md")
            return json.loads(skill_manage(
                action="patch", name=name, old_string=old, new_string=new,
            ))
        finally:
            reset_current_write_origin(token)

    def test_repeated_identical_write_gets_the_same_answer(self, tmp_path, monkeypatch):
        """The real #67140 shape: no stubbing of load_usage, so the first write's
        telemetry side effect is live. Both attempts must agree."""
        monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
        (tmp_path / ".hermes" / "skills").mkdir(parents=True, exist_ok=True)
        with _skill_dir(tmp_path):
            _create_skill("flip-skill", VALID_SKILL_CONTENT)
            first = self._bg_patch(
                tmp_path, "flip-skill", "Do the thing.", "Do the new thing.",
            )
            second = self._bg_patch(
                tmp_path, "flip-skill", "Do the thing.", "Do the new thing.",
            )

        assert first["success"] == second["success"], (
            "autonomous write policy flipped between two identical attempts: "
            f"first={first.get('success')} second={second.get('success')}"
        )
        assert first["success"] is False

    def test_foreground_write_to_unmanaged_skill_still_allowed(self, tmp_path, monkeypatch):
        """Fail-closed applies to AUTONOMOUS writes only. A user-directed
        foreground edit to their own skill must keep working."""
        monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
        with _skill_dir(tmp_path):
            _create_skill("no-record", VALID_SKILL_CONTENT)
            with patch("tools.skill_usage.load_usage", return_value={}):
                res = json.loads(skill_manage(
                    action="patch", name="no-record",
                    old_string="Do the thing.", new_string="Do the new thing.",
                ))
        assert res["success"] is True

    def test_adopted_skill_becomes_writable_by_autonomous_curation(self, tmp_path, monkeypatch):
        """Adoption is the documented path from refused to allowed."""
        monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
        with _skill_dir(tmp_path):
            _create_skill("adopt-me", VALID_SKILL_CONTENT)
            with patch("tools.skill_usage.load_usage", return_value={}):
                before = self._bg_patch(
                    tmp_path, "adopt-me", "Do the thing.", "Do the new thing.",
                )
            with patch(
                "tools.skill_usage.load_usage",
                return_value={"adopt-me": {"created_by": "agent"}},
            ), patch(
                "tools.skill_usage.get_record",
                side_effect=lambda n: {"created_by": "agent", "pinned": False},
            ):
                after = self._bg_patch(
                    tmp_path, "adopt-me", "Do the thing.", "Do the new thing.",
                )

        assert before["success"] is False
        assert after["success"] is True, after


# ---------------------------------------------------------------------------
# Pinned-skill guard — skill_manage refuses only `delete` on pinned skills.
# Patches and edits go through so pinned skills can still evolve as pitfalls
# come up. The user unpins via `hermes curator unpin <name>` to delete.
# ---------------------------------------------------------------------------

class TestPinnedGuard:
    """Delete is refused on pinned skills; patch/edit/write_file/remove_file are allowed."""

    @staticmethod
    def _pin(name: str):
        """Return a patch context that marks *name* as pinned in skill_usage."""
        def _fake_get_record(skill_name, _name=name):
            return {"pinned": True} if skill_name == _name else {"pinned": False}
        return patch("tools.skill_usage.get_record", side_effect=_fake_get_record)

    def test_edit_allowed_when_pinned(self, tmp_path):
        """Pin does NOT block edit — agent can still improve pinned skills."""
        with _skill_dir(tmp_path):
            _create_skill("my-skill", VALID_SKILL_CONTENT)
            with self._pin("my-skill"):
                result = _edit_skill("my-skill", VALID_SKILL_CONTENT_2)
        assert result["success"] is True, result
        # Content updated
        content = (tmp_path / "my-skill" / "SKILL.md").read_text()
        assert "A test skill" not in content

    def test_delete_refuses_pinned(self, tmp_path):
        """Delete is the one action pin still blocks — it's the irrecoverable one."""
        with _skill_dir(tmp_path):
            _create_skill("my-skill", VALID_SKILL_CONTENT)
            with self._pin("my-skill"):
                result = _delete_skill("my-skill")
        assert result["success"] is False
        assert "pinned" in result["error"].lower()
        assert "cannot be deleted" in result["error"]
        assert "hermes curator unpin my-skill" in result["error"]
        # Skill still exists
        assert (tmp_path / "my-skill" / "SKILL.md").exists()

    def test_broken_sidecar_fails_open(self, tmp_path):
        """If skill_usage.get_record raises, we allow delete through.

        Rationale: a corrupted telemetry file shouldn't lock the agent out
        of skills it would otherwise be allowed to touch.
        """
        with _skill_dir(tmp_path):
            _create_skill("my-skill", VALID_SKILL_CONTENT)
            with patch("tools.skill_usage.get_record",
                       side_effect=RuntimeError("sidecar broken")):
                result = _delete_skill("my-skill")
        assert result["success"] is True


# ---------------------------------------------------------------------------
# Bundled-skill guard
# ---------------------------------------------------------------------------


class TestBundledSkillGuard:
    """All five mutating actions must refuse bundled skills with a clear
    message, instead of letting the syscall fail with EACCES.

    Bundled skills live under the rendered rails dir
    (``~/.hermes/hermes-agent/skills/``) which is root-owned read-only to the
    agent in the deployment model. Surfacing the reason here keeps logs
    readable and tells the agent / operator the supported way to override.
    """

    @contextmanager
    def _bundled_layout(self, tmp_path):
        """Stand up a fake rendered install: a bundled rails dir AND a user
        skills dir, both visible to ``_find_skill`` via get_all_skills_dirs.
        ``_get_bundled_dir`` resolves to the rails dir."""
        bundled = tmp_path / "hermes-agent" / "skills"
        user = tmp_path / "skills"
        bundled.mkdir(parents=True)
        user.mkdir(parents=True)
        with patch("tools.skill_manager_tool.SKILLS_DIR", user), \
             patch("agent.skill_utils.get_all_skills_dirs",
                   return_value=[user, bundled]), \
             patch("tools.skills_sync._get_bundled_dir", return_value=bundled):
            yield bundled, user

    def _seed_bundled(self, bundled_dir: Path, name: str = "bundled-one"):
        skill = bundled_dir / name
        skill.mkdir()
        (skill / "SKILL.md").write_text(VALID_SKILL_CONTENT)
        return skill

    def test_edit_bundled_refused(self, tmp_path):
        with self._bundled_layout(tmp_path) as (bundled, _):
            self._seed_bundled(bundled)
            result = _edit_skill("bundled-one", VALID_SKILL_CONTENT_2)
        assert result["success"] is False
        assert "bundled" in result["error"].lower()
        assert "read-only" in result["error"].lower()

    def test_patch_bundled_refused(self, tmp_path):
        with self._bundled_layout(tmp_path) as (bundled, _):
            self._seed_bundled(bundled)
            result = _patch_skill(
                "bundled-one", "Do the thing.", "Do something else.",
            )
        assert result["success"] is False
        assert "bundled" in result["error"].lower()

    def test_delete_bundled_refused(self, tmp_path):
        with self._bundled_layout(tmp_path) as (bundled, _):
            self._seed_bundled(bundled)
            result = _delete_skill("bundled-one", absorbed_into="")
        assert result["success"] is False
        assert "bundled" in result["error"].lower()
        # Refused before any rmtree happened.
        assert (bundled / "bundled-one" / "SKILL.md").exists()

    def test_write_file_bundled_refused(self, tmp_path):
        with self._bundled_layout(tmp_path) as (bundled, _):
            self._seed_bundled(bundled)
            result = _write_file(
                "bundled-one", "references/note.md", "hello",
            )
        assert result["success"] is False
        assert "bundled" in result["error"].lower()

    def test_remove_file_bundled_refused(self, tmp_path):
        with self._bundled_layout(tmp_path) as (bundled, _):
            skill = self._seed_bundled(bundled)
            (skill / "references").mkdir()
            (skill / "references" / "note.md").write_text("hello")
            result = _remove_file("bundled-one", "references/note.md")
        assert result["success"] is False
        assert "bundled" in result["error"].lower()
        assert (skill / "references" / "note.md").exists()

    def test_user_skill_with_same_name_unaffected(self, tmp_path):
        """The local skills dir is searched first by _find_skill, so a user
        skill with the same name as a bundled one resolves to the user copy
        and is freely editable. The guard only fires for skills whose path
        actually resolves under the bundled rails dir."""
        with self._bundled_layout(tmp_path) as (bundled, user):
            self._seed_bundled(bundled, "shared-name")
            user_skill = user / "shared-name"
            user_skill.mkdir()
            (user_skill / "SKILL.md").write_text(VALID_SKILL_CONTENT)
            result = _edit_skill("shared-name", VALID_SKILL_CONTENT_2)
        assert result["success"] is True

    def test_guard_no_op_when_bundled_dir_unavailable(self, tmp_path):
        """If _get_bundled_dir raises (e.g. broken import), the guard becomes
        a no-op. Better than blocking writes on a broken bundled-dir lookup —
        the syscall layer will still EACCES if the path is genuinely RO."""
        user = tmp_path / "skills"
        user.mkdir()
        with patch("tools.skill_manager_tool.SKILLS_DIR", user), \
             patch("agent.skill_utils.get_all_skills_dirs", return_value=[user]), \
             patch("tools.skills_sync._get_bundled_dir",
                   side_effect=RuntimeError("import broken")):
            _create_skill("free-one", VALID_SKILL_CONTENT)
            result = _edit_skill("free-one", VALID_SKILL_CONTENT_2)
        assert result["success"] is True


# ---------------------------------------------------------------------------
# State-repo commit propagation (ITRY-1283 D1)
# ---------------------------------------------------------------------------


class TestStateRepoCommitPropagation:
    """skill_manage must surface state-repo commit failures as tool errors.

    The disk write succeeds first (atomic_write_text already ran); we want the
    LLM to know the change wasn't versioned so it can retry once the state
    repo is healthy.
    """

    def test_commit_failure_propagates_as_tool_error(self, tmp_path):
        from agent.state_repo import StateRepoError

        with _skill_dir(tmp_path), \
             patch("agent.state_repo.commit_skill_change",
                   side_effect=StateRepoError("simulated index lock contention")):
            raw = skill_manage(
                action="create", name="propagation-test",
                content=VALID_SKILL_CONTENT,
            )
        result = json.loads(raw)
        assert result["success"] is False
        assert "state-repo commit failed" in result["error"]
        assert "simulated index lock contention" in result["error"]
        # The disk write happened — that's expected; the tool error tells the
        # LLM the version-control side didn't land. Retrying picks it up via
        # `git add -A` since the file is still on disk.
        assert (tmp_path / "propagation-test" / "SKILL.md").exists()

    def test_no_state_repo_does_not_block_success(self, tmp_path, monkeypatch):
        """On a dev workstation without ~/.hermes/state, commit_skill_change
        returns None. skill_manage must still succeed — the install simply
        isn't versioned."""
        monkeypatch.delenv("HERMES_STATE_DIR", raising=False)
        # state_repo_dir() falls back to ~/.hermes/state which doesn't exist
        # in tmp; force the fallback path with a fake home.
        monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path / "fake-home"))
        with _skill_dir(tmp_path):
            raw = skill_manage(
                action="create", name="dev-mode",
                content=VALID_SKILL_CONTENT,
            )
        result = json.loads(raw)
        assert result["success"] is True
        assert "state_commit_sha" not in result

    def test_external_skill_dir_skips_state_commit(self, tmp_path, monkeypatch):
        """Skills resolved from ``skills.external_dirs`` live outside
        SKILLS_DIR. The state repo only versions the local tree, so writes
        to external skills must succeed without attempting a commit (no
        bogus ``pathspec did not match`` rollback)."""
        import subprocess as _sp
        state = tmp_path / "state"
        state.mkdir()
        _sp.run(["git", "init", "--quiet", "-b", "main", str(state)], check=True)
        for k, v in (("user.email", "t@h.l"), ("user.name", "t")):
            _sp.run(["git", "-C", str(state), "config", k, v], check=True)
        (state / "skills").mkdir()
        (state / "README.md").write_text("seed\n")
        _sp.run(["git", "-C", str(state), "add", "-A"], check=True)
        _sp.run(["git", "-C", str(state), "commit", "-m", "seed", "--quiet"], check=True)
        monkeypatch.setenv("HERMES_STATE_DIR", str(state))

        # SKILLS_DIR is the local tree; the external dir lives elsewhere
        # and is discoverable via get_all_skills_dirs but not via SKILLS_DIR.
        local_skills = tmp_path / "local-skills"
        local_skills.mkdir()
        external_skills = tmp_path / "external-skills"
        external_skills.mkdir()
        # _find_skill matches by directory name, so the skill dir must be
        # named after the skill, not after VALID_SKILL_CONTENT's frontmatter.
        ext_skill = external_skills / "test-skill"
        ext_skill.mkdir()
        (ext_skill / "SKILL.md").write_text(VALID_SKILL_CONTENT)

        with patch("tools.skill_manager_tool.SKILLS_DIR", local_skills), \
             patch(
                 "agent.skill_utils.get_all_skills_dirs",
                 return_value=[local_skills, external_skills],
             ):
            raw = skill_manage(
                action="patch", name="test-skill",
                old_string="Step 1: Do the thing.",
                new_string="Step 1: Do the updated thing.",
            )
        result = json.loads(raw)
        assert result["success"] is True, result
        assert "state_commit_sha" not in result
        # No new commit landed against the state repo.
        log = _sp.run(
            ["git", "-C", str(state), "log", "--oneline"],
            check=True, capture_output=True, text=True,
        ).stdout.splitlines()
        assert len(log) == 1, f"expected only the seed commit, got: {log}"

    def test_categorized_skill_commit_lands_proper_sha(self, tmp_path, monkeypatch):
        """Creating a skill under a category must produce a proper
        ``skill: create`` commit instead of failing with a ``pathspec did
        not match`` error and silently leaving the on-disk write to be
        mopped up by ``hermes-sync``."""
        import subprocess as _sp
        state = tmp_path / "state"
        state.mkdir()
        _sp.run(["git", "init", "--quiet", "-b", "main", str(state)], check=True)
        for k, v in (("user.email", "t@h.l"), ("user.name", "t")):
            _sp.run(["git", "-C", str(state), "config", k, v], check=True)
        skills_root = state / "skills"
        skills_root.mkdir()
        (state / "README.md").write_text("seed\n")
        _sp.run(["git", "-C", str(state), "add", "-A"], check=True)
        _sp.run(["git", "-C", str(state), "commit", "-m", "seed", "--quiet"], check=True)
        monkeypatch.setenv("HERMES_STATE_DIR", str(state))

        with patch("tools.skill_manager_tool.SKILLS_DIR", skills_root), \
             patch("agent.skill_utils.get_all_skills_dirs", return_value=[skills_root]):
            raw = skill_manage(
                action="create", name="quay-run", category="quay",
                content=VALID_SKILL_CONTENT,
            )
            result = json.loads(raw)
            assert result["success"] is True, result
            assert "state_commit_sha" in result

            # Now patch the categorized skill — this is the exact path that
            # used to fail. The write must commit, not roll back.
            raw_patch = skill_manage(
                action="patch", name="quay-run",
                old_string="Step 1: Do the thing.",
                new_string="Step 1: Do the updated thing.",
            )
            patch_result = json.loads(raw_patch)
        assert patch_result["success"] is True, patch_result
        assert "state_commit_sha" in patch_result

        # The patch commit must land under skills/quay/quay-run, and the
        # commit message must be a real skill commit (not an auto: state
        # mop-up).
        files = _sp.run(
            ["git", "-C", str(state), "show", "--name-only", "--pretty=", "HEAD"],
            check=True, capture_output=True, text=True,
        ).stdout
        assert "skills/quay/quay-run/SKILL.md" in files.splitlines()
        msg = _sp.run(
            ["git", "-C", str(state), "log", "-1", "--pretty=%B"],
            check=True, capture_output=True, text=True,
        ).stdout
        assert msg.startswith("skill: patch quay-run (session ")

    def test_successful_commit_attaches_sha(self, tmp_path, monkeypatch):
        """When the state repo is configured and the commit lands, the SHA is
        attached to the tool result so downstream consumers (snapshot/replay)
        can pin the write."""
        # Build a real tmp git repo as the state repo with a skills/ subdir
        # that doubles as the SKILLS_DIR for this test.
        import subprocess as _sp
        state = tmp_path / "state"
        state.mkdir()
        _sp.run(["git", "init", "--quiet", "-b", "main", str(state)], check=True)
        for k, v in (("user.email", "t@h.l"), ("user.name", "t")):
            _sp.run(["git", "-C", str(state), "config", k, v], check=True)
        skills_root = state / "skills"
        skills_root.mkdir()
        (state / "README.md").write_text("seed\n")
        _sp.run(["git", "-C", str(state), "add", "-A"], check=True)
        _sp.run(["git", "-C", str(state), "commit", "-m", "seed", "--quiet"], check=True)
        monkeypatch.setenv("HERMES_STATE_DIR", str(state))

        with patch("tools.skill_manager_tool.SKILLS_DIR", skills_root), \
             patch("agent.skill_utils.get_all_skills_dirs", return_value=[skills_root]):
            raw = skill_manage(
                action="create", name="versioned",
                content=VALID_SKILL_CONTENT,
            )
        result = json.loads(raw)
        assert result["success"] is True
        assert "state_commit_sha" in result
        # Verify the commit actually landed with the expected message shape.
        msg = _sp.run(
            ["git", "-C", str(state), "log", "-1", "--pretty=%B"],
            check=True, capture_output=True, text=True,
        ).stdout
        assert msg.startswith("skill: create versioned (session ")
# _delete_skill — recursive-delete safety (port of Kilo Code #11240)
# ---------------------------------------------------------------------------


class TestDeleteSkillRmtreeGuard:
    """Defense-in-depth before ``shutil.rmtree`` in ``_delete_skill``.

    Mirrors the Kilo Code #11227 fix: never let a recursive skill delete
    escape the skills tree, target a skills root, or follow a symlink.
    """

    def test_normal_delete_still_works(self, tmp_path):
        with _skill_dir(tmp_path):
            _create_skill("good-skill", VALID_SKILL_CONTENT)
            result = _delete_skill("good-skill", absorbed_into="")
        assert result["success"] is True, result
        assert not (tmp_path / "good-skill").exists()

    def test_symlinked_skill_dir_refused(self, tmp_path):
        """A skill dir that is a symlink must not be rmtree'd — rmtree would
        otherwise follow it and delete the link target's contents."""
        victim = tmp_path.parent / "precious_victim"
        victim.mkdir()
        (victim / "important.txt").write_text("DO NOT DELETE")
        skills = tmp_path / "skills"
        skills.mkdir()
        evil = skills / "evil-skill"
        evil.symlink_to(victim, target_is_directory=True)
        try:
            with patch("tools.skill_manager_tool.SKILLS_DIR", skills), \
                 patch("agent.skill_utils.get_all_skills_dirs", return_value=[skills]), \
                 patch("tools.skill_manager_tool._find_skill",
                       return_value={"path": evil}):
                result = _delete_skill("evil-skill", absorbed_into="")
            assert result["success"] is False
            assert "symlink" in result["error"].lower()
            assert (victim / "important.txt").exists()
        finally:
            import shutil as _sh
            _sh.rmtree(victim, ignore_errors=True)


    def test_out_of_tree_path_refused(self, tmp_path):
        """A path that resolves outside every known skills root is refused."""
        skills = tmp_path / "skills"
        skills.mkdir()
        outside = tmp_path / "outside_skill"
        outside.mkdir()
        (outside / "SKILL.md").write_text("x")
        with patch("tools.skill_manager_tool.SKILLS_DIR", skills), \
             patch("agent.skill_utils.get_all_skills_dirs", return_value=[skills]), \
             patch("tools.skill_manager_tool._find_skill",
                   return_value={"path": outside}):
            result = _delete_skill("outside", absorbed_into="")
        assert result["success"] is False
        assert "skills root" in result["error"].lower()
        assert outside.exists()


# ---------------------------------------------------------------------------
# Curator consolidation-pass fail-closed delete guard (#29912)
# ---------------------------------------------------------------------------


@contextmanager
def _curator_pass(tmp_path, *, monkeypatch):
    """Run the body as the curator/background-review fork.

    Points HERMES_HOME at ``tmp_path/.hermes`` so skill_usage's archive path
    (``get_hermes_home()``) resolves into the same tree the skill manager
    searches, and flips ``is_background_review()`` → True so the consolidation
    guard fires.

    Also stubs the ownership check to report every skill as curator-managed.
    The ownership guard runs BEFORE the consolidation / read-before-write
    guards these tests target, and since #67140 a skill with no usage record
    fails closed — so without this, every test in this class would be refused
    by ownership and never reach the guard under test. The real curator only
    ever operates on managed sediment, so "managed" is the correct premise
    here; tests that specifically exercise the ownership guard set their own
    records instead.
    """
    hermes_home = tmp_path / ".hermes"
    skills_root = hermes_home / "skills"
    skills_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    with patch("tools.skill_manager_tool.SKILLS_DIR", skills_root), \
         patch("tools.skills_tool.SKILLS_DIR", skills_root), \
         patch("agent.skill_utils.get_all_skills_dirs", return_value=[skills_root]), \
         patch("tools.skill_usage._is_curator_managed_record", return_value=True), \
         patch("tools.skill_provenance.is_background_review", return_value=True):
        yield skills_root


def _skill_content(name: str) -> str:
    """SKILL.md whose frontmatter ``name:`` matches the directory name."""
    return (
        "---\n"
        f"name: {name}\n"
        "description: A test skill for unit testing.\n"
        "---\n\n"
        f"# {name}\n\n"
        "Step 1: Do the thing.\n"
    )

def _create_curator_skill(name: str, content: str):
    """Create a skill and record the agent ownership a real curator create has."""
    from tools.skill_usage import mark_agent_created

    result = _create_skill(name, content)
    assert result["success"] is True, result
    mark_agent_created(name)
    return result


class TestCuratorConsolidationDeleteGuard:
    """Curator consolidation may only archive verified consolidations."""

    def test_bare_prune_during_curator_pass_refused(self, tmp_path, monkeypatch):
        with _curator_pass(tmp_path, monkeypatch=monkeypatch) as skills_root:
            _create_curator_skill("active-skill", VALID_SKILL_CONTENT)
            result = _delete_skill("active-skill", absorbed_into="")
        assert result["success"] is False
        assert result.get("_fail_closed") is True
        assert (skills_root / "active-skill").exists()

<<<<<<< HEAD
    def test_omitted_absorbed_into_during_curator_pass_refused(self, tmp_path, monkeypatch):
        with _curator_pass(tmp_path, monkeypatch=monkeypatch) as skills_root:
            _create_curator_skill("active-skill", VALID_SKILL_CONTENT)
            result = _delete_skill("active-skill")  # absorbed_into omitted
        assert result["success"] is False
        assert result.get("_fail_closed") is True
        assert (skills_root / "active-skill").exists()

    def test_whitespace_absorbed_into_during_curator_pass_refused(self, tmp_path, monkeypatch):
        with _curator_pass(tmp_path, monkeypatch=monkeypatch) as skills_root:
            _create_curator_skill("active-skill", VALID_SKILL_CONTENT)
            result = _delete_skill("active-skill", absorbed_into="   ")
        assert result["success"] is False
        assert result.get("_fail_closed") is True
        assert (skills_root / "active-skill").exists()

    def test_verified_consolidation_archives_recoverably(self, tmp_path, monkeypatch):
        with _curator_pass(tmp_path, monkeypatch=monkeypatch) as skills_root:
            _create_curator_skill("umbrella", _skill_content("umbrella"))
            _create_curator_skill("narrow", _skill_content("narrow"))
            result = _delete_skill("narrow", absorbed_into="umbrella")
        assert result["success"] is True, result
        assert result.get("_archived") is True
        assert "absorbed into 'umbrella'" in result["message"]
        assert not (skills_root / "narrow").exists()
        assert (skills_root / ".archive" / "narrow").exists()
        assert (skills_root / "umbrella").exists()

    def test_consolidation_into_missing_umbrella_still_rejected(self, tmp_path, monkeypatch):
        with _curator_pass(tmp_path, monkeypatch=monkeypatch) as skills_root:
            _create_curator_skill("narrow", VALID_SKILL_CONTENT)
            result = _delete_skill("narrow", absorbed_into="ghost-umbrella")
        assert result["success"] is False
        assert "does not exist" in result["error"]
        assert (skills_root / "narrow").exists()

    def test_foreground_bare_prune_unaffected(self, tmp_path):
        with _skill_dir(tmp_path):
            _create_skill("user-skill", VALID_SKILL_CONTENT)
            result = _delete_skill("user-skill", absorbed_into="")
        assert result["success"] is True
        assert result.get("_fail_closed") is None
        assert result.get("_archived") is None
        assert not (tmp_path / "user-skill").exists()

    def test_dispatcher_preserves_usage_record_on_curator_archive(self, tmp_path, monkeypatch):
        from tools import skill_usage
        with _curator_pass(tmp_path, monkeypatch=monkeypatch):
            _create_skill("umbrella", _skill_content("umbrella"))
            _create_skill("narrow", _skill_content("narrow"))
            skill_usage.mark_agent_created("narrow")
            raw = skill_manage("delete", "narrow", absorbed_into="umbrella")
            result = json.loads(raw)
            assert result["success"] is True, result
            rec = skill_usage.get_record("narrow")
        assert rec.get("state") == skill_usage.STATE_ARCHIVED

    def test_background_review_patch_requires_skill_view_first(self, tmp_path, monkeypatch):
        from tools.skills_tool import skill_view
        from tools.skill_manager_tool import _reset_background_review_read_marks

        _reset_background_review_read_marks()
        with _curator_pass(tmp_path, monkeypatch=monkeypatch):
            _create_curator_skill("reviewed", _skill_content("reviewed"))

            blocked = json.loads(skill_manage(
                action="patch",
                name="reviewed",
                old_string="Step 1: Do the thing.",
                new_string="Step 1: Do the thing safely.",
            ))
            assert blocked["success"] is False
            assert blocked.get("_read_before_write_required") is True

            viewed = json.loads(skill_view("reviewed"))
            assert viewed["success"] is True

            allowed = json.loads(skill_manage(
                action="patch",
                name="reviewed",
                old_string="Step 1: Do the thing.",
                new_string="Step 1: Do the thing safely.",
            ))
            assert allowed["success"] is True, allowed

        _reset_background_review_read_marks()
=======
>>>>>>> upstream/main

    def test_background_review_support_file_overwrite_requires_that_file_read(self, tmp_path, monkeypatch):
        from tools.skills_tool import skill_view
        from tools.skill_manager_tool import _reset_background_review_read_marks

        _reset_background_review_read_marks()
        with _curator_pass(tmp_path, monkeypatch=monkeypatch):
            _create_curator_skill("reviewed", _skill_content("reviewed"))
            ref = tmp_path / ".hermes" / "skills" / "reviewed" / "references"
            ref.mkdir()
            (ref / "workflow.md").write_text("old workflow\n", encoding="utf-8")

            # Reading SKILL.md does not authorize overwriting a linked file.
            assert json.loads(skill_view("reviewed"))["success"] is True
            blocked = json.loads(skill_manage(
                action="write_file",
                name="reviewed",
                file_path="references/workflow.md",
                file_content="new workflow\n",
            ))
            assert blocked["success"] is False
            assert blocked.get("_read_before_write_required") is True

            assert json.loads(skill_view("reviewed", "references/workflow.md"))["success"] is True
            allowed = json.loads(skill_manage(
                action="write_file",
                name="reviewed",
                file_path="references/workflow.md",
                file_content="new workflow\n",
            ))
            assert allowed["success"] is True, allowed

        _reset_background_review_read_marks()
