import json

import pytest

from scripts.verify_narrative_api import CASES, prepare_cases
from core.prompts import PROMPT_VERSION


@pytest.mark.asyncio
async def test_narrative_probe_prepares_independent_private_and_group_windows(tmp_path):
    sources, memory, service, prepared = await prepare_cases(tmp_path)
    try:
        assert len(prepared) == len(CASES) == 2
        private, group = prepared
        assert private[1].space["space_type"] == "private"
        assert group[1].space["space_type"] == "group"
        for case, batch, references, directory in prepared:
            payload = json.loads(references.messages()[1]["content"])
            assert len(payload["sources"]) == len(case["messages"])
            assert batch.reference_protocol == "reference.v1"
            assert batch.prompt_version == PROMPT_VERSION
            assert json.loads((directory / "input.json").read_text())["prompt_version"] == batch.prompt_version
            assert payload["known_nodes"] == []
            assert payload["known_profiles"] == []
            assert (directory / "input.json").is_file()
            assert (directory / "request.json").is_file()
        group_payload = json.loads(group[2].messages()[1]["content"])
        users = {source["actor_logical_user_id"] for source in group_payload["sources"] if source["actor_kind"] == "user"}
        assert len(users) == 2
    finally:
        await memory.close()
        await sources.close()
