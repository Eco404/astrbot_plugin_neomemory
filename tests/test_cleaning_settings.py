from types import SimpleNamespace

import pytest
import pytest_asyncio

from core.cleaning_settings import CleaningSettings, validate_rules
from core.settings import SettingsConflict
from core.text_cleaning import default_cleaning_config
from storage.memory_store import MemoryStore
from storage.pipeline_store import PipelineStore


@pytest_asyncio.fixture
async def service(tmp_path):
    memory = MemoryStore(tmp_path / "memory.db")
    await memory.open()
    try:
        yield CleaningSettings(PipelineStore(memory), SimpleNamespace())
    finally:
        await memory.close()


@pytest.mark.asyncio
async def test_release_settings_round_trip(service):
    await service.load()
    initial = default_cleaning_config()
    assert service.config == initial
    saved = await service.save({**initial, "groups": [], "rules": []})
    assert saved["revision"] == initial["revision"] + 1
    assert (await service.pipeline.get_state(service.KEY))["format_version"] == 1
    await service.load()
    assert service.config == saved
    with pytest.raises(SettingsConflict):
        await service.save(initial)


def test_rule_configuration_requires_current_kinds_and_valid_root_order():
    rule = dict(id="r", name="条目", enabled=True, group_id=None, kind="literal", pattern="x", replacement="", skip_markdown=False)
    group = dict(id="g", enabled=True, name="分组")
    order = [dict(kind="rule", id="r"), dict(kind="group", id="g")]
    config = {"groups": [group], "rules": [rule], "revision": 1, "root_order": order}
    validate_rules(config)
    with pytest.raises(ValueError):
        validate_rules(dict(config, rules=[dict(rule, kind="unknown")]))
    for invalid in ([*order, order[0]], order[:1], [order[0], order[0]]):
        with pytest.raises(ValueError):
            validate_rules(dict(config, root_order=invalid))
