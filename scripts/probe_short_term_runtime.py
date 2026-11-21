"""Independent real short-summary generation through the AstrBot request hook.

No platform messages or live configuration changes. The manifest is frozen before
calls; at most eight HTTP requests, including retries. Evidence stays local.
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
from dataclasses import asdict
import hashlib
import io
import json
import logging
import os
from pathlib import Path
import time
from types import SimpleNamespace
from unittest.mock import patch

from scripts.verify_stage4_api import credentials, failure

MODEL = 'gpt-6-luna'
PERSONA = '我是清和，说话自然温暖，愿意帮朋友把出行安排理清。我没有亲自去线下办理事情。'
MESSAGES = [
 ('user', '周六我想去海边观鸟，还没有订交通票。'),
 ('agent', '可以先确认天气，我愿意帮你整理要问的问题，目前还没有查询。'),
 ('user', '只有观鸟站开放、当天不下大雨两个条件都满足，我才去。'),
 ('agent', '两个条件都要确认，暂时别把出行当成定局。'),
 ('user', '我还没问站点开放时间，雨衣也没找到。'),
 ('agent', '开放时间和雨具先放在待办里。'),
 ('user', '如果去，优先上午到，最晚下午四点回。'),
 ('agent', '那我们下一步先确认站点是否开放。'),
 ('user', '刚看到观鸟站周六关闭的通知，所以取消海边方案，改去城市植物馆。'),
 ('agent', '原来的海边计划取消，植物馆的开放时间还得另查。'),
 ('user', '植物馆的票还没订，我最想确认那里能不能寄存背包。'),
 ('agent', '我可以帮你整理开放时间和寄存问题，但还没查询结果。'),
 ('user', '现在先不决定到馆时间，等开放时间确认。'),
 ('agent', '好，保留待确认的问题，暂不把票和时间写成已落实。'),
 ('user', '先继续聊一会儿。'), ('agent', '我在。'),
 ('user', '我先喝口水。'), ('agent', '慢慢来。'),
 ('user', '暂时没有新的安排。'), ('agent', '那就先保留待办。'),
 ('user', '还没收到植物馆的信息。'), ('agent', '目前还不能确认。'),
 ('user', '等有消息再说。'), ('agent', '好。'),
]


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')


def spec():
    from core.config import NeoMemoryConfig
    from core.short_term_prompts import PROMPT_VERSION
    files = ['scripts/probe_short_term_runtime.py', 'core/short_term.py', 'core/short_term_prompts.py',
             'adapters/runtime.py', 'main.py', 'storage/short_term_store.py']
    return dict(model=MODEL, persona=PERSONA, messages=MESSAGES, version=PROMPT_VERSION,
        config=asdict(NeoMemoryConfig(llm_provider_id='short-runtime-llm', embedding_provider_id='',
            automatic_extraction_enabled=False, topic_consolidation_enabled=False,
            job_max_attempts=1, extraction_max_regenerations=3)),
        boundaries=[18, 24], http_budget=8, sdk_retries=0, host_request_max_retries=1,
        method='Real plugin initialize, 18 captured synthetic records, runtime.passive via inject_memory, actual background runner and real provider, ConsoleStore query, next request temporary injection; six more records then repeat. No direct short enqueue and no manual candidate recovery.',
        controls=['same space different host conversation has no injection', 'explicit host summary suppresses own injection', 'new generation never injects prior-generation short state'],
        review=['First summary keeps unconfirmed opening/weather and travel intent without claiming booking.',
                'Second summary reflects cancelled sea plan, unbooked botanical hall and pending locker/opening questions.',
                'Agent offered help but did not perform external query. Newest ten records remain raw.'],
        code_hashes={f: hashlib.sha256(Path(f).read_bytes()).hexdigest() for f in files})


async def run(directory, secret_path):
    from astrbot.api.provider import ProviderRequest
    from astrbot.core.agent.message import Message, dump_messages_with_checkpoints
    from astrbot.core.provider.sources.openai_responses_source import ProviderOpenAIResponses
    from astrbot.core.star.context import Context
    from core.identity import extract_event_identity
    from core.source_ingest import normalize_event, normalize_agent_response
    from main import NeoMemoryPlugin
    import main as main_module
    from tests.test_recall_runtime import RuntimeEvent

    frozen = json.loads((directory / 'manifest.json').read_text())
    if frozen != json.loads(json.dumps(spec())):
        raise ValueError('specification_changed')
    with (directory / 'started.json').open('x') as marker:
        json.dump({'started_at': time.time()}, marker)
    secret = credentials(secret_path)['ecoapi']
    provider = ProviderOpenAIResponses({'id': 'short-runtime-llm', 'type': 'openai_responses',
        'model': MODEL, 'api_base': secret['base_url'], 'key': [secret['api_token']], 'timeout': 120}, {})
    count = {'http': 0, 'generations': 0}
    async def http(_request):
        if count['http'] >= 8:
            raise RuntimeError('http_budget_exhausted')
        count['http'] += 1
    provider.client.max_retries = 0
    provider.client._client.event_hooks['request'].append(http)
    original_chat = provider.text_chat
    async def once(**kwargs):
        return await original_chat(**kwargs, request_max_retries=1)
    provider.text_chat = once
    async def lookup(identifier):
        return provider if identifier == 'short-runtime-llm' else None
    host = object.__new__(Context)
    host.provider_manager = SimpleNamespace(inst_map={'short-runtime-llm': provider}, provider_insts=[provider],
        embedding_provider_insts=[], get_provider_by_id=lookup)
    host.register_web_api = lambda *args, **kwargs: None
    host.persona_manager = SimpleNamespace(get_persona_v3_by_id=lambda _: {'prompt': PERSONA})
    async def current(*args):
        return 'short-runtime-conversation'
    async def conversation(*args):
        return SimpleNamespace(cid='short-runtime-conversation', persona_id='persona-test')
    host.conversation_manager = SimpleNamespace(get_curr_conversation_id=current, get_conversation=conversation)
    report = {'counts': count, 'phases': [], 'controls': {}, 'runtime_diagnostics': {}}
    plugin = None
    with patch.object(main_module.StarTools, 'get_data_dir', staticmethod(lambda _: directory / 'plugin')):
        try:
            plugin = NeoMemoryPlugin(host, frozen['config'])
            await plugin.initialize()
            actual = plugin.runtime.adapter.generate
            async def record_call(messages, **kwargs):
                count['generations'] += 1
                target = directory / f'call-{count["generations"]}'
                target.mkdir()
                save(target / 'request.json', messages)
                try:
                    raw = await actual(messages, **kwargs)
                    (target / 'candidate.txt').write_text(raw)
                    return raw
                finally:
                    save(target / 'diagnostics.json', kwargs.get('diagnostics', {}))
            plugin.runtime.adapter.generate = record_call
            event = RuntimeEvent(text='继续刚才的安排')
            captured = []
            async def capture(start, stop):
                for index in range(start, stop):
                    role, text = MESSAGES[index]
                    # AstrBot responses retain the incoming user event's message ID.
                    item = RuntimeEvent(message_id=f'short-source-{index if role == "user" else index - 1}', text=text)
                    args = dict(persona_id='persona-test', resolve_event_persona=False,
                        host_conversation_id='short-runtime-conversation', observed_at=1795000000 + index)
                    if role == 'user':
                        source = normalize_event(item, **args)
                    else:
                        source = normalize_agent_response(item, SimpleNamespace(completion_text=text, role='assistant'), **args)
                    captured.append(await plugin.source_store.capture(source))
                await plugin.maintenance.scan_once()
            def request(cid='short-runtime-conversation', contexts=None):
                value = ProviderRequest(prompt='继续刚才的安排', contexts=contexts or [])
                value.conversation = SimpleNamespace(cid=cid, persona_id='persona-test')
                return value
            async def inject(req):
                started = time.monotonic()
                await plugin.inject_memory(event, req)
                return round(time.monotonic() - started, 4)
            async def completed(previous_count):
                deadline = time.monotonic() + 160
                while time.monotonic() < deadline:
                    jobs = await plugin.jobs.list()
                    jobs = [j for j in jobs if j['kind'] == 'short_term']
                    if len(jobs) > previous_count and jobs[0]['state'] in {'succeeded','failed','cancelled'}:
                        return jobs[0]
                    await asyncio.sleep(.2)
                report['runtime_diagnostics'] = plugin.runtime.diagnostics
                raise TimeoutError('background_short_term_not_completed')
            for phase, (start, stop) in enumerate(((0,18),(18,24)), 1):
                await capture(start, stop)
                space, identity = await plugin.runtime.scope(event)
                initial = request()
                hook_seconds = await inject(initial)
                job = await completed(phase - 1)
                save(directory / f'job-{phase}.json', job)
                if job['state'] != 'succeeded':
                    raise RuntimeError('short_term_background_failed')
                listing = await plugin.console_store.context(space['space_id'], 'short_term')
                save(directory / f'console-{phase}.json', listing)
                state = listing['items'][0]
                following = request()
                next_seconds = await inject(following)
                text = '\n'.join(part.text for part in following.extra_user_content_parts)
                persisted = dump_messages_with_checkpoints([Message.model_validate(await following.assemble_context())])
                expected_to = stop - 10
                checks = dict(console_one_row=len(listing['items']) == 1,
                    expected_covered_count=state['coverage']['source_count'] == expected_to,
                    expected_version=state['content_version'] == phase,
                    newest_ten_reserved=state['covered_to_source_id'] == captured[expected_to - 1].source_id,
                    injected=bool(text) and state['display_summary_text'] in text,
                    temporary=all(p._no_save for p in following.extra_user_content_parts),
                    not_saved_in_host_history=state['display_summary_text'] not in json.dumps(persisted, ensure_ascii=False))
                report['phases'].append(dict(phase=phase, hook_seconds=hook_seconds, next_hook_seconds=next_seconds,
                    checks=checks, summary=state['summary_text'], coverage=state['coverage']))
                save(directory / 'report.json', report)
                if not all(checks.values()):
                    raise RuntimeError('short_term_runtime_assertion_failed')
            wrong = request('different-conversation')
            await inject(wrong)
            report['controls']['different_conversation_excluded'] = not wrong.extra_user_content_parts
            suppressed = request(contexts=[{'role':'user','content':'Our previous history conversation summary:已有宿主摘要'}])
            await inject(suppressed)
            report['controls']['host_summary_suppresses_injection'] = not suppressed.extra_user_content_parts
            # Test the next generation read without scheduling new model work.
            fresh = await plugin.memory_store.create_generation(space['space_id'])
            await plugin.memory_store.activate_generation(space['space_id'], fresh['generation_id'])
            snapshot = await plugin.runtime.context_store.snapshot(space['space_id'], logical_user_id=space['logical_user_id'],
                stream_id=identity.stream_id, host_conversation_id='short-runtime-conversation',
                conversation_key=state['conversation_id'])
            report['controls']['generation_isolated'] = snapshot['short_term'] is None
            report['runtime_diagnostics'] = plugin.runtime.diagnostics
            report['ok'] = all(report['controls'].values())
        except Exception as exc:
            report['failure'] = failure(exc)
            report['ok'] = False
        finally:
            if plugin is not None:
                await plugin.terminate()
            await provider.client.close()
            save(directory / 'report.json', report)
    return {'ok': report['ok'], 'counts': count, 'phases': len(report['phases']), 'failure': report.get('failure')}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('directory', type=Path)
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--credentials', type=Path, default=Path('docs/api_key.md'))
    args = parser.parse_args()
    if not args.execute:
        args.directory.mkdir(parents=True, exist_ok=False)
        save(args.directory / 'manifest.json', spec())
        print('frozen')
        return
    logging.disable(logging.CRITICAL)
    os.environ['NO_COLOR'] = '1'
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        result = asyncio.run(run(args.directory, args.credentials))
    print(json.dumps(result))


if __name__ == '__main__':
    main()
