"""Measure extraction-context selection on fixed synthetic inputs with real embeddings."""
from __future__ import annotations
import argparse
import asyncio
import contextlib
import hashlib
import io
import json
import logging
import os
from pathlib import Path
from types import SimpleNamespace
import time

from scripts.verify_stage4_api import credentials, failure

A = [f"第{i+1}份独立档案：我完成了天文馆观测活动的望远镜校准，调整赤道仪并记录木星位置。" for i in range(31)]
B = "上周小提琴弓送到琴行换弓毛，师傅要等新材料到货才开始。"
C = "社区图书角的书架组装已完成，但缺固定螺栓，尚未开放使用。"
RECENT = [
 "我给阳台的罗勒浇水，叶片状态良好。", "洗衣机的排水滤网已经清理。", "我为月底旅行查了公交路线，尚未订票。",
 "旧手机的数据已转移，新设备还没有装全部软件。", "我听完了朋友推荐的播客。", "今天练习做豆腐汤，最后加了少量盐。",
 "社区电影放映改到周末，具体时间未定。", "我收到了图书馆借阅到期提醒。", "一个朋友搬家，我帮忙折好纸箱。", "室内灯泡换好，开关测试正常。",
]
NODES = [(f"a{i:02}", text) for i,text in enumerate(A)] + [("b_target",B),("c_target",C)] + [(f"z{i:02}",text) for i,text in enumerate(RECENT)]
QA = "天文馆观测活动的望远镜校准、赤道仪调整和木星位置记录有新进展。"
QB = "那把送去修的琴弓，现在材料到了，可以换毛了。"
QC = "图书角那套架子的螺栓到货了，装好后就能开放。"
CASES = [
 {"name":"two_topics","queries":[QA,QB],"targets":["b_target"],"limit":12},
 {"name":"three_topics","queries":[QA,QB,QC],"targets":["b_target","c_target"],"limit":12},
 {"name":"single_topic","queries":[QA],"targets":[],"limit":12},
 {"name":"duplicate_topic","queries":[QA,QA,QB],"targets":["b_target"],"limit":12},
 {"name":"short_chatter","queries":[QA,"嗯嗯，好的。"],"targets":[],"limit":12},
 {"name":"unrelated_question","queries":[QA,"火星是否存在未发现的生命？"],"targets":[],"limit":12},
 {"name":"recent_overlap","queries":[QA,RECENT[-1]],"targets":["z09"],"limit":12},
 {"name":"small_budget","queries":[QA,QB,QC],"targets":["b_target","c_target"],"limit":3},
]

def save(path,value):path.write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')

class Memory:
    def __init__(self,nodes):self.nodes=nodes
    async def extraction_context_page(self,*args,after='',limit=128,recent=False):
        nodes=list(reversed(self.nodes)) if recent else [n for n in self.nodes if not after or n['node_id']>after]
        page=nodes[:limit]
        return {'nodes':page,'after':page[-1]['node_id'] if page else after,'has_more':len(nodes)>limit}

class FixedEmbeddings:
    fingerprint='frozen-real-vectors'
    batch_size=16
    def __init__(self,vectors):self.vectors=vectors
    async def embed(self,texts,**kwargs):return [self.vectors[t] for t in texts]
    async def cached(self,texts,**kwargs):return [self.vectors[t] for t in texts]

async def compare(vectors):
    from core.extraction_context import select_context_ids, CONTEXT_VERSION
    memory=Memory([{'node_id':i,'content_text':t,'content_version':1} for i,t in NODES])
    embeddings=FixedEmbeddings(vectors)
    results=[]
    for case in CASES:
        started=time.perf_counter()
        ids=await select_context_ids(memory,embeddings,space_id='synthetic',generation_id='synthetic-generation',
            sources=[{'content_text':t} for t in case['queries']],limit=case['limit'],timeout_seconds=10)
        results.append({'name':case['name'], 'policy':CONTEXT_VERSION, 'ids':ids,
            'seconds':time.perf_counter()-started, 'targets_present':[i for i in case['targets'] if i in ids],
            'node_count':len(ids),'unique':len(ids)==len(set(ids)),
            'selected_utf8_bytes':sum(len(dict(NODES)[i].encode()) for i in ids)})
    return results

async def run(directory,secrets):
    from astrbot.core.provider.sources.openai_embedding_source import OpenAIEmbeddingProvider
    from astrbot.core.star.context import Context
    from adapters.providers import ProviderAdapter
    from core.embeddings import EmbeddingService
    from storage.cache_store import CacheStore
    cfg=secrets['Cloudflare Embedding']
    provider=OpenAIEmbeddingProvider({'id':'probe-embedding','type':'openai_embedding','embedding_model':cfg['model'],
        'embedding_api_base':cfg['api_address'].rstrip('/')+'/accounts/'+cfg['account_id']+'/ai/v1',
        'embedding_api_key':cfg['api_token'],'embedding_dimensions':0,'embedding_dimensions_mode':'never','timeout':30}, {})
    provider.client.max_retries=0
    report={'synthetic_only':True,'generative_calls':0,'http_requests':0,'model':cfg['model']}
    async def count(request):
        if report['http_requests']>=6:raise RuntimeError('probe_http_limit')
        report['http_requests']+=1
    provider.client._client.event_hooks['request'].append(count)
    async def lookup(identifier):return provider if identifier=='probe-embedding' else None
    host=object.__new__(Context)
    host.provider_manager=SimpleNamespace(inst_map={'probe-embedding':provider},provider_insts=[],embedding_provider_insts=[provider],get_provider_by_id=lookup)
    cache=CacheStore(directory/'cache.db');await cache.open()
    embeddings=EmbeddingService(ProviderAdapter(host),cache,provider_id='probe-embedding',batch_size=16)
    texts=list(dict.fromkeys([t for _,t in NODES]+[t for c in CASES for t in c['queries']]))
    try:
        started=time.perf_counter()
        returned=await embeddings.embed(texts,namespace='context-coverage',timeout_seconds=60)
        report['embedding_seconds']=time.perf_counter()-started
        vectors=dict(zip(texts,returned,strict=True));save(directory/'vectors.json',vectors)
        report['texts']=len(texts);report['dimension']=len(returned[0]);report['cases']=await compare(vectors)
        report['all_limits_ok']=all(r['unique'] and r['node_count'] <= c['limit'] for c,r in zip(CASES,report['cases']))
    except Exception as exc:report['failure']=failure(exc)
    finally:await cache.close();await provider.terminate()
    save(directory/'report.json',report);return report

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--credentials',type=Path,default=Path('docs/api_key.md'));args=parser.parse_args()
    args.output.mkdir(parents=True,exist_ok=False);(args.output/'host/data/config').mkdir(parents=True)
    os.environ['ASTRBOT_ROOT']=str((args.output/'host').resolve())
    from core.extraction_context import CONTEXT_VERSION, QUERY_COVERAGE_MIN_SCORE
    manifest={'nodes':NODES,'cases':CASES,'policy':CONTEXT_VERSION,
        'query_coverage_min_score':QUERY_COVERAGE_MIN_SCORE,'http_limit':6,'generative_calls':0,
        'acceptance':['selected nodes within the requested limit','target coverage reported','no private input']}
    raw=json.dumps(manifest,ensure_ascii=False,sort_keys=True);save(args.output/'manifest.json',{'specification':manifest,'sha256':hashlib.sha256(raw.encode()).hexdigest()})
    logging.disable(logging.CRITICAL)
    with contextlib.redirect_stdout(io.StringIO()),contextlib.redirect_stderr(io.StringIO()):
        from loguru import logger
        logger.remove();report=asyncio.run(run(args.output,credentials(args.credentials)))
    print(json.dumps({k:v for k,v in report.items() if k!='cases'},ensure_ascii=False))
    return int('failure' in report)
if __name__=='__main__':raise SystemExit(main())
