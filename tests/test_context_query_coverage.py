"""Independent candidate-allocation cases; fixed vectors are not semantic evidence."""
import math
import numpy as np
import pytest
from core.extraction_context import select_context_ids, _rank_page_with_query_coverage

class Memory:
    def __init__(self,nodes,page_size=128):self.nodes=nodes;self.page_size=page_size;self.scanned=[]
    async def extraction_context_page(self,*args,after='',limit=128,recent=False):
        nodes=list(reversed(self.nodes)) if recent else [n for n in self.nodes if not after or n['node_id']>after]
        size=limit if recent else min(limit,self.page_size);page=nodes[:size]
        if not recent:self.scanned.extend(n['node_id'] for n in page)
        return {'nodes':page,'after':page[-1]['node_id'] if page else after,'has_more':len(nodes)>size}
class Embeddings:
    fingerprint='fixed';batch_size=16
    def __init__(self,vectors):self.vectors=vectors;self.calls=[]
    async def embed(self,texts,**kwargs):self.calls.append(list(texts));return [self.vectors[t] for t in texts]
    async def cached(self,texts,**kwargs):return [self.vectors[t] for t in texts]

def fixture():
    nodes=[{'node_id':f'a{i:02}','content_text':'a','content_version':1} for i in range(31)]
    nodes += [{'node_id':'b_target','content_text':'b','content_version':1}]
    nodes += [{'node_id':f'z{i:02}','content_text':'recent','content_version':1} for i in range(10)]
    vectors={'a':[1,0,0,0],'b':[0,.8,.6,0],'recent':[0,0,0,1],
             'qa':[1,0,0,0],'qb':[0,1,0,0],'weak':[.1,.2,math.sqrt(.95),0]}
    return nodes,vectors
async def selected(nodes,vectors,queries,limit=30,page_size=128):
    memory=Memory(nodes,page_size);embedding=Embeddings(vectors)
    result=await select_context_ids(memory,embedding,space_id='s',generation_id='g',sources=[{'content_text':q} for q in queries],
        limit=limit,timeout_seconds=10)
    return result,memory,embedding

@pytest.mark.asyncio
async def test_lesser_topic_best_is_preserved_without_growing_context():
    nodes,vectors=fixture()
    new,_,_=await selected(nodes,vectors,['qa','qb'])
    assert len(new)==30
    assert new[:10]==tuple(f'z{i:02}' for i in reversed(range(10)))
    assert 'b_target' in new
    assert len(set(new))==30
    actual_default = await select_context_ids(
        Memory(nodes), Embeddings(vectors), space_id='s', generation_id='g',
        sources=[{'content_text': query} for query in ['qa', 'qb']],
        limit=30, timeout_seconds=10,
    )
    assert actual_default == new

@pytest.mark.asyncio
async def test_duplicate_queries_and_overlap_do_not_claim_extra_slots():
    nodes,vectors=fixture()
    once,_,_=await selected(nodes,vectors,['qa','qb'])
    duplicate,_,embedding=await selected(nodes,vectors,['qa','qa','qb'])
    assert duplicate==once
    assert embedding.calls==[['qa','qb']]
    overlapping,_,_=await selected(nodes,vectors,['qa','recent'])
    baseline,_,_=await selected(nodes,vectors,['qa'])
    assert overlapping==baseline

@pytest.mark.asyncio
@pytest.mark.parametrize('page_size',[1,7,128])
async def test_paging_has_deterministic_global_and_query_best(page_size):
    nodes,vectors=fixture()
    whole,_,_=await selected(nodes,vectors,['qa','qb'])
    paged,memory,_=await selected(nodes,vectors,['qa','qb'],page_size=page_size)
    assert paged==whole
    assert memory.scanned==[n['node_id'] for n in nodes]

@pytest.mark.asyncio
@pytest.mark.parametrize('limit',[0,1,2,3,4,10,30,50])
async def test_small_limits_preserve_hard_bound_and_recent_priority(limit):
    nodes,vectors=fixture()
    result,_,_=await selected(nodes,vectors,['qa','qb'],limit=limit)
    assert len(result)==min(limit,len(nodes)) and len(set(result))==len(result)
    if limit:
        assert result[0]=='z09'
    if limit==1:
        assert result==('z09',) # no semantic quota remains

@pytest.mark.asyncio
async def test_low_similarity_query_does_not_displace_global_candidates():
    nodes,vectors=fixture()
    # A normalized unrelated query has no cosine > .2 with any node.
    vectors['weak']=[.1,.2,-math.sqrt(.95),0]
    result,_,_=await selected(nodes,vectors,['qa','weak'])
    baseline,_,_=await selected(nodes,vectors,['qa'])
    assert result==baseline


def test_query_best_ties_are_page_order_independent():
    nodes=[{'node_id':'b'},{'node_id':'a'}];vectors=[[1,0],[1,0]];queries=[[1,0]]
    best,coverage=[],{}
    _rank_page_with_query_coverage(best,coverage,nodes,vectors,queries,2)
    # Inputs from storage are normally sorted; equal-score identity must not depend on page layout.
    assert coverage[0][1]=='a'
