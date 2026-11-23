"""Local evidence for passive recall, computed over the current space only."""

from __future__ import annotations

import math
import re
import unicodedata
from collections import Counter

import numpy as np

_REFERENCE = re.compile(r"\{\{user:[^{}\s]+\}\}")
# A standalone follow-up can use its conversational antecedent as a query.
_FOLLOWUP = re.compile(
    r"(?:(?:那|那么|然后|后来|之后|接下来|再后来|结果|那件事|这件事|那个|这个)"
    r"(?:呢|怎么样了?|怎样了?|怎么了|结果呢|结局呢|后来呢)?|还有呢|"
    r"what happened (?:next|then)|and then|what about (?:it|that)|tell me more)"
)


# Subjectless questions can ask for a place, time or outcome supplied by the previous turn.
_REFERENTIAL = re.compile(
    r"(?:那|那么)?(?:它们?|他|她|那件事|这件事|那个|这个|后来|最后|最终|之后|当时)"
    r"(?:后来|最后|当时|是|又|到底|现在|被|要|会|都|还)*"
    r"(?:怎么|怎样|为什么|什么时候|在哪|去哪|到哪|.{0,2}哪[里儿]?).{0,8}"
    r"|(?:what|where|when|how|why) (?:was|were|is|are|did|does|do) (?:it|that|they)(?: .{1,24})?"
)
_ACKNOWLEDGEMENT = re.compile(r"嗯+|哦+|好(?:的|呀|啊|哒)?|收到|明白了|谢谢(?:你)?|多谢|好的谢谢|ok(?:ay)?|thanks|thank you|got it")


def _standalone(text: str) -> str:
    return text.casefold().strip(" \t\n,.!?，。！？…")


def is_followup(text: str) -> bool:
    text = _standalone(text)
    return bool(_FOLLOWUP.fullmatch(text) or _REFERENTIAL.fullmatch(text))


def is_acknowledgement(text: str) -> bool:
    return not text.rstrip().endswith(("?", "？")) and bool(_ACKNOWLEDGEMENT.fullmatch(_standalone(text)))


def _terms(text: str) -> set[str]:
    text = unicodedata.normalize("NFKC", _REFERENCE.sub(" ", text)).casefold()
    terms = set()
    # Split Chinese from alphanumerics even in mixed names such as 型号X57.
    for run in re.findall(r"[\u3400-\u9fff]+|[^\W_\u3400-\u9fff]+", text):
        if "\u3400" <= run[0] <= "\u9fff":
            terms.update(run[i:i + 2] for i in range(len(run) - 1))
        else:
            terms.add(run)
    return terms


def lexical_scores(nodes, queries: list[str]) -> list[np.ndarray]:
    """IDF-weighted query coverage, tokenizing each document once per recall."""
    term_sets = [_terms(query) for query in queries]
    query_terms = set().union(*term_sets)
    overlaps = []
    frequencies = Counter()
    for node in nodes:
        terms = _terms(f"{node.get('title') or ''} {node['content_text']}") & query_terms
        overlaps.append(terms)
        frequencies.update(terms)
    weights = {term: math.log1p((len(nodes) - frequencies[term] + .5) / (frequencies[term] + .5))
               for term in query_terms}
    scores = []
    for terms in term_sets:
        total = sum(weights[term] for term in terms)
        values = np.zeros(len(nodes), dtype=float)
        if total:
            for index, document in enumerate(overlaps):
                values[index] = sum(weights[term] for term in document & terms) / total
        scores.append(values)
    return scores
