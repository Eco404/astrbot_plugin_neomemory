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


def is_followup(text: str) -> bool:
    return bool(_FOLLOWUP.fullmatch(text.casefold().strip(" \t\n,.!?，。！？…")))


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
