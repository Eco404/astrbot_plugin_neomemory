"""Identify recent delivered assistant messages missing from AstrBot's history."""

from collections import Counter
from itertools import groupby


def _assistant_texts(contexts):
    for message in contexts or ():
        get = message.get if isinstance(message, dict) else lambda key: getattr(message, key, None)
        if get("role") != "assistant":
            continue
        content = get("content")
        if isinstance(content, str):
            yield content.strip()
        elif isinstance(content, (list, tuple)):
            parts = (part.get("text") if isinstance(part, dict) else getattr(part, "text", None)
                     for part in content)
            yield "".join(text for text in parts if isinstance(text, str)).strip()


def missing_deliveries(history, contexts):
    """The host may keep separate assistant messages or a single complete contact."""
    available = Counter(_assistant_texts(contexts))
    missing = []
    for contact, rows in groupby(reversed(history), key=lambda row: row.get("proactive_contact_id")):
        texts = [text for row in rows if row.get("delivered_proactive")
                 for text in row.get("delivered_texts", [row["content_text"]])]
        if not texts:
            continue
        if contact and len(texts) > 1:
            combined = next((value for value in ("".join(texts).strip(), "\n".join(texts).strip())
                             if available[value]), None)
            if combined:
                available[combined] -= 1
                continue
        for text in texts:
            if available[text.strip()]:
                available[text.strip()] -= 1
            else:
                missing.append(text)
    return missing
