"""Project edits over the original message component boundaries."""

from itertools import accumulate


def project_edits(parts, edits):
    """Leave each replacement in the component where its match starts."""
    edits = iter(edits)
    edit = next(edits, None)
    if edit is None:
        return parts
    cleaned = []
    for index, (part, end) in enumerate(zip(parts, accumulate(map(len, parts)))):
        start = end - len(part)
        cursor = start
        chunks = []
        while edit and (edit[0] < end or index == len(parts) - 1):
            left, right, replacement = edit
            if left >= start:
                chunks.extend((part[cursor - start:left - start], replacement))
            cursor = min(end, right)
            if right > end:
                break
            edit = next(edits, None)
        cleaned.append("".join(chunks) + part[cursor - start:])
    return cleaned
