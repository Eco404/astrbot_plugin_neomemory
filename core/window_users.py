"""Window-local user aliases for topic synthesis and rolling summaries."""

from .memory_text import map_user_references


class WindowUsers:
    def __init__(self, allowed_ids):
        self.aliases = {identity: f"u{index}" for index, identity in enumerate(sorted(set(allowed_ids)), 1)}
        self.identities = {alias: identity for identity, alias in self.aliases.items()}

    def encode(self, identity):
        # A read-only topic or source excerpt may mention someone outside the
        # authoritative set. Give them a distinct alias for input only.
        if identity not in self.aliases:
            self.aliases[identity] = f"u{len(self.aliases) + 1}"
        return self.aliases[identity]

    def encode_text(self, text):
        return map_user_references(text, self.encode)

    def decode(self, alias):
        """Return the full logical-user ID for a window-local alias."""
        return self.identities[alias]

    def decode_text(self, text):
        return map_user_references(text, self.identities.__getitem__)
