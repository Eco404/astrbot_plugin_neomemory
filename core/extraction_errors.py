"""Field-level diagnostics for candidates rejected against trusted input."""


class ExtractionCapacityError(ValueError):
    """A complete encoded candidate exceeds its independent payload capacity."""

    def __init__(self, actual_chars: int, limit_chars: int):
        self.actual_chars = actual_chars
        self.limit_chars = limit_chars
        super().__init__("extraction output exceeds character limit")


class ExtractionWindowChanged(ValueError):
    """The source, ownership, or generation of the prepared window no longer matches."""


class ExtractionContextChanged(ValueError):
    """Derived memory or its write protection changed after the window was prepared."""


class InvalidExtractionCandidate(ValueError):
    """A model-selected field violates a fixed extraction precondition.

    Codes and paths are constructed by the validator; diagnostics contain no
    source text, identifiers or model-supplied error messages.
    """

    def __init__(self, code: str, path: str, message: str):
        self.code = code
        self.path = path
        super().__init__(message)


class DuplicateJSONKey(ValueError):
    """A candidate declares the same JSON field more than once."""

    def __init__(self, key: str):
        self.key = key
        super().__init__("duplicate JSON key")
