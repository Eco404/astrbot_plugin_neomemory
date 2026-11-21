"""Compatibility entrypoint; the stage-six smoke also covers maintenance flows."""

import asyncio

from full_smoke import main

if __name__ == "__main__":
    asyncio.run(main())
