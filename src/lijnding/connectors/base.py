from __future__ import annotations
from typing import Any, Optional

class BaseConnector:
    """
    Abstract base class for all connectors. All connectors have an async
    interface, with sync operations wrapped in `asyncio.to_thread`.
    """
    async def put(self, item: Optional[Any]):
        raise NotImplementedError

    async def get(self) -> Optional[Any]:
        raise NotImplementedError
