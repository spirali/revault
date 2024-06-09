import enum
from dataclasses import dataclass
from typing import Any
from .key import Key


EntryId = int


class AnnounceResult(enum.Enum):
    FINISHED = 0
    COMPUTE_HERE = 1
    COMPUTING_ELSEWHERE = 2


@dataclass
class Entry:
    entry_id: int
    key: Key
    result: Any
