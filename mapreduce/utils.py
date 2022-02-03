"""Utils file.

This file is to house code common between the Manager and the Worker

"""

from enum import Enum
import json
import pathlib


class WorkerState(Enum):
    """Keep track of worker state."""

    READY = 1
    BUSY = 2
    DEAD = 3


class ManagerState(Enum):
    """Keep track of worker state."""

    READY = 1
    BUSY = 2


class ManagerStage(Enum):
    """Keep track of worker stage."""

    MAPPING = 1
    GROUPING = 2
    REDUCING = 3


class PathJSONEncoder(json.JSONEncoder):
    """
    Extended the Python JSON encoder to encode Pathlib objects.

    Docs: https://docs.python.org/3/library/json.html

    Usage:
    >>> json.dumps({
            "executable": TESTDATA_DIR/"exec/wc_map.sh",
        }, cls=PathJSONEncoder)
    """

    def default(self, o):
        """Override base class method to include Path object serialization."""
        if isinstance(o, pathlib.Path):
            return str(o)
        return super().default(o)
