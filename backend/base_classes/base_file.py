from dataclasses import dataclass, fields
from typing import List

from constants import FileType


@dataclass
class HiddenFile:
    pass


@dataclass
class BaseFile:
    type: FileType
    file_path: str
    child_files: List[str]

    def to_dict(self):
        result_dict = {field.name: getattr(self, field.name) for field in fields(self) if not isinstance(getattr(self, field.name), HiddenFile)}
        result_dict["type"] = result_dict["type"].value

        return result_dict
