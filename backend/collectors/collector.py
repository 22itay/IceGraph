from abc import ABC, abstractmethod

from base_classes.spark_table_action import SparkTableAction
from collectors.files_collection import FilesCollection


class Collector(SparkTableAction, ABC):
    @abstractmethod
    def collect(self) -> FilesCollection:
        pass
