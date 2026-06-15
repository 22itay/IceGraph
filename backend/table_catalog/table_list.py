import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import suppress
from typing import Optional

from pyspark.errors import AnalysisException

from base_classes.utils import timed, verify_iceberg_table
from constants import TABLE_LIST_CACHE_TTL_SECONDS
from spark_connect import open_spark_connect_session
from table_catalog.table_list_utils import (
    collect_table_candidates_from_sql,
    default_catalog,
    is_iceberg_spark_catalog,
    list_catalogs,
    list_namespaces,
)

table_list_cache_ttl_seconds = int(os.getenv("TABLE_LIST_CACHE_TTL_SECONDS", TABLE_LIST_CACHE_TTL_SECONDS))


class TableListCollector:
    _cache: Optional[tuple[float, list[str]]] = None

    def __init__(self):
        self._spark = open_spark_connect_session()

    @timed
    def collect(self) -> list[str]:
        now = time.time()
        if (
            TableListCollector._cache
            and now - TableListCollector._cache[0] < table_list_cache_ttl_seconds
        ):
            return TableListCollector._cache[1]

        candidates, iceberg_only = self._collect_candidates()
        if iceberg_only:
            result = sorted(candidates)
        else:
            result = sorted(self._filter_iceberg_tables(candidates))

        TableListCollector._cache = (now, result)
        return result

    def _collect_candidates(self) -> tuple[set[str], bool]:
        candidates: set[str] = set()
        default_catalog_name = default_catalog(self._spark)
        catalogs = list_catalogs(self._spark, default_catalog_name)
        iceberg_only = all(is_iceberg_spark_catalog(self._spark, catalog) for catalog in catalogs)

        for catalog in catalogs:
            for namespace in list_namespaces(self._spark, catalog, default_catalog_name):
                candidates.update(
                    collect_table_candidates_from_sql(
                        self._spark, catalog, namespace, default_catalog_name
                    )
                )

        return candidates, iceberg_only

    def _filter_iceberg_tables(self, candidates: set[str]) -> list[str]:
        if not candidates:
            return []

        if len(candidates) == 1:
            table = next(iter(candidates))
            return [table] if self._table_is_iceberg(table) else []

        loadable: list[str] = []
        max_workers = min(len(candidates), 8)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self._table_is_iceberg, table): table for table in candidates
            }
            for future in as_completed(futures):
                table = futures[future]
                if future.result():
                    loadable.append(table)

        return loadable

    @staticmethod
    def _table_is_iceberg(table_name: str) -> bool:
        with suppress(AnalysisException):
            verify_iceberg_table(table_name)
            return True
        return False


def collect_table_list() -> list[str]:
    return TableListCollector().collect()
