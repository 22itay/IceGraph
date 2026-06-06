import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import suppress
from typing import Optional

from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession

from base_classes.utils import timed
from spark_connect import open_spark_connect_session

TABLE_LIST_CACHE_TTL_SECONDS = 60
_table_list_cache: Optional[tuple[float, list[str]]] = None


def _row_value(row, *names):
    for name in names:
        value = getattr(row, name, None)
        if value is None and hasattr(row, "__getitem__"):
            with suppress(Exception):
                value = row[name]
        if value is not None:
            return value
    return None


def _namespace_to_str(namespace) -> str:
    if isinstance(namespace, (list, tuple)):
        return ".".join(str(part) for part in namespace)
    return str(namespace)


def _sql_ident(*parts: str) -> str:
    return ".".join(f"`{part}`" for part in parts if part)


def _catalog_conf(spark: SparkSession, catalog: str, suffix: str) -> Optional[str]:
    with suppress(Exception):
        return spark.conf.get(f"spark.sql.catalog.{catalog}{suffix}")
    return None


def _default_catalog(spark: SparkSession) -> str:
    with suppress(Exception):
        return spark.catalog.currentCatalog()
    return "spark_catalog"


def _is_iceberg_spark_catalog(spark: SparkSession, catalog: str) -> bool:
    impl = _catalog_conf(spark, catalog, "") or ""
    return "org.apache.iceberg.spark.SparkCatalog" in impl and "SparkSessionCatalog" not in impl


def _list_catalogs(spark: SparkSession, default_catalog: str) -> list[str]:
    catalogs: list[str] = []
    with suppress(AnalysisException):
        catalogs = [_row_value(row, "catalog") for row in spark.sql("SHOW CATALOGS").collect()]

    catalogs = [catalog for catalog in catalogs if catalog]
    return catalogs or [default_catalog]


def _list_namespaces(spark: SparkSession, catalog: str, default_catalog: str) -> list[str]:
    seen: set[str] = set()
    namespaces: list[str] = []

    def add_namespace(namespace: Optional[str]) -> None:
        if namespace and namespace not in seen:
            seen.add(namespace)
            namespaces.append(namespace)

    queries = [f"SHOW NAMESPACES IN {_sql_ident(catalog)}"]
    if catalog == default_catalog:
        queries.extend(["SHOW NAMESPACES", "SHOW DATABASES"])

    for query in queries:
        with suppress(AnalysisException):
            for row in spark.sql(query).collect():
                add_namespace(_namespace_to_str(_row_value(row, "namespace", "namespace_name", "databaseName")))
        if namespaces:
            return namespaces

    with suppress(Exception):
        add_namespace(spark.catalog.currentDatabase())

    return namespaces or ["default"]


def _qualify_table_name(catalog: str, namespace: str, table: str, default_catalog: str) -> str:
    if catalog == default_catalog:
        return f"{namespace}.{table}"
    return f"{catalog}.{namespace}.{table}"


def _collect_table_candidates_from_sql(
    spark: SparkSession,
    catalog: str,
    namespace: str,
    default_catalog: str,
) -> set[str]:
    tables: set[str] = set()
    queries = [f"SHOW TABLES IN {_sql_ident(catalog, namespace)}"]

    if catalog == default_catalog:
        queries.append(f"SHOW TABLES IN {_sql_ident(namespace)}")

    for query in queries:
        with suppress(AnalysisException):
            for row in spark.sql(query).collect():
                table_name = _row_value(row, "tableName", "table")
                if not table_name:
                    continue

                is_temporary = _row_value(row, "isTemporary")
                if is_temporary in (True, "true"):
                    continue

                tables.add(_qualify_table_name(catalog, namespace, table_name, default_catalog))

        if tables:
            return tables

    return tables


def _is_iceberg_table(spark: SparkSession, table_name: str) -> bool:
    with suppress(AnalysisException, AttributeError, IndexError):
        provider_row = (
            spark.sql(f"DESCRIBE FORMATTED {table_name}")
            .filter("col_name = 'Provider'")
            .collect()
        )
        if provider_row:
            return provider_row[0].data_type.lower().strip() == "iceberg"
    return False


def _filter_iceberg_tables(spark: SparkSession, candidates: set[str]) -> list[str]:
    if not candidates:
        return []

    if len(candidates) == 1:
        table = next(iter(candidates))
        return [table] if _is_iceberg_table(spark, table) else []

    loadable: list[str] = []
    max_workers = min(len(candidates), 8)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_is_iceberg_table, spark, table): table for table in candidates}
        for future in as_completed(futures):
            table = futures[future]
            if future.result():
                loadable.append(table)

    return loadable


def _collect_candidates(spark: SparkSession) -> tuple[set[str], bool]:
    candidates: set[str] = set()
    default_catalog = _default_catalog(spark)
    catalogs = _list_catalogs(spark, default_catalog)
    iceberg_only = all(_is_iceberg_spark_catalog(spark, catalog) for catalog in catalogs)

    for catalog in catalogs:
        for namespace in _list_namespaces(spark, catalog, default_catalog):
            candidates.update(_collect_table_candidates_from_sql(spark, catalog, namespace, default_catalog))

    return candidates, iceberg_only


@timed
def collect_table_list() -> list[str]:
    global _table_list_cache

    now = time.time()
    if _table_list_cache and now - _table_list_cache[0] < TABLE_LIST_CACHE_TTL_SECONDS:
        return _table_list_cache[1]

    spark = open_spark_connect_session()
    candidates, iceberg_only = _collect_candidates(spark)

    if iceberg_only:
        result = sorted(candidates)
    else:
        result = sorted(_filter_iceberg_tables(spark, candidates))

    _table_list_cache = (now, result)
    return result
