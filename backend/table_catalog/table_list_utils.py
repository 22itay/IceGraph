from contextlib import suppress
from typing import Optional

from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from base_classes.utils import get_spark_row_value, qualify_table_name


def namespace_to_str(namespace) -> str:
    if isinstance(namespace, (list, tuple)):
        return ".".join(str(part) for part in namespace)
    return str(namespace)


def sql_ident(*parts: str) -> str:
    return ".".join(f"`{part}`" for part in parts if part)


def get_spark_catalog_config_value(spark: SparkSession, catalog: str, suffix: str) -> Optional[str]:
    with suppress(Exception):
        return spark.conf.get(f"spark.sql.catalog.{catalog}{suffix}")
    return None


def default_catalog(spark: SparkSession) -> str:
    with suppress(Exception):
        return spark.catalog.currentCatalog()
    return "spark_catalog"


def is_iceberg_spark_catalog(spark: SparkSession, catalog: str) -> bool:
    impl = get_spark_catalog_config_value(spark, catalog, "") or ""
    return "org.apache.iceberg.spark.SparkCatalog" in impl and "SparkSessionCatalog" not in impl


def list_catalogs(spark: SparkSession, default_catalog_name: str) -> list[str]:
    catalogs: list[str] = []
    with suppress(AnalysisException):
        catalogs = [get_spark_row_value(row, "catalog") for row in spark.sql("SHOW CATALOGS").collect()]

    catalogs = [catalog for catalog in catalogs if catalog]
    return catalogs or [default_catalog_name]


def collect_namespace_rows(spark: SparkSession, query: str) -> list[str]:
    namespaces: set[str] = set()

    with suppress(AnalysisException):
        for row in spark.sql(query).collect():
            namespace = namespace_to_str(
                get_spark_row_value(row, "namespace", "namespace_name", "databaseName")
            )
            if namespace:
                namespaces.add(namespace)

    return list(namespaces)


def list_namespaces(spark: SparkSession, catalog: str, default_catalog_name: str) -> list[str]:
    namespaces = collect_namespace_rows(spark, f"SHOW NAMESPACES IN {sql_ident(catalog)}")
    if namespaces:
        return namespaces

    if catalog == default_catalog_name:
        namespaces = collect_namespace_rows(spark, "SHOW NAMESPACES")
        if namespaces:
            return namespaces

        namespaces = collect_namespace_rows(spark, "SHOW DATABASES")
        if namespaces:
            return namespaces

    with suppress(Exception):
        current_database = spark.catalog.currentDatabase()
        if current_database:
            return [current_database]

    return ["default"]


def collect_table_names_from_query(
    spark: SparkSession,
    query: str,
    catalog: str,
    namespace: str,
    default_catalog_name: str,
) -> set[str]:
    tables: set[str] = set()

    with suppress(AnalysisException):
        df = spark.sql(query)
        if "isTemporary" in df.columns:
            df = df.filter(~F.col("isTemporary"))

        for row in df.collect():
            table_name = get_spark_row_value(row, "tableName", "table")
            if table_name:
                tables.add(qualify_table_name(catalog, namespace, table_name, default_catalog_name))

    return tables


def collect_table_candidates_from_sql(
    spark: SparkSession,
    catalog: str,
    namespace: str,
    default_catalog_name: str,
) -> set[str]:
    tables = collect_table_names_from_query(
        spark,
        f"SHOW TABLES IN {sql_ident(catalog, namespace)}",
        catalog,
        namespace,
        default_catalog_name,
    )
    if tables:
        return tables

    if catalog == default_catalog_name:
        return collect_table_names_from_query(
            spark,
            f"SHOW TABLES IN {sql_ident(namespace)}",
            catalog,
            namespace,
            default_catalog_name,
        )

    return tables
