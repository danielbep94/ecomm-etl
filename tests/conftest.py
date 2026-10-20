import os

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("ecomm-etl-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
    )
    if os.environ.get("ECOMM_TEST_DELTA_JARS"):
        builder = (
            builder.config("spark.jars", os.environ["ECOMM_TEST_DELTA_JARS"])
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.databricks.delta.snapshotPartitions", "2")
        )
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def product_rows():
    def row(internal=1, upc=11, sku=111, name="Product", description="Description"):
        return (
            internal,
            upc,
            internal + 1000,
            name,
            "Brand",
            "Category",
            description,
            "Pack",
            "Manufacturer",
            1.0,
            "Flavor",
            sku,
            "1L",
        )

    return row


@pytest.fixture
def catalogs(spark, product_rows):
    from conf import settings as C

    schema = ",".join(
        f"{c} {'long' if C.EDP_EXPECTED_TYPES[c] == 'int64' else 'double' if C.EDP_EXPECTED_TYPES[c] == 'float64' else 'string'}"
        for c in C.EDP_EXPECTED_COLUMNS
    )
    edp = spark.createDataFrame([product_rows()], schema)
    waters = spark.createDataFrame([product_rows(2, 22, 222)], schema)
    stores = spark.createDataFrame(
        [(1, "Online", "Chain", "Digital")], "ID_STORE long, TYPE string, CADENA string, CANAL string"
    )
    return edp, waters, stores
