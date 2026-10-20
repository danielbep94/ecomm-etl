# Tablas Snowflake del ETL

Este documento refleja el código del repositorio. No se consultó la estructura
real de la cuenta Snowflake. El DDL de la fact se obtuvo del esquema Spark de
`enrich_fact` + `add_operational_metadata`, usando el mismo mapeo de tipos que
`src/load.py`.

## Objetos y creación

| Objeto | Función | Comportamiento |
| --- | --- | --- |
| `PRD_MDP.MDP_STG.FACT_ecommerce_danone` | Fact mensual enriquecida | Se crea si falta, después de validar el lote; reemplaza solo ANIO/MES. |
| `PRD_MDP.MDP_STG.FACT_ecommerce_danone_STAGE_` + UUID sin guiones | Staging transitorio por intento | Mismo esquema que la fact; se crea, se carga y se elimina al finalizar. |
| `PRD_MDP.MDP_STG.PIPELINE_RUNS` | Auditoría compartida | Debe existir. Solo SELECT y append de una fila terminal por intento; sin CREATE, ALTER, UPDATE, MERGE o DELETE. |

Las bases `PRD_MDP`, el schema `MDP_STG`, el warehouse y los permisos deben existir.
Los catálogos EDP/Waters/tiendas y los hallazgos de validación permanecen en Delta
bajo `hive_metastore.ecomm_dph`; no se crean copias separadas de esos catálogos en
Snowflake. Los identificadores SQL sin comillas se resuelven en mayúsculas en
Snowflake, por lo que `FACT_ecommerce_danone` se verá como `FACT_ECOMMERCE_DANONE`.

## DDL de la fact

No hace falta ejecutarlo manualmente: el loader usa este CREATE IF NOT EXISTS.
No altera una tabla ya existente; si su contrato no coincide, debe revisarse
antes de cargar. El código no define PK/UNIQUE ni clustering. La idempotencia se
consigue reemplazando transaccionalmente el periodo, sin agregar o deduplicar la
fact. Las columnas son nullable en el DDL; los campos fuente obligatorios se
validan antes de escribir.

```sql
CREATE TABLE IF NOT EXISTS PRD_MDP.MDP_STG.FACT_ecommerce_danone (
    ID_STORE NUMBER(19,0),
    ID_INTERNO NUMBER(19,0),
    NORMALIZED_BU VARCHAR,
    TIME_ID NUMBER(19,0),
    UNIDADES DOUBLE,
    MONTO DOUBLE,
    BU VARCHAR,
    ANIO NUMBER(10,0),
    MES NUMBER(10,0),
    PRODUCT_UPC NUMBER(19,0),
    PRODUCT_ID_IMPORT NUMBER(19,0),
    PRODUCT_NOMBRE VARCHAR,
    PRODUCT_MARCA VARCHAR,
    PRODUCT_CATEGORIA VARCHAR,
    PRODUCT_DESCRIPCION VARCHAR,
    PRODUCT_EMPAQUE VARCHAR,
    PRODUCT_FABRICANTE VARCHAR,
    PRODUCT_FACTOR_CONVERSION DOUBLE,
    PRODUCT_SABOR VARCHAR,
    PRODUCT_SKU NUMBER(19,0),
    PRODUCT_TAMANO VARCHAR,
    STORE_TYPE VARCHAR,
    STORE_CADENA VARCHAR,
    STORE_CANAL VARCHAR,
    RUN_ID VARCHAR,
    LOGICAL_DATASET VARCHAR,
    SOURCE_FILE_NAME VARCHAR,
    SOURCE_FILE_PATH VARCHAR,
    SOURCE_FILE_FINGERPRINT VARCHAR,
    SOURCE_FILE_YEAR NUMBER(10,0),
    LOAD_TIMESTAMP_UTC TIMESTAMP_NTZ
);
```

Las columnas `PRODUCT_*` vienen del catálogo correspondiente a NORMALIZED_BU;
`STORE_*` vienen del catálogo de tiendas. `TIME_ID` conserva la clave original
YYYYMM o YYYYMMDD. ANIO/MES son el periodo derivado. Los timestamps operacionales
de la fact se almacenan como TIMESTAMP_NTZ y representan UTC. MONTO y UNIDADES
son DOUBLE como exige el esquema fuente; la conciliación usa NUMBER(38,6),
tolerancia monetaria 0.01 y comparación de registros con multiplicidades.

## Contrato del tracker existente

Esta es la estructura esperada, no una instrucción para recrear la tabla.
`CREATED_AT_UTC` se envía explícitamente con el mismo valor de COMPLETED_AT_UTC;
el default CURRENT_TIMESTAMP() de la tabla existente puede mantenerse. La duración
la produce el helper como Python float / Spark DoubleType y Snowflake la convierte
a NUMBER(18,3). Los timestamps del contrato de tracking son TIMESTAMP_TZ.

| Columna | Tipo Snowflake esperado | Nullable |
| --- | --- | --- |
| `RUN_ID` | `VARCHAR` | No |
| `PIPELINE_NAME` | `VARCHAR` | No |
| `PIPELINE_VERSION` | `VARCHAR` | Sí |
| `ENVIRONMENT` | `VARCHAR` | No |
| `STATUS` | `VARCHAR` | No |
| `DATABRICKS_JOB_ID` | `VARCHAR` | Sí |
| `DATABRICKS_JOB_RUN_ID` | `VARCHAR` | Sí |
| `DATABRICKS_TASK_RUN_ID` | `VARCHAR` | Sí |
| `DATABRICKS_TASK_NAME` | `VARCHAR` | Sí |
| `ATTEMPT_NUMBER` | `NUMBER` | Sí |
| `TRIGGER_TYPE` | `VARCHAR` | Sí |
| `SOURCE_NAME` | `VARCHAR` | Sí |
| `SOURCE_FILE` | `VARCHAR` | Sí |
| `TARGET_NAME` | `VARCHAR` | Sí |
| `PERIOD_START_DATE` | `DATE` | Sí |
| `PERIOD_END_DATE` | `DATE` | Sí |
| `STARTED_AT_UTC` | `TIMESTAMP_TZ` | No |
| `COMPLETED_AT_UTC` | `TIMESTAMP_TZ` | No |
| `DURATION_SECONDS` | `NUMBER(18,3)` | No |
| `SOURCE_ROWS` | `NUMBER` | Sí |
| `STAGING_ROWS` | `NUMBER` | Sí |
| `TRANSFORMED_ROWS` | `NUMBER` | Sí |
| `TARGET_ROWS_BEFORE` | `NUMBER` | Sí |
| `TARGET_ROWS_AFTER` | `NUMBER` | Sí |
| `TARGET_ROW_DELTA` | `NUMBER` | Sí |
| `BUSINESS_METRICS_JSON` | `VARCHAR` | Sí |
| `ERROR_TYPE` | `VARCHAR` | Sí |
| `ERROR_MESSAGE` | `VARCHAR` | Sí |
| `CREATED_AT_UTC` | `TIMESTAMP_TZ` | No |

Los estados terminales son SUCCEEDED, VALIDATION_FAILED y FAILED. BUSINESS_METRICS_JSON
es VARCHAR con JSON compacto, no una tabla adicional. Cada intento tiene un RUN_ID
nuevo. El código no impone una restricción única en Snowflake ni reintenta un
append con resultado ambiguo.

## Verificación en Snowflake

Ejecuta estas consultas de lectura. La fact solo existirá después de llegar a la
fase Snowflake de una primera carga válida; el tracker debe existir desde antes.

```sql
DESCRIBE TABLE PRD_MDP.MDP_STG.PIPELINE_RUNS;
DESCRIBE TABLE PRD_MDP.MDP_STG.FACT_ecommerce_danone;

SELECT ANIO, MES, NORMALIZED_BU, COUNT(*) AS FILAS, SUM(MONTO) AS MONTO
FROM PRD_MDP.MDP_STG.FACT_ecommerce_danone
WHERE ANIO = 2026
GROUP BY ANIO, MES, NORMALIZED_BU
ORDER BY ANIO, MES, NORMALIZED_BU;

SELECT RUN_ID, STATUS, PERIOD_START_DATE, PERIOD_END_DATE,
       SOURCE_ROWS, STAGING_ROWS, TRANSFORMED_ROWS,
       TARGET_ROWS_BEFORE, TARGET_ROWS_AFTER, ERROR_TYPE, ERROR_MESSAGE
FROM PRD_MDP.MDP_STG.PIPELINE_RUNS
WHERE PIPELINE_NAME = 'ecommerce_danone'
ORDER BY STARTED_AT_UTC DESC
LIMIT 20;
```

El reemplazo mensual usa staging + BEGIN + DELETE del periodo + INSERT +
conciliación + COMMIT, en una sola conexión nativa Snowflake. Si falla dentro de
la transacción intenta ROLLBACK. No hay una transacción global entre Snowflake,
Delta y el movimiento de archivos DBFS; consulta README para reintentos.
