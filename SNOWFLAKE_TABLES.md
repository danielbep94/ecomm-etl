# Tablas Snowflake del ETL

El destino es un modelo estrella con **tres catálogos y una fact**. Las tablas
no necesitan existir: el ETL ejecuta `CREATE TABLE IF NOT EXISTS` después de
validar el lote. También puedes ejecutar manualmente
[conf/snowflake_business_tables.sql](conf/snowflake_business_tables.sql) en Snowflake
antes de correr el notebook. El SQL crea solo las cuatro tablas de negocio.
No se han creado objetos en tu cuenta desde este entorno.

## Estructura

Todas pertenecen a `PRD_MDP.MDP_STG`:

| Tabla | Columnas | Grano y clave de join |
| --- | --- | --- |
| `CATALOG_EDP` | 26 | Un producto EDP por snapshot: `RUN_ID, ID_INTERNO`. |
| `CATALOG_WATERS` | 26 | Un producto Waters por snapshot: `RUN_ID, ID_INTERNO`. |
| `CATALOG_STORE` | 16 | Una tienda por snapshot: `RUN_ID, ID_STORE`. |
| `FACT_ECOMMERCE_DANONE` | 16 | Registro fuente de ventas, sin agregación; referencia ambos catálogos por `RUN_ID` y sus IDs. |

Los catálogos conservan todas las columnas de sus CSV. Productos tiene:
`ID_INTERNO, UPC, ID_IMPORT, NOMBRE, MARCA, CATEGORIA, DESCRIPCION, EMPAQUE,
FABRICANTE, FACTOR_CONVERSION, SABOR, SKU, TAMANO`. Tiendas tiene:
`ID_STORE, TYPE, CADENA, CANAL`.

La fact conserva sus seis columnas fuente: `TIME_ID, ID_STORE, ID_INTERNO,
UNIDADES, MONTO, BU`; añade `NORMALIZED_BU, ANIO, MES` y siete columnas de
trazabilidad: `RUN_ID, LOGICAL_DATASET, SOURCE_FILE_NAME, SOURCE_FILE_PATH,
SOURCE_FILE_FINGERPRINT, SOURCE_FILE_YEAR, LOAD_TIMESTAMP_UTC`.

Los catálogos añaden esas mismas siete columnas, ANIO/MES del lote, `CATALOG_BU`
(solo productos) y `IS_ACTIVE, VALID_FROM_UTC, VALID_TO_UTC`. Los IDs son
`NUMBER(19,0)`, los periodos/años `NUMBER(10,0)`, las medidas `DOUBLE`, los textos
`VARCHAR`, la bandera `BOOLEAN` y los timestamps operacionales `TIMESTAMP_NTZ`
con valores UTC. `TIME_ID` mantiene YYYYMM o YYYYMMDD.

Los atributos descriptivos pertenecen a los catálogos en Snowflake. La tabla
curated de Delta conserva su formato enriquecido para la conciliación interna.
No se crean columnas `PRODUCT_*` o `STORE_*` en la fact Snowflake.

## Joins del modelo estrella

```sql
SELECT f.TIME_ID, f.ANIO, f.MES, f.NORMALIZED_BU,
       f.ID_INTERNO, COALESCE(e.NOMBRE, w.NOMBRE) AS PRODUCTO,
       COALESCE(e.MARCA, w.MARCA) AS MARCA,
       f.ID_STORE, s.CADENA, s.CANAL, f.UNIDADES, f.MONTO
FROM PRD_MDP.MDP_STG.FACT_ECOMMERCE_DANONE f
LEFT JOIN PRD_MDP.MDP_STG.CATALOG_EDP e
  ON f.RUN_ID = e.RUN_ID AND f.ID_INTERNO = e.ID_INTERNO
 AND f.NORMALIZED_BU = 'EDP'
LEFT JOIN PRD_MDP.MDP_STG.CATALOG_WATERS w
  ON f.RUN_ID = w.RUN_ID AND f.ID_INTERNO = w.ID_INTERNO
 AND f.NORMALIZED_BU = 'WATERS'
LEFT JOIN PRD_MDP.MDP_STG.CATALOG_STORE s
  ON f.RUN_ID = s.RUN_ID AND f.ID_STORE = s.ID_STORE
WHERE f.ANIO = 2026 AND f.MES = 1;
```

Elige MES según tu archivo. El join usa **RUN_ID + ID**, porque un mismo producto
o tienda puede cambiar entre cargas. No filtres `IS_ACTIVE=true` en joins de
ventas históricas: su snapshot puede estar inactivo y seguir siendo el correcto.
`NORMALIZED_BU` selecciona el catálogo de productos; WATER/PET/BEC/PET_BEC se
normalizan a WATERS. La cobertura y la ausencia de multiplicación se validan en
el ETL y en QA. Las restricciones PK/FK de Snowflake estándar no se presuponen
como controles de integridad; el DDL no las declara.

## Carga y permisos

El ETL copia la historia completa de catálogos Delta a staging y usa MERGE por
RUN_ID + ID para insertar snapshots nuevos y actualizar vigencias de existentes,
sin borrar historia. Crea cuatro staging transitorios con sufijo RUN_ID, comprueba
conteos y registros, y abre una única transacción nativa: tres MERGE, reemplazo
DELETE/INSERT del ANIO/MES de la fact, o de todo ANIO con MES=ALL,, conciliación exacta de las cuatro tablas y
verificación de joins de toda la fact, incluidos meses anteriores. Confirma todo
con COMMIT o intenta ROLLBACK ante un error. Todo DDL ocurre fuera de la transacción.
Al finalizar intenta eliminar los cuatro staging; las fallas quedan en métricas.

Database, schema y warehouse deben existir. El rol necesita USAGE sobre ellos,
CREATE TABLE en el schema, SELECT/INSERT/UPDATE sobre catálogos,
SELECT/INSERT/DELETE sobre fact y permisos para crear/cargar/leer/eliminar staging.
El código valida el esquema existente y rechaza contratos incompatibles antes
del DML; no altera ni reemplaza tablas existentes automáticamente. Una fact
antigua enriquecida requiere una migración revisada antes de usar esta versión.

Delta, Snowflake y DBFS tienen commits independientes. No existe una transacción
distribuida; un fallo posterior al COMMIT requiere el reproceso documentado en
README. Las tablas Snowflake adicionales a la historia Delta fallan conciliación
en lugar de ser borradas.

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

```sql
DESCRIBE TABLE PRD_MDP.MDP_STG.CATALOG_EDP;
DESCRIBE TABLE PRD_MDP.MDP_STG.CATALOG_WATERS;
DESCRIBE TABLE PRD_MDP.MDP_STG.CATALOG_STORE;
DESCRIBE TABLE PRD_MDP.MDP_STG.FACT_ECOMMERCE_DANONE;

SELECT ANIO, MES, NORMALIZED_BU, COUNT(*) AS FILAS, SUM(MONTO) AS MONTO
FROM PRD_MDP.MDP_STG.FACT_ECOMMERCE_DANONE
GROUP BY ANIO, MES, NORMALIZED_BU
ORDER BY ANIO, MES, NORMALIZED_BU;

SELECT RUN_ID, STATUS, PERIOD_START_DATE, PERIOD_END_DATE, ERROR_TYPE, ERROR_MESSAGE
FROM PRD_MDP.MDP_STG.PIPELINE_RUNS
WHERE PIPELINE_NAME = 'ecommerce_danone'
ORDER BY STARTED_AT_UTC DESC
LIMIT 20;
```

Ejecuta `notebooks/job_ecommerce_danone.py` y después
`notebooks/qa_ecommerce_danone.py` con el mismo ANIO/MES. QA incluye
`qa_snowflake_catalog_reconciliation` y `qa_snowflake_star_relationships` además
de los controles existentes; no requiere volver a subir los CSV.
`qa_joined_ecommerce_data` devuelve las ventas Snowflake con los atributos de
productos/tiendas y banderas de coincidencia usando los joins anteriores.
Los catálogos rechazan cualquier duplicado por snapshot, incluso registros
idénticos; las claves de versiones históricas se validan antes del staging.

## Carga de archivo anual

Usa ANIO=2026 y MES=ALL en el notebook de carga y en QA. El SQL del reemplazo
anual usa `WHERE ANIO=2026`; todo ese año se sustituye con los registros del
archivo, incluso si este contiene menos de doce meses. Los meses ausentes del
archivo se eliminan del año reemplazado. Otros años se conservan. Sigue siendo
una sola transacción Snowflake para los tres catálogos y la fact completa.

Las columnas y tipos de las cuatro tablas no cambian. La fact conserva el mes
real derivado de TIME_ID; el campo MES del lote en catálogos/validaciones es NULL
para una carga anual. El tracker registra los límites del año. Los controles de
reproceso consultan periodos solapados para proteger tanto cargas mensuales como
anuales ya exitosas. QA anual une catálogos por RUN_ID, incluso si cada mes se
cargó en un intento diferente.
