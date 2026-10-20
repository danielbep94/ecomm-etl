# E-commerce — Databricks / Snowflake

ETL mensual en Python y PySpark para cuatro CSV de e-commerce. Valida el lote
completo, conserva snapshots de catálogos, reemplaza un solo periodo en Delta y
Snowflake, registra el resultado terminal y permite consultar QA sin los archivos
originales. El código de producción está en los ocho módulos solicitados.

## Flujo de e-commerce

El job valida primero los cuatro CSV completos: columnas y tipos, claves
obligatorias, duplicados, periodos, BU, cobertura y conservación de filas en los
joins. Un error bloquea todas las escrituras de negocio. Los catálogos no aceptan
duplicados: `ID_INTERNO`, `UPC`, `ID_IMPORT` y `SKU` deben ser únicos dentro de cada
catálogo de productos por snapshot; `ID_STORE` debe ser único en tiendas. Tanto
copias idénticas como atributos conflictivos hacen fallar el lote; no se eliminan
duplicados automáticamente ni se permite bajar su severidad a advertencia.

Tras pasar la validación, se persisten las tablas Delta y se concilia staging
Snowflake. Los tres MERGE de catálogos y el reemplazo mensual de la fact se
confirman en **una transacción Snowflake** solamente si las conciliaciones y los
joins pasan. Un fallo dentro de esa transacción revierte los cuatro cambios.
Los commits de Delta, la creación de tablas, DBFS y el tracker son independientes.
Las advertencias de montos/unidades negativos se conservan como ajustes válidos.

El notebook QA cruza la fact ya cargada en Snowflake con productos y tiendas de
la misma versión y devuelve **`qa_joined_ecommerce_data`**, además de los controles.
No depende de los CSV ni ejecuta una nueva carga.

## Probar con los archivos de tu captura

1. Clona o actualiza la rama `main` en Databricks e instala el proyecto con
   `%pip install .` desde la raíz de la Git folder. Revisa los requisitos de
   cómputo/conectores y configura las credenciales descritas abajo.
2. Conserva los cuatro CSV de la captura en
   `dbfs:/FileStore/tables/ecomm_dph/incoming`.
3. Opcionalmente ejecuta [el SQL de creación](conf/snowflake_business_tables.sql)
   en Snowflake. El ETL también crea las cuatro tablas si faltan.
4. Abre `notebooks/job_ecommerce_danone.py`, configura `ANIO=2026`, `MES` con el
   mes que quieres cargar (1–12, presente en el archivo) y
   `FORCE_REPROCESS=false`, y ejecuta todas las celdas. El archivo anual se
   valida completo, pero solo se carga el mes seleccionado.
5. Ejecuta `notebooks/qa_ecommerce_danone.py` con el mismo ANIO/MES y
   `FAIL_ON_QA_ERROR=true`. Comprueba `qa_final_status` y los resultados de
   conciliación y joins. Un reproceso de un mes ya exitoso requiere volver a
   depositar los cuatro CSV y usar `FORCE_REPROCESS=true`.

## Suposiciones y alcance

- El esquema Hive `ecomm_dph` y su inicialización ya existen. El ETL **no crea ni
  modifica el esquema** ni sus propiedades.
- Los CSV son UTF-8, separados por coma, con cabecera, comillas dobles y comillas
  escapadas duplicándolas. Cada registro ocupa una línea física; los registros
  multilínea, vacíos, mal entrecomillados o con ancho incorrecto fallan validación.
- Los identificadores numéricos usan `LongType`, las medidas `DoubleType` y los
  objetos `StringType`. No hay inferencia de esquema. Se aceptan representaciones
  enteras exactas como `1`, `1.0` y `1e3`, sin redondear identificadores.
- `TIME_ID` usa **un solo formato por archivo**: `YYYYMM` o `YYYYMMDD`. Se
  comprueban las fechas reales y todos los años del archivo anual.
- `src/run_tracking.py` usa el tracker reutilizable adjunto por el usuario,
  conservando sus funciones, firmas, normalización de conteos, duración Double
  y `CREATED_AT_UTC`. Se mantienen los controles de integración: redacción de
  secretos, estados terminales y verificación de la tabla existente antes del
  append. No existe un segundo módulo `def_tracking.py`.
- Un único productor deposita el lote y no modifica `incoming` mientras corre el
  job. Configura **`max_concurrent_runs = 1` para todo el pipeline**, no por mes.
  Ningún otro job debe escribir estos destinos simultáneamente. Las huellas y la
  metadata detectan modificaciones de archivos durante el procesamiento.
- Un lote con errores de validación no modifica tablas de negocio. Delta realiza
  commits independientes por tabla; Snowflake realiza su propio commit mensual.
  **No existe una transacción distribuida entre Delta, Snowflake y DBFS.** Una
  falla técnica posterior puede dejar cambios ya confirmados; el reintento del
  periodo los sustituye de forma idempotente.
- Las sumas monetarias de conciliación usan `DECIMAL/NUMBER(38,6)`, manteniendo
  las columnas fuente en `DoubleType`. La tolerancia es 0.01. Además se compara
  el multiconjunto completo de registros, incluyendo multiplicidades, para no
  ocultar diferencias con el mismo conteo o total.

## Árbol del repositorio

```text
ecomm-etl/                         # raíz del proyecto ecommerce_danone
├── conf/
│   ├── credentials.py
│   ├── settings.py
│   └── snowflake_business_tables.sql
├── logs/
│   └── logger.py
├── src/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   ├── run_tracking.py
│   └── main.py
├── notebooks/
│   ├── job_ecommerce_danone.py
│   └── qa_ecommerce_danone.py
├── tests/                         # opcional, no requerido por el ETL
│   ├── conftest.py
│   ├── test_extract.py
│   ├── test_transform.py
│   ├── test_load.py
│   ├── test_run_tracking.py
│   ├── test_main.py
│   └── test_notebooks.py
├── .gitignore
├── pyproject.toml
└── README.md
```

`conf`, `logs` y `src` son paquetes namespace de Python; no se requieren archivos
`__init__.py`. Los notebooks son puntos de entrada a las funciones existentes;
la lógica ETL y QA permanece en `src/`. No se añaden frameworks de configuración,
schemas ni recursos adicionales.

## Requisitos en Databricks

1. Clona `https://github.com/danielbep94/ecomm-etl.git`, rama `main`, en una Git folder.
2. Usa cómputo clásico con APIs JVM/RDD, Hive Metastore y DBFS root/FileStore
   habilitados. Databricks Runtime 16.4 LTS o una versión compatible con Spark
   3.5/4.0 y Python >=3.10 es la base prevista. Serverless, Spark Connect y los
   entornos que bloquean DBFS root no cumplen este contrato de almacenamiento.
3. Instala este paquete en el entorno del job. Desde una sesión cuyo directorio
   actual sea la raíz de la Git folder:

   ```python
   %pip install .
   ```

   Reinicia Python si Databricks lo solicita. Las dependencias de producción son
   `snowflake-connector-python` y `cryptography`. No instales PySpark ni Delta
   desde PyPI en el cluster: ambos los proporciona Databricks Runtime. En jobs
   también puedes instalar el wheel de este proyecto como librería.
4. El conector **Spark de Snowflake** `net.snowflake.spark.snowflake` debe estar
   disponible en el Runtime. El conector Python no reemplaza ese JAR. Si tu
   Runtime no lo incluye, instala las versiones oficiales del conector Spark y
   JDBC compatibles con sus versiones de Spark y Scala. Comprueba la clase:

   ```python
   spark._jvm.Thread.currentThread().getContextClassLoader().loadClass(
       "net.snowflake.spark.snowflake.DefaultSource"
   )
   ```
5. El principal del job necesita leer/escribir los dos directorios DBFS y las
   tablas Hive. Snowflake necesita acceso de red HTTPS al account configurado y
   al almacenamiento interno usado por su conector, permisos sobre el warehouse,
   SELECT/INSERT/DELETE sobre la fact, SELECT/INSERT/UPDATE sobre los tres
   catálogos, CREATE TABLE para los destinos y las
   tablas transitorias por intento, y SELECT/INSERT sobre el tracker existente.

No se han ejecutado llamadas a tus servicios remotos desde este repositorio.
Las pruebas locales no certifican permisos, conectores ni datos de producción.

## Definición de tablas Snowflake

El modelo estrella consta de `CATALOG_EDP`, `CATALOG_WATERS`, `CATALOG_STORE` y
`FACT_ECOMMERCE_DANONE`, bajo `PRD_MDP.MDP_STG`. La fact contiene claves y medidas;
los catálogos conservan las columnas originales y la historia por RUN_ID.
Consulta [SNOWFLAKE_TABLES.md](SNOWFLAKE_TABLES.md) para estructuras y joins.

Como las tablas aún no existen, puedes ejecutar
[conf/snowflake_business_tables.sql](conf/snowflake_business_tables.sql) en Snowflake
o dejar que el ETL las cree automáticamente en la primera carga válida. Database,
schema y la tabla compartida `PIPELINE_RUNS` deben existir previamente.

## Configuración y credenciales

`conf/settings.py` centraliza los nombres y las severidades. Los valores del
requerimiento apuntan a `PROD` / `PRD_MDP`; verifica los destinos que usarás para
las pruebas antes de lanzar el pipeline.

| Elemento | Valor |
| --- | --- |
| Hive schema | `ecomm_dph` |
| Incoming | `dbfs:/FileStore/tables/ecomm_dph/incoming` |
| Current | `dbfs:/FileStore/tables/ecomm_dph/current` |
| Snowflake destino | `PRD_MDP.MDP_STG.FACT_ecommerce_danone` |
| Tracking existente | `PRD_MDP.MDP_STG.PIPELINE_RUNS` |
| Snowflake host | `danonenam.east-us-2.azure.snowflakecomputing.com` |
| Database / Schema | `PRD_MDP` / `MDP_STG` |
| Warehouse / Role | `PRD_MDP_ANL_WH` / `PRD_MDP` |

El scope Databricks `DAN-AM-P-KVT800-R-MDP-DB` debe contener las claves
`snowflake-user` y `Snowflake-Private-Key`. La clave privada debe ser RSA, sin
cifrar, en formato PEM o PKCS8 DER codificado en base64. No se incluyen valores
secretos en el repositorio. No se imprimen opciones del conector ni errores que
contengan su contenido.

La tabla `PIPELINE_RUNS` debe existir con la estructura suministrada. El código
la consulta y solo hace append de una fila terminal por intento; no ejecuta DDL,
MERGE, UPDATE ni DELETE sobre ella. No hay reintentos automáticos del append,
pues una respuesta de red fallida puede tener un commit ambiguo. El tracker
adjunto incluye explícitamente `CREATED_AT_UTC=COMPLETED_AT_UTC` y escribe con
correspondencia de columnas por nombre.

## Entrada: exactamente cuatro archivos

Deposita únicamente el lote completo en `incoming`:

```text
TBL_BI_BQ_FACTS_ECOMM_ONE_DANONE_FULL_2025.csv
2004_TBL_BI_BQ_CAT_ECOMM_PRODUCTS_EDP.csv
2006_TBL_BI_BQ_CAT_ECOMM_PRODUCTS_PET_BEC.csv
TBL_BI_BQ_CAT_ECOMM_POS.csv
```

La comparación de nombres y extensiones no distingue mayúsculas; los nombres
físicos originales se conservan. Para EDP también se admite el nombre anterior
`edp_2004_TBL_BI_BQ_CAT_ECOMM_PRODUCTS_EDP.csv`; envía solo una de las dos variantes.
`2004` y `2006` son parte del nombre, no del periodo de proceso. Se ignoran directorios y archivos de sistema/temporales.
Cualquier archivo de negocio adicional o inesperado invalida el lote. No se
asignan datasets por posición.

Las cabeceras deben coincidir con las listas de `conf/settings.py`. Solo se
normalizan BOM inicial y espacios exteriores; no se cambian nombres desconocidos.
Por defecto, un orden diferente genera WARNING y columnas faltantes, adicionales,
vacías o duplicadas generan ERROR.

### Esquemas fuente

- EDP y Waters, lógicamente separados: `ID_INTERNO, UPC, ID_IMPORT, NOMBRE, MARCA,
  CATEGORIA, DESCRIPCION, EMPAQUE, FABRICANTE, FACTOR_CONVERSION, SABOR, SKU, TAMANO`.
  Los identificadores según el mapa son Long; `FACTOR_CONVERSION`
  es Double; las demás columnas son String. En concreto los Long son
  `ID_INTERNO`, `UPC`, `ID_IMPORT`, `SKU`.
- Tiendas: `ID_STORE` (Long), `TYPE`, `CADENA`, `CANAL` (String).
- Fact anual: `TIME_ID, ID_STORE, ID_INTERNO` (Long), `UNIDADES, MONTO` (Double),
  `BU` (String).

Se distinguen faltantes de valores numéricos mal formados; se rechazan fracciones
en IDs, desbordamientos de Long, NaN e infinito. Los IDs son no negativos y el
factor debe ser positivo. Los valores obligatorios vacíos, `NULL`, `NONE`, `N/A`
y `NA` fallan. BU se normaliza con el mapa del requerimiento.

## Usar el lote anual 2026 mostrado en Databricks

Sincroniza `main` antes de ejecutar. Se admite
`2004_TBL_BI_BQ_CAT_ECOMM_PRODUCTS_EDP.csv` sin prefijo `edp_`, conservando ese nombre.
Con los cuatro CSV en `dbfs:/FileStore/tables/ecomm_dph/incoming`, abre
`notebooks/job_ecommerce_danone.py` y configura ANIO=2026, MES=un mes que exista
en el archivo anual, FORCE_REPROCESS=false. Por ejemplo, MES=7 procesa julio
solo si hay registros de julio. El job no carga todos los meses a la vez.

Antes de empezar conserva tus originales: en un fallo se elimina el lote enviado
de `incoming`; en éxito se mueve a `current`. Para procesar otro mes o reintentar,
vuelve a depositar los cuatro CSV. FORCE_REPROCESS=true se utiliza para sustituir
un periodo que ya terminó correctamente.

Después abre `notebooks/qa_ecommerce_danone.py` con el mismo ANIO/MES y
FAIL_ON_QA_ERROR=true, dejando inicialmente vacíos los filtros opcionales. Consulta
[SNOWFLAKE_TABLES.md](SNOWFLAKE_TABLES.md) para el DDL y las consultas de comprobación.

## Ejecución interactiva

Desde Databricks, con la raíz del repo disponible para imports:

```python
from src.main import run_pipeline

record = run_pipeline(
    anio=2025,
    mes=7,
    force_reprocess=False,
    spark=spark,
    dbutils=dbutils,
)
print(record["RUN_ID"], record["STATUS"])
```

Si prefieres widgets:

```python
from src.main import run_from_widgets

run_from_widgets(spark=spark, dbutils=dbutils)
```

La primera llamada crea los widgets `ANIO`, `MES` y `FORCE_REPROCESS` si no existen.
Completa los valores antes de volver a ejecutarla. Los parámetros ya proporcionados
por el job se conservan. Solo `true` / `false` se convierten a Boolean, evitando
que el texto `"false"` se interprete como verdadero.

## Notebooks de Databricks

Los archivos `.py` de `notebooks/` tienen el encabezado de notebook source de
Databricks y se abren como notebooks al sincronizar la Git folder. Detectan la
raíz del clon para importar `src.main`; no duplican la lógica ni exponen opciones
Snowflake o secretos. Configura las librerías del proyecto en el cómputo antes
de ejecutarlos, como se indica arriba.

### Notebook del job

Abre `notebooks/job_ecommerce_danone.py`. Completa los widgets antes de ejecutar
todas las celdas:

```json
{"ANIO":"2025","MES":"7","FORCE_REPROCESS":"false"}
```

El notebook valida los parámetros antes de iniciar el pipeline. Los valores
vacíos o inválidos hacen fallar esa validación sin iniciar discovery ni tocar
archivos. Invoca `run_pipeline` una sola vez, propaga los errores y muestra RUN_ID,
estado y conteos de filas. No recrea el schema ni instala dependencias.

### Notebook de QA

Abre `notebooks/qa_ecommerce_danone.py`. Parámetros mínimos:

```json
{"ANIO":"2025","MES":"7","FAIL_ON_QA_ERROR":"true"}
```

Widgets opcionales, vacíos por defecto: `RUN_ID`, `LOGICAL_DATASET`, `BU`,
`ID_INTERNO` e `ID_STORE`. Los filtros se pasan a `run_qa_validation` y se muestran
sus diecisiete DataFrames, empezando por los datos cruzados e incluyendo el estado
final. Con `FAIL_ON_QA_ERROR=true`, un
estado `FAIL` hace fallar la tarea **después** de mostrar todos los resultados.
`PASS_WITH_WARNINGS` no falla. Para inspección interactiva puedes ponerlo en
`false`. El notebook QA no llama al ETL ni modifica tablas o archivos.

## Ejecución como job

Crea una tarea de tipo **Notebook** con origen Git, repositorio
`https://github.com/danielbep94/ecomm-etl.git`, rama `main`, y selecciona el notebook
`notebooks/job_ecommerce_danone.py`. Usa el diccionario de parámetros anterior.
Mantén **`max_concurrent_runs = 1`** para el job completo.

Puedes añadir una segunda tarea Notebook con `notebooks/qa_ecommerce_danone.py`,
dependiente de que la tarea ETL termine correctamente, y los mismos ANIO/MES.
Deja RUN_ID vacío para revisar el snapshot persistido actual del periodo; puedes
copiar un RUN_ID de la salida del ETL para una inspección específica.

La entrada Python script `src/main.py` sigue disponible. Sus argumentos son:

```json
["--anio", "2025", "--mes", "7", "--force-reprocess", "false"]
```

```bash
python src/main.py --anio 2025 --mes 7 --force-reprocess false
```

Cada intento ETL genera un solo RUN_ID antes de descubrir archivos. Se leen los
identificadores de job/task cuando existen; la ejecución interactiva usa valores
nulos, intento 0 y trigger `INTERACTIVE`.

## Validación y carga

1. Comprueba el schema existente y crea únicamente `incoming` o `current` si
   faltan. No reejecuta la inicialización ni modifica DBPROPERTIES.
2. Registra el manifiesto con tamaño, modificación, año, patrón y SHA-256.
3. Consulta el historial para clasificar FIRST_LOAD, RETRY o REPROCESS.
4. Lee todos los fuentes disponibles como strings y recoge sus hallazgos antes
   de permitir cualquier escritura de negocio.
5. Valida duplicados de todas las claves solicitadas, colisiones entre catálogos,
   año/formato de TIME_ID, BU, cobertura de productos/tiendas y multiplicación de
   joins. No elimina ni agrega registros duplicados.
6. Por defecto, duplicados y colisiones generan ERROR; montos/unidades negativos
   generan WARNING y se conservan como devoluciones/ajustes.
7. Persiste resultados PASS/WARNING/FAIL y detalles distribuidos de claves en
   `ecomm_dph.ecommerce_validation_results`. Los logs muestran reglas y conteos,
   no registros completos. Las muestras detalladas quedan solo en Delta.
8. Después de validar todo, activa snapshots de catálogos con un MERGE Delta por
   catálogo. El mismo commit desactiva el snapshot anterior e inserta el nuevo;
   se preserva historia con RUN_ID, IS_ACTIVE y vigencias UTC.
9. Escribe staging y curated solo para ANIO/MES mediante `replaceWhere`. Curated
   conserva las columnas fuente, NORMALIZED_BU, atributos `PRODUCT_*`, atributos
   `STORE_*` y metadata operacional. Se conserva el grano de la fact fuente.
10. Concilia filas, MONTO y registros entre fuente transformada y Delta.
11. Crea las cuatro tablas Snowflake si faltan y cuatro staging transitorios
    con sufijo RUN_ID. Proyecta la fact a sus 16 columnas sin descripciones y
    copia la historia completa de catálogos desde Delta. Después de conciliar
    staging, en **una misma conexión Python** ejecuta BEGIN, tres MERGE por
    RUN_ID + ID, DELETE/INSERT del periodo de la fact, conciliación de las cuatro
    tablas, validación de joins históricos y COMMIT. Ante error intenta ROLLBACK.
    El DDL ocurre fuera de la transacción. No se borra historia de catálogos.
12. Comprueba también el resultado ya confirmado desde Spark. Solo entonces
    sustituye archivos en `current`, verifica el movimiento y el vacío del lote
    original en `incoming`, y hace un único append terminal al tracker.

Las tablas de staging Snowflake se eliminan al terminar. Si falla su eliminación,
`staging_cleanup[dataset]=CLEANUP_REQUIRED` y `remaining_staging_tables`
conservan el estado y los nombres para limpieza operativa. Cada nombre incluye
un UUID y nunca es el tracker.

## Fallos, reintentos y reproceso

- Un ERROR de validación produce `VALIDATION_FAILED`, persiste los hallazgos,
  elimina los archivos descubiertos del lote de `incoming` y eleva una excepción.
- Una falla técnica produce `FAILED`, conserva métricas disponibles, intenta
  eliminar el lote y vuelve a elevar la excepción original. Fallos de limpieza
  quedan explícitos en las métricas; nunca se ocultan como un borrado exitoso.
- No existen `archive` ni `rejected`. No se mueven archivos fallidos a `current`.
  Conserva una copia externa de tus CSV para poder volver a enviarlos.
- Vuelve a subir los **cuatro** archivos antes de reintentar. Si solo hay intentos
  fallidos y ningún éxito previo para el periodo, se clasifica RETRY y no se
  bloquea por una huella ya vista. Cada intento tiene un RUN_ID nuevo.
- Si ya hubo un éxito, `FORCE_REPROCESS=false` falla antes de cargar negocio con
  `DUPLICATE_PERIOD_VALIDATION_ERROR`; el lote enviado también se elimina.
- Para reemplazar conscientemente un periodo exitoso, vuelve a enviar los cuatro
  CSV y usa `force_reprocess=True`. Ningún otro periodo debe cambiar.
- Una falla de tracking después del éxito de negocio hace fallar el job, aunque
  los destinos y los archivos ya se hayan actualizado. Comprueba ese RUN_ID en
  Snowflake antes de repetir un append: un error de red no demuestra ausencia
  de commit. El tracker nunca se reintenta automáticamente.
- La caída de una máquina entre commits o antes del bloque de limpieza puede
  impedir el borrado o la fila terminal. Las APIs no permiten garantizar esos
  efectos cuando el proceso o el servicio no están disponibles. Inspecciona el
  RUN_ID y los destinos, corrige la causa y reenvía el lote completo.

## QA sobre datos persistidos

No necesita archivos en `incoming`; usa staging del periodo, snapshots históricos
identificados por RUN_ID, curated, validaciones Delta e historial/destino Snowflake.

```python
from src.main import run_qa_validation

qa = run_qa_validation(2025, 7, spark=spark, dbutils=dbutils)
for name, dataframe in qa.items():
    print(name)
    display(dataframe)
```

Filtros opcionales:

```python
qa = run_qa_validation(
    2025, 7,
    logical_dataset="ECOMMERCE_FACT",
    bu="EDP",
    id_interno=123,
    id_store=456,
    spark=spark,
    dbutils=dbutils,
)
```

También admite `run_id`. Los resultados son `qa_pipeline_history`,
`qa_validation_results`, `qa_period_detection`, `qa_catalog_duplicate_summary`,
`qa_catalog_duplicate_detail`, `qa_cross_catalog_collisions`,
`qa_product_catalog_coverage`, `qa_store_catalog_coverage`,
`qa_fact_duplicate_summary`, `qa_negative_values`, `qa_join_multiplication_check`,
`qa_source_delta_reconciliation`, `qa_delta_snowflake_reconciliation`,
`qa_snowflake_catalog_reconciliation`, `qa_snowflake_star_relationships`,
`qa_joined_ecommerce_data` y
`qa_final_status` (`PASS`, `PASS_WITH_WARNINGS` o `FAIL`). QA es de solo lectura.

Para consultar los datos cruzados desde Python:

```python
display(qa["qa_joined_ecommerce_data"])
```

El resultado conserva las columnas de la fact y añade `PRODUCT_*`, `STORE_*`,
`PRODUCT_MATCHED` y `STORE_MATCHED`. El join de productos usa
`RUN_ID + ID_INTERNO + NORMALIZED_BU`; el de tiendas usa `RUN_ID + ID_STORE`.
Conserva las ventas sin coincidencia, con atributos nulos y bandera falsa, para
poder investigarlas. QA también detecta filas multiplicadas por claves duplicadas.
Una misma clave en distintos RUN_ID representa versiones históricas y no es un
duplicado dentro de un snapshot. Los filtros de periodo, BU y IDs también se
aplican al resultado cruzado. El notebook muestra todos los resultados antes de
fallar la tarea cuando QA devuelve FAIL.

Staging y curated conservan la última versión de cada mes. Tras un reproceso,
consultar QA de un RUN_ID reemplazado no reconstruye su antigua fact: mostrará
FAIL si ya no existe esa versión. Catálogos, hallazgos e historial sí conservan
la auditoría por intento. La detección anual original se consulta en el JSON del
tracker; la detección de QA corresponde a los registros mensuales persistidos.

## Pruebas locales opcionales

No son necesarias para ejecutar el ETL en Databricks:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
pytest -q
ruff check conf logs src notebooks tests
```

Requieren Java compatible con el Spark instalado. Las pruebas cubren descubrimiento,
CSV estricto, huellas, cabeceras, tipos, faltantes, duplicados, colisiones, fechas,
BU, cobertura, multiplicación, devoluciones, conciliación exacta, aislamiento del
periodo, flujo de reintento/reproceso, limpieza y tracking append-only.
Las transacciones Snowflake se prueban con un doble de conexión; no ejecutan SQL
contra la cuenta real. La validación integral de conectores, permisos y servicios
corresponde a la prueba posterior en tu workspace Databricks.

Las tres pruebas de integración con escritura real en Delta son opcionales: configura
`ECOMM_TEST_DELTA_JARS` con las rutas de los JARs oficiales de Delta Spark y Delta
Storage, separadas por coma, e instala el paquete Python `delta-spark` compatible.
Sin esa variable se omiten explícitamente esas tres pruebas. En la validación de
este desarrollo se ejecutaron con Spark 4.0.4, Delta 4.0.1, Python 3.12 y Java 21;
los JARs se descargaron de Maven Central y se verificaron sus digests publicados.

### Validación realizada

La suite completa terminó con **97 pruebas aprobadas, 0 fallos y 0 omitidas**.
Las pruebas locales usan Python 3.12, Spark 4.0.4 y Delta Lake 4.0.1 e incluyen
un lote completo de cuatro CSV con cargas Delta y movimientos locales de archivos
reales. La suite también comprueba creación de las cuatro tablas, rechazo de
esquemas incompatibles, rollback de cambios en catálogos/fact, joins históricos
sin multiplicación y los dos notebooks. Snowflake se ejercita con dobles de
conexión; la ejecución integral en tu Databricks/Snowflake está pendiente.
