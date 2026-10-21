# Pruebas y mantenimiento manual

Este directorio contiene la suite local y las herramientas de reset de tablas de
prueba. No se incluye en el wheel de producción y el ETL no lo importa.

## Ejecutar la suite

Desde la raíz del repositorio, instala el proyecto con sus dependencias de
desarrollo y ejecuta `pytest -q`. Las pruebas de Snowflake usan dobles de conexión
compartidos en `snowflake_fakes.py`; no necesitan credenciales ni acceden a tu cuenta.

Para incluir las escrituras reales de Delta, instala una versión de `delta-spark`
compatible con tu Spark y define `ECOMM_TEST_DELTA_JARS` con las rutas a los JARs de
Delta Spark y Delta Storage, separadas por coma. Sin esa variable, pytest omite
esos casos de integración. Los casos locales crean tablas temporales de prueba y
las eliminan al finalizar.

## Reset de las tablas de prueba configuradas

En Databricks, abre `tests/reset_ecommerce_test_tables.py` desde la Git folder
completa. El notebook comparte `notebooks/_bootstrap.py` mediante `%run` y llama
a `reset_business_tables.py`, que reutiliza los esquemas y validaciones del loader.
Los tests automáticos nunca ejecutan este notebook contra tu Snowflake real.

Ejecutarlo manualmente **vacía todas las filas de los cinco destinos Delta y los
cuatro destinos Snowflake configurados, incluidos todos los años y snapshots**.
Estos destinos fueron identificados por el usuario como tablas de prueba. Detén
los jobs y otros escritores antes de usarlo. Conserva PIPELINE_RUNS,
ecommerce_validation_results y los archivos de incoming/current. El reset no es
atómico entre tablas o sistemas; revisa TABLE_NAME/COMPLETED_TABLES si falla.

El notebook muestra primero los nueve destinos y después verifica ROWS_AFTER=0.
Necesita las dependencias Python en su propio entorno y los permisos documentados
en el README principal. Se usa para preparar pruebas, no para añadir años ni para
reprocesar un mes: esas cargas se hacen con el job habitual y FORCE_REPROCESS.
