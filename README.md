# ecomm-etl

Base de un proyecto ETL en Python para Databricks.

## Empezar en Databricks

1. Crea una carpeta Git (Git folder) en tu workspace.
2. Clona `https://github.com/danielbep94/ecomm-etl.git` y selecciona la rama `main`.
3. Abre `notebooks/00_inicio.py` y ejecútalo con un recurso de cómputo de Databricks.

El notebook muestra las versiones de Python y Spark y un DataFrame con cinco filas para comprobar la ejecución. No necesita credenciales de servicios externos ni escribe datos.

## Estructura

- `notebooks/`: notebooks de Databricks.
- `src/ecomm_etl/`: código Python reutilizable.
- `pyproject.toml`: metadatos del paquete Python.

Spark lo proporciona Databricks Runtime. Los orígenes de datos, las transformaciones y los destinos del ETL se definirán cuando se concrete el pipeline.
