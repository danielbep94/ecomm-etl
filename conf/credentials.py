"""Databricks secret retrieval; never log returned connector options."""

import base64

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
SNOWFLAKE_URL = "danonenam.east-us-2.azure.snowflakecomputing.com"
SNOWFLAKE_DATABASE = "PRD_MDP"
SNOWFLAKE_SCHEMA = "MDP_STG"
SNOWFLAKE_WAREHOUSE = "PRD_MDP_ANL_WH"
SNOWFLAKE_ROLE = "PRD_MDP"
SNOWFLAKE_SECRET_SCOPE = "DAN-AM-P-KVT800-R-MDP-DB"
SNOWFLAKE_SECRET_KEYS = {"sfUser": "snowflake-user", "pem_private_key": "Snowflake-Private-Key"}


def get_snowflake_options(dbutils):
    """Return Spark connector options with an unencrypted PKCS8 DER/base64 key.

    Accept a PEM key or the base64 body of a PKCS8 DER key in the secret scope.
    Encrypted private keys require a separate, explicitly configured secret and
    are intentionally rejected rather than attempting an interactive prompt.
    """
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    try:
        user = dbutils.secrets.get(scope=SNOWFLAKE_SECRET_SCOPE, key=SNOWFLAKE_SECRET_KEYS["sfUser"])
        raw = dbutils.secrets.get(scope=SNOWFLAKE_SECRET_SCOPE, key=SNOWFLAKE_SECRET_KEYS["pem_private_key"])
        if not user or not raw:
            raise ValueError("Empty secret")
        key = (
            serialization.load_pem_private_key(raw.encode("utf-8"), password=None)
            if "-----BEGIN" in raw
            else serialization.load_der_private_key(
                base64.b64decode("".join(raw.split()), validate=True), password=None
            )
        )
        if not isinstance(key, rsa.RSAPrivateKey) or key.key_size < 2048:
            raise ValueError("RSA private key of at least 2048 bits required")
        der = key.private_bytes(
            serialization.Encoding.DER, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
        )
    except Exception:
        raise RuntimeError("Snowflake secret retrieval or private-key validation failed.") from None
    return {
        "sfURL": SNOWFLAKE_URL,
        "sfDatabase": SNOWFLAKE_DATABASE,
        "sfSchema": SNOWFLAKE_SCHEMA,
        "sfWarehouse": SNOWFLAKE_WAREHOUSE,
        "sfRole": SNOWFLAKE_ROLE,
        "sfUser": user,
        "pem_private_key": base64.b64encode(der).decode("ascii"),
        "sfTimezone": "UTC",
    }
