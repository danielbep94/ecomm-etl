"""The pipeline's sole structured logging configuration."""

import logging
import sys

from src.run_tracking import sanitize_error_message


class BusinessLogger(logging.LoggerAdapter):
    def event(self, stage, status, dataset=None, source_file=None, **fields):
        context = {
            **self.extra,
            "STAGE": stage,
            "LOGICAL_DATASET": dataset,
            "SOURCE_FILE_NAME": source_file,
            "STATUS": status,
            **fields,
        }
        message = " | ".join(
            f"{key}={sanitize_error_message(str(value))}" for key, value in context.items() if value is not None
        )
        self.info(message)


def get_logger(run_id, anio=None, mes=None):
    logger = logging.getLogger("ecommerce_danone")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)sZ | %(levelname)s | %(message)s"))
        import time

        handler.formatter.converter = time.gmtime
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return BusinessLogger(logger, {"RUN_ID": run_id, "ANIO": anio, "MES": mes})
