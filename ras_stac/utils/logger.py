"""Logging setup and configuration for ras_stac."""

import logging


def setup_logging():
    """Configure logging settings."""
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)

    logging.basicConfig(
        level=logging.INFO,
        format="""{"time": "%(asctime)s" , "level": "%(levelname)s", "message": "%(message)s"}""",
        handlers=[logging.StreamHandler()],
    )
