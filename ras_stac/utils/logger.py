import logging


def setup_logging():
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)

    logging.basicConfig(
        level=logging.INFO,
        format="""{"time": "%(asctime)s" , "level": "%(levelname)s", "message": "%(message)s"}""",
        handlers=[logging.StreamHandler()],
    )
