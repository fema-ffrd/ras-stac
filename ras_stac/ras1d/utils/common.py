import os


def file_location(fpath: str) -> str:
    """Check if file is local or on s3."""
    if os.path.exists(fpath):
        return "local"
    elif fpath.startswith("s3://"):
        return "s3"
    else:
        raise ValueError(f"Path {fpath} is neither on local machine nor an S3 URL")
