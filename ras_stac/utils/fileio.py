from pathlib import Path

from ras_stac.utils.s3_utils import list_keys


def ls(directory: str | Path) -> list[Path]:
    """List files in a directory."""
    match file_location(directory):
        case "local":
            return [f for f in directory.iterdir() if f.is_file()]
        case "s3":
            return list_keys(directory)


def file_location(file: str | Path) -> str:
    """Return the location of a file."""
    file = Path(file)
    if file.is_file() or file.is_dir():
        return "local"
    elif str(file).startswith("s3:"):
        return "s3"
    else:
        return "unknown"
