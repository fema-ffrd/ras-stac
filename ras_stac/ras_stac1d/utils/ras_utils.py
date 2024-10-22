def prj_is_ras(prj_contents: str):
    """Verify if prj is from hec-ras model."""
    if "Proj Title" in prj_contents.split("\n")[0]:
        return True
    else:
        return False
