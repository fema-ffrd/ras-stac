import os


def file_location(fpath: str) -> str:
    """Check if file is local or on s3."""
    if os.path.exists(fpath):
        return "local"
    elif fpath.startswith("s3://"):
        return "s3"
    else:
        raise ValueError(f"Path {fpath} is neither on local machine nor an S3 URL")


def make_thumbnail(gdfs: dict) -> Figure:
    # Figure definition
    cdict = {
        "Banks": "red",
        "Junction": "red",
        "BCLines": "brown",
        "BreakLines": "black",
        "Connections": "cyan",
        "Structure": "black",
        "Mesh": "yellow",
        "River": "blue",
        "StorageAreas": "orange",
        "TwoDAreas": "purple",
        "XS": "green",
    }
    dpi = 300
    thumb_dim = 200  # Took this from the radiant earth browser
    crs = gdfs["River"].crs
    fig, ax = plt.subplots(1, 1, figsize=(thumb_dim / dpi, thumb_dim / dpi))

    # Add data
    for layer in gdfs.keys():
        gdfs[layer].plot(ax=ax, color=cdict[layer], linewidth=1, label=layer)
    try:
        ctx.add_basemap(ax, crs=crs, source=ctx.providers.USGS.USTopo)
    except requests.exceptions.HTTPError as e:
        try:
            ctx.add_basemap(ax, crs=crs, source=ctx.providers.Esri.WorldStreetMap)
        except requests.exceptions.HTTPError as e:
            ctx.add_basemap(ax, crs=crs, source=ctx.providers.OpenStreetMap.Mapnik)

    # Format
    ax.legend()
    ax.set_xticks([])
    ax.set_yticks([])
    fig.tight_layout()
    return fig


def get_huc8(x: float, y: float) -> str:
    """Query USGS ArcGIS service for the HUC-8 a point falls within"""
    huc8 = None
    tries = 0
    while tries < 5:
        resp = requests.get(
            "https://hydro.nationalmap.gov/arcgis/rest/services/wbd/MapServer/4/query?geometry={},{}&geometryType=esriGeometryPoint&inSR=4326&spatialRel=esriSpatialRelIntersects&f=json&outFields=huc8".format(
                x, y
            )
        )
        try:
            huc8 = resp.json()["features"][0]["attributes"]["huc8"]
        except Exception:
            tries += 1
    return huc8
