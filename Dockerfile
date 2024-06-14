FROM ghcr.io/osgeo/gdal:ubuntu-small-3.8.5

RUN apt-get update && \
    apt-get install jq -y && \
    apt-get install -y python3-pip && \
    pip3 install rasterio --no-binary rasterio

COPY requirements.txt .
RUN pip3 install -r requirements.txt

WORKDIR /plugins

# Copy plugin utils
COPY ras_stac/utils ras_stac/utils

# Copy plugin functions
COPY ras_stac/ras_geom_hdf.py ras_stac/
COPY ras_stac/ras_plan_hdf.py ras_stac/
COPY ras_stac/ras_plan_dg.py ras_stac/

COPY tests tests
