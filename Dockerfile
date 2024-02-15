FROM ghcr.io/osgeo/gdal:ubuntu-small-3.8.0

RUN apt-get update && \
    apt-get install -y python3-pip && \
    pip3 install rasterio --no-binary rasterio

COPY requirements.txt .
RUN pip3 install -r requirements.txt

WORKDIR /plugins

# Copy plugin utils
COPY ras-stac/utils utils

# Copy plugin functions
COPY ras-stac/ras_geom_hdf.py .
COPY ras-stac/ras_plan_hdf.py .

# Copy plugin main functions
COPY ras-stac/plugins .