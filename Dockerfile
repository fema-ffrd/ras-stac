FROM ghcr.io/osgeo/gdal:ubuntu-small-3.8.5

RUN apt-get update && \
    apt-get install jq -y && \
    apt-get install -y python3-pip && \
    pip install --upgrade pip \

WORKDIR /app

COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY ./ ./
