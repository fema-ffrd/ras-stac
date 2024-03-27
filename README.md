# ras-stac
Utilities for making SpatioTemporal Asset Catalogs of HEC-RAS models

This repository contains code for developing STAC items from HEC-RAS models. Current activities focus on creating items for geometry files `g**.hdf` stored in AWS S3. More to come. 

*Source code largely ported from [ffrd-stac](https://github.com/arc-pts/ffrd-stac/blob/204e1ec85068936856b317fa9446da3c4da5d8d4/ffrd_stac/rasmeta.py).*


## Getting Started

1. For local development, create a `.env` file using the `example.env` file.

2. Start a minio service and load data using the [cloud-mock](https://github.com/fema-ffrd/cloud-mock) repository.

3. Run the `populate-sample-data.sh` script to test set-up, connetivity, and view a sample stac catalog created using this library.


**NOTE** It is recommended that ras-stac not be run in a container for testing and development due to networking issues that complicate use of these tools, when using minio. 
