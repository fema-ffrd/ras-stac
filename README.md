# ras-stac
[![CI](https://github.com/fema-ffrd/rashdf/actions/workflows/continuous-integration.yml/badge.svg?branch=main)](https://github.com/fema-ffrd/ras-stac/actions/workflows/continuous-integration.yml)
[![Release](https://github.com/fema-ffrd/ras-stac/actions/workflows/release.yml/badge.svg)](https://github.com/fema-ffrd/ras-stac/actions/workflows/release.yml)
[![PyPI version](https://badge.fury.io/py/ras-stac.svg)](https://badge.fury.io/py/ras-stac)

Utilities for making SpatioTemporal Asset Catalogs of HEC-RAS models

This repository contains code for developing STAC items from HEC-RAS models. Current activities focus on creating items for geometry files `g**.hdf` and plan files `p**.hdf` stored in AWS S3. More to come. 

## Developer Setup

For local development, create a `.env` file using the `example.env` file.

Create a virtual environment in the project directory:
```
$ python -m venv venv
```

Activate the virtual environment:
```
$ source ./venv/bin/activate
(venv) $
```

Install required libraries:
```
(venv) $ pip install -r requirements.txt
```

With the virtual environment activated, run the tests:
```
(venv) $ pytest
```

Or run the tests in a docker container:
```
(venv) $ ./docker-test.sh
```
*When `docker-test.sh` is executed, the Docker image is built and `pytest` is invoked to run the Python test scripts. This leverages test data that is included in this repository. This does not use cloud storage, s3, minio, nor other forms of network data.*

