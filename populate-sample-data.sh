#!/bin/bash

set -a
source .env
set +a
set -e


# Create a ras-geometry item
echo "Creating geometry_item...."
python -m ras_stac.ras_geom_hdf "$(jq -r . ras_stac/example-inputs/green-geometry.json)"

# Create a catalog and add the ras-geometry item
echo "Creating catalog...."
python -m new_catalog stac/Greenbrier/Greenbrier.g01.hdf.json
echo "Success!"

# Create a ras-plan item
echo "Creating plan_item...."
python -m ras_stac.ras_plan_hdf "$(jq -r . ras_stac/example-inputs/green-plan.json)"

# Update catalog with new ras-plan item
echo "Updating catalog...."
python -m update_catalog stac/Greenbrier/Greenbrier.p01.hdf.json
echo "Success!"

# Create a ras-stac item for the depth grid
echo "Creating dg_item...."
python -m ras_stac.ras_plan_dg "$(jq -r . ras_stac/example-inputs/green-depth-grid.json)"

# Update catalog with new ras-plan item
echo "Updating catalog...."
python -m update_catalog stac/Greenbrier/Greenbrier.p01.dg.json
echo "Success!"
