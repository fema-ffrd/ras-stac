#!/bin/bash

set -a
source .env
set +a
set -e


# Create a ras-geometry item
echo "Creating geometry_item...."
cd ras-stac &&
python -m ras_geom_hdf "$(jq -r . example-inputs/green-geometry.json)"

# Create a catalog and add the ras-geometry item
echo "Creating catalog...."
cd .. 
python -m new_catalog stac/Greenbrier/Greenbrier.g01.hdf.json
echo "Success!"

# Create a ras-plan item
echo "Creating plan_item...."
cd ras-stac &&
python -m ras_plan_hdf "$(jq -r . example-inputs/green-plan.json)"

# Update catalog with new ras-plan item
echo "Updating catalog...."
cd .. 
python -m update_catalog stac/Greenbrier/Greenbrier.p01.hdf.json
echo "Success!"

# Create a ras-stac item for the depth grid
cd ras-stac &&
echo "Creating dg_item...."
python -m ras_plan_dg "$(jq -r . example-inputs/green-depth-grid.json)"

# Update catalog with new ras-plan item
echo "Updating catalog...."
cd .. 
python -m update_catalog stac/Greenbrier/Greenbrier.p01.dg.json
echo "Success!"
