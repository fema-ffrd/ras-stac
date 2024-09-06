from ras_stac.ras_geom_hdf import new_geom_item
from ras_stac.utils.s3_utils import *
from model_items_utils import create_model_thumbnails, bc_lines_to_metadata, get_all_model_files

model_name = "BluestoneLocal"

bucket_name = "kanawha-pilot"
model_prefix = f"FFRD_Kanawha_Compute/ras/{model_name}"
png_output_s3_path = f"s3://kanawha-pilot/stac/Kanawha-0505/thumbnails/{model_name}.png"
new_item_s3_path = f"s3://kanawha-pilot/stac/Kanawha-0505/model_items/{model_name}.json"


session, s3_client, s3_resource = init_s3_resources()


model_files = get_all_model_files(bucket_name, model_prefix, s3_client)

for file in model_files:
    if file.endswith('.g01.hdf'):
        model_geom = file

ras_geom_hdf, ras_model_name = read_geom_hdf_from_s3(model_geom)

# Reading breaklines cause errors on a couple models, assuming no breaklines in those 
try:
    model_breaklines = ras_geom_hdf.breaklines()
except:
    model_breaklines = None

bc_lines = ras_geom_hdf.bc_lines()
mesh_polygons = ras_geom_hdf.mesh_cell_polygons()

bcline_metadata = bc_lines_to_metadata(bc_lines)


create_model_thumbnails(model_breaklines, bc_lines, mesh_polygons, ras_model_name, png_output_s3_path, s3_client)

model_files.extend([png_output_s3_path, model_geom])
geom_item = new_geom_item(ras_geom_hdf, ras_model_name, other_assets = model_files, item_props_to_add = bcline_metadata, s3_resource=s3_resource)

copy_item_to_s3(geom_item, new_item_s3_path, s3_client)