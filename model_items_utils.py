import contextily as ctx
import matplotlib.pyplot as plt
from io import BytesIO
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
from ras_stac.utils.s3_utils import *


def create_model_thumbnails(breaklines_gdf, bc_lines_gdf, mesh_polygons_gdf, title, png_output_s3_path, s3_client):
    # Convert GeoDataFrames to EPSG:4326 for plotting labels
    if breaklines_gdf is not None:
        breaklines_geo = breaklines_gdf.to_crs(epsg=4326)
    bc_lines_geo = bc_lines_gdf.to_crs(epsg=4326)
    mesh_polygons_geo = mesh_polygons_gdf.to_crs(epsg=4326)

    fig, ax = plt.subplots(figsize=(12, 12))
    
    # Plot each GeoDataFrame
    mesh_polygons_geo.plot(ax=ax, edgecolor='silver', facecolor='none', linestyle='-', alpha=0.7, label='Mesh Polygons')
    if breaklines_gdf is not None:
        breaklines_geo.plot(ax=ax, edgecolor='red', linestyle='-', alpha=0.7, label='Breaklines')
    bc_lines_geo.plot(ax=ax, edgecolor='blue', linestyle='-', alpha=0.7, label='BC Lines')

    # Add openstreetmap basemap
    ctx.add_basemap(ax, crs=mesh_polygons_geo.crs.to_string(), source=ctx.providers.OpenStreetMap.Mapnik, alpha=0.4)

    ax.set_title(title, fontsize=15)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")

    # Manually add legend because the mesh polygons wont show in legend with facecolor='none' when plotted
    legend_handles = [
        Line2D([0], [0], color='blue', linestyle='-', linewidth=2, label='BC Lines'),
        Patch(color='silver', edgecolor='silver', label='Mesh Polygons')
    ]
    
    # Add breaklines to the legend only if they are present
    if breaklines_gdf is not None:
        legend_handles.insert(0, Line2D([0], [0], color='red', linestyle='-', linewidth=2, label='Breaklines'))

    ax.legend(handles=legend_handles, loc='upper right')

    # plt.plot()

    buf = BytesIO()
    plt.savefig(buf, format="png")
    buf.seek(0)

    plt.close(fig)

    bucket, key = split_s3_path(png_output_s3_path)
    s3_client.put_object(Bucket=bucket, Key=key, Body=buf, ContentType="image/png")

def break_lines_to_metadata(bc_lines_df):
    """Reads bc line dataframe and converts to stac format for metadata"""
    bc_dict = {}

    for _, row in bc_lines_df.iterrows():
        key = f"BC_Lines:{row['name'].replace(' ', '_')}"
        
        value = [f"Mesh_name: {row['mesh_name']}", f"Type: {row['type']}"]
        
        bc_dict[key] = value

    return bc_dict

def get_all_model_files(bucket_name, prefix, s3_client):
    """Gets all files from a given prefix without going further into any folders at that location. 
    Used to retrieve all associated model files."""

    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    files = []

    if 'Contents' in response:
        for obj in response['Contents']:
            key = obj['Key']
            
            if '/' not in key[len(prefix) + 1:]:
                file_path = f"s3://{bucket_name}/{key}"
                files.append(file_path)

    return files