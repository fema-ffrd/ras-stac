from typing import List, Any


def get_dict_values(dicts: List[dict], key: Any) -> list:
    """
    This function retrieves the values of a specific key from a list of dictionaries.

    Parameters:
        dicts (List[dict]): The list of dictionaries.
        key (Any): The key to retrieve the values of.

    Returns:
        List[dict]: A list with the values of the key in the dictionaries. If a dictionary does not have the key, it is skipped.
    """
    results = []
    for d in dicts:
        if key in d:
            results.append(d[key])
    return results


GEOM_HDF_IGNORE_PROPERTIES = [
    "geometry:complete_geometry",
    "2d_flow_areas:cell_volume_tolerance",
    "2d_flow_areas:data_date",
    "2d_flow_areas:extents",
    "2d_flow_areas:cell_maximum_index",
    "2d_flow_areas:face_area_conveyance_ratio",
    "2d_flow_areas:face_area_elevation_tolerance",
    "2d_flow_areas:face_profile_tolerance",
    "2d_flow_areas:infiltration_date_last_modified",
    "2d_flow_areas:infiltration_file_date",
    "2d_flow_areas:infiltration_filename",
    "2d_flow_areas:infiltration_layername",
    "2d_flow_areas:land_cover_date_last_modified",
    "2d_flow_areas:land_cover_file_date",
    "2d_flow_areas:land_cover_filename",
    "2d_flow_areas:land_cover_layername",
    "2d_flow_areas:mannings_n",
    "2d_flow_areas:property_tables_last_computed",
    "2d_flow_areas:terrain_file_date",
    "2d_flow_areas:terrain_filename",
    "2d_flow_areas:version",
    "2d_flow_areas:infiltration_override_table_hash",
    "2d_flow_areas:property_tables_lc_hash",
]
