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

PLAN_HDF_IGNORE_PROPERTIES = [
    "plan_parameters:1d_cores",
    "plan_parameters:1d_methodology",
    "plan_parameters:1d2d_flow_tolerance",
    "plan_parameters:1d2d_maxiter",
    "plan_parameters:1d2d_minimum_flow_tolerance",
    "plan_parameters:1d2d_ws_tolerance",
    "plan_information:plan_shortid",
    "plan_information:plan_title",
    "plan_information:time_window",
    "plan_parameters:2d_boundary_condition_ramp_up_fraction",
    "plan_parameters:2d_boundary_condition_volume_check",
    "plan_parameters:2d_cores_per_mesh",
    "plan_parameters:2d_coriolis",
    "plan_parameters:2d_initial_conditions_ramp_up_time_hrs",
    "plan_parameters:2d_latitude_for_coriolis",
    "plan_parameters:2d_longitudinal_mixing_coefficient",
    "plan_parameters:2d_matrix_solver",
    "plan_parameters:2d_maximum_iterations",
    "plan_parameters:2d_number_of_time_slices",
    "plan_parameters:2d_only",
    "plan_parameters:2d_smagorinsky_mixing_coefficient",
    "plan_parameters:2d_theta",
    "plan_parameters:2d_theta_warmup",
    "plan_parameters:2d_transverse_mixing_coefficient",
    "plan_parameters:2d_turbulence_formulation",
    "plan_parameters:gravity",
    "plan_information:flow_title",
    "plan_information:geometry_title",
    "plan_information:plan_title",
    "plan_parameters:hdf_chunk_size",
    "plan_parameters:hdf_compression",
    "plan_parameters:hdf_fixed_rows",
    "plan_parameters:hdf_flush_buffer",
    "plan_parameters:hdf_spatial_parts",
    "plan_parameters:hdf_use_max_rows",
    "plan_parameters:hdf_write_time_slices",
    "plan_parameters:hdf_write_warmup",
    "plan_parameters:pardiso_solver",
    "meteorology:enabled",
    "meteorology:raster_cols",
    "meteorology:raster_left",
    "meteorology:raster_rows",
    "meteorology:raster_top",
    "results_summary:run_time_window",
    "results_summary:computation_time_total_minutes",
    "unsteady_results:short_id",
    "volume_accounting:total_boundary_flux_of_water_in",
    "volume_accounting:total_boundary_flux_of_water_out",
    "volume_accounting:vol_accounting_in",
    "volume_accounting:error",
    "volume_accounting:volume_ending",
    "volume_accounting:volume_starting",
    "volume_accounting:precipitation_excess_acre_feet",
    "start_datetime",
    "end_datetime",
    "datetime",
]