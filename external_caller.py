import sys
import warnings

from ras_stac.ras1d.converter import process_in_place_s3

warnings.filterwarnings("ignore")

from papipyplug import parse_input, plugin_logger, print_results

PLUGIN_PARAMS = {"required": ["bucket", "s3_prefix", "counties"], "optional": ["bucket"]}

if __name__ == "__main__":
    plugin_logger()

    input_params = parse_input(sys.argv, PLUGIN_PARAMS)

    in_path = input_params.get("in_path")
    crs = input_params.get("crs")
    out_path = input_params.get("out_path")

    if in_path == "TEST":
        results = f"{crs} | {out_path}"
    else:
        results = process_in_place_s3(in_path, crs, out_path)
    print_results(results)
