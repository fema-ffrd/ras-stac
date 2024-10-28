import sys
import warnings

from ras_stac.ras1d.converter import process_in_place_s3

warnings.filterwarnings("ignore")

from papipyplug import parse_input, plugin_logger, print_results

PLUGIN_PARAMS = {"required": ["in_prefix", "crs", "out_prefix"]}

if __name__ == "__main__":
    plugin_logger()

    input_params = parse_input(sys.argv, PLUGIN_PARAMS)

    in_prefix = input_params.get("in_prefix")
    crs = input_params.get("crs")
    out_prefix = input_params.get("out_prefix")

    if in_prefix == "TEST":
        results = f"{crs} | {out_prefix}"
    else:
        results = process_in_place_s3(in_prefix, crs, out_prefix)
    print_results(results)
