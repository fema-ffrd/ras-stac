import json
import logging
import sys
import warnings

from botocore.exceptions import ClientError

from ras_stac.ras1d.converter import process_in_place_s3
from ras_stac.ras1d.utils.s3_utils import str_from_s3

warnings.filterwarnings("ignore")

from papipyplug import parse_input, plugin_logger, print_results

PLUGIN_PARAMS = {"required": ["in_prefix", "crs", "out_prefix"], "optional": []}


def owp_wrapper(in_prefix: str, out_prefix: str) -> dict:
    """Temporary wrapper to meet OWP MIP 30% deliverable deadline"""
    crs_path = in_prefix.replace("source_models", "source_crs") + "crs_inference.json"
    try:
        crs_dict = json.loads(str_from_s3(crs_path))
        crs = crs_dict["best_crs"]
    except ClientError:
        crs = None
    return process_in_place_s3(in_prefix, crs, out_prefix)


def main():
    plugin_logger()

    input_params = parse_input(sys.argv, PLUGIN_PARAMS)

    in_prefix = input_params.get("in_prefix")
    crs = input_params.get("crs")
    out_prefix = input_params.get("out_prefix")

    # Debugging option
    if in_prefix == "TEST":
        results = f"{crs} | {out_prefix}"
        print_results(results)
        return

    # Process
    try:
        results = owp_wrapper(in_prefix, out_prefix)
        print_results(results)
    except Exception as e:
        logging.warning(e)


if __name__ == "__main__":
    main()
