import sys

from ras_geom_hdf import PLUGIN_PARAMS, main
from papipyplug import parse_input, plugin_logger, print_results

if __name__ == "__main__":
    plugin_logger()
    input_params = parse_input(sys.argv, PLUGIN_PARAMS)
    result = main(input_params)
    print_results(result)
