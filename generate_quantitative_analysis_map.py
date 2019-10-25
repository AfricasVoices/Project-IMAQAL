import argparse
import json

from core_data_modules.logging import Logger
from core_data_modules.util import IOUtils, SHAUtils


Logger.set_project_name("IMAQAL")
log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generates a map to be used in quantitative analysis")

    parser.add_argument("user", help="User launching this program")
    parser.add_argument("messages_json_input_path", metavar="messages-json-input-path",
                        help="Path to a JSON file to read the TracedData of the messages data from")
    parser.add_argument("individuals_json_input_path", metavar="individuals-json-input-path",
                        help="Path to a JSON file to read the TracedData of the messages data from")
    parser.add_argument("output_dir", metavar="output-dir",
                        help="Directory to write the output map to")

    args = parser.parse_args()

    user = args.user
    messages_json_input_path = args.messages_json_input_path
    individuals_json_input_path = args.individuals_json_input_path
    output_dir = args.output_dir

    IOUtils.ensure_dirs_exist(output_dir)

    messages_quantitative_map = {
        "origin_traced_data_file_sha": SHAUtils.sha_file_at_path(messages_json_input_path)
    }

    individuals_quantitative_map = {
        "origin_traced_data_file_sha": SHAUtils.sha_file_at_path(individuals_json_input_path)
    }

    # Write messages quantitative map
    with open(f'{output_dir}/messages_quantitative_map.json', "w") as f:
        json.dump(messages_quantitative_map, f)

    # Write individuals quantitative map
    with open(f'{output_dir}/individuals_quantitative_map.json', "w") as f:
        json.dump(individuals_quantitative_map, f)
