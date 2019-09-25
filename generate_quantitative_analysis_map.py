import argparse

from core_data_modules.logging import Logger
from core_data_modules.traced_data.io import TracedDataJsonIO
from core_data_modules.util import IOUtils

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

    # Read the messages dataset
    log.info(f"Loading the messages dataset from {messages_json_input_path}...")
    with open(messages_json_input_path) as f:
        messages = TracedDataJsonIO.import_jsonl_to_traced_data_iterable(f)
    log.info(f"Loaded {len(messages)} messages")

    # Read the individuals dataset
    log.info(f"Loading the individuals dataset from {individuals_json_input_path}...")
    with open(individuals_json_input_path) as f:
        individuals = TracedDataJsonIO.import_jsonl_to_traced_data_iterable(f)
    log.info(f"Loaded {len(individuals)} individuals")
