import argparse
import json

from core_data_modules.logging import Logger
from core_data_modules.traced_data.io import TracedDataJsonIO
from core_data_modules.util import IOUtils, SHAUtils
from core_data_modules.cleaners import Codes

from src.lib import PipelineConfiguration

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

    individuals_quantitative_map = {}
    messages_quantitative_map = {}

    individuals_quantitative_map['INDIVIDUAL_SHA'] = SHAUtils.sha_string(f'{individuals}')
    messages_quantitative_map['MESSAGE_SHA'] = SHAUtils.sha_string(f'{messages}')

    individuals_quantitative_map['ids'] = []
    for uid in individuals:
        if uid['consent_withdrawn'] == Codes.FALSE:
            demog_data_for_id = {}

            # Create labelled demographic map for each uid
            for plan in PipelineConfiguration.DEMOG_CODING_PLANS:
                key = plan.analysis_file_key
                demog_data_for_id[key] = plan.code_scheme.get_code_with_id(uid[plan.coded_field]["CodeID"]).string_value

            avf_phone_uid = {uid["avf_phone_id"] : [demog_data_for_id]}
            individuals_quantitative_map['ids'].append(avf_phone_uid)

    # Write individuals quantitative map
    with open(f'{output_dir}/individuals_quantitative_map.json', "wb") as f:
        f.write(json.dumps(individuals_quantitative_map).encode("utf-8"))

    # Write messages quantitative map
    with open(f'{output_dir}/messages_quantitative_map.json', "wb") as f:
        f.write(json.dumps(messages_quantitative_map).encode("utf-8"))
