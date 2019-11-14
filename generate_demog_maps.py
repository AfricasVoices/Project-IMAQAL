import argparse
import json
from core_data_modules.cleaners import Codes
import sys

from core_data_modules.logging import Logger
from core_data_modules.traced_data.io import TracedDataJsonIO

from src.lib import PipelineConfiguration

Logger.set_project_name("IMAQAL")
log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generates graphs for analysis")

    parser.add_argument("user", help="User launching this program")
    parser.add_argument("messages_json_input_path", metavar="messages-json-input-path",
                        help="Path to a directory with JSON files to read the TracedData of the messages data from")
    parser.add_argument("demog_map_json_output_path", metavar="demog-map-json-output-path",
                        help="Path to a JSON file to write per episode demog map ")

    args = parser.parse_args()

    user = args.user
    messages_json_input_path = args.messages_json_input_path
    demog_map_json_output_path = args.demog_map_json_output_path

    rqa_raw_fields =[]
    for plan in PipelineConfiguration.RQA_CODING_PLANS:
        rqa_raw_fields.append(plan.raw_field)

    for rqa_raw_field in rqa_raw_fields:
        # Read the messages dataset
        with open(f'{messages_json_input_path}/{rqa_raw_field}_messages_traced_data.jsonl') as f:
            messages = TracedDataJsonIO.import_jsonl_to_traced_data_iterable(f)
        log.info(f"Loaded {len(messages)} messages")

        log.info(f"Generating demog map for {rqa_raw_field}...")
        demog_map = dict()
        sys.setrecursionlimit(20000)
        for avf_uid in messages:
            if avf_uid["uid"] in demog_map.keys():
                continue
            if avf_uid.get(f'{rqa_raw_field}', "") != "" and avf_uid["consent_withdrawn"] == Codes.FALSE:
                uid = avf_uid["uid"]
                demog_data_for_id = {}
                for plan in PipelineConfiguration.DEMOG_CODING_PLANS:
                    key = plan.analysis_file_key
                    print()
                    demog_data_for_id[key] = plan.code_scheme.get_code_with_id(avf_uid[plan.coded_field]["CodeID"]).string_value
                demog_map[uid] = demog_data_for_id

        log.info(f'Writing demog map for {len(demog_map.keys())} uids for {rqa_raw_field}')
        with open(f'{demog_map_json_output_path}/{rqa_raw_field}_demog_map.json', "w") as f:
            json.dump(demog_map, f)
