import argparse
import json
from core_data_modules.cleaners import Codes
import sys
from collections import OrderedDict
import csv

from core_data_modules.logging import Logger
from core_data_modules.traced_data.io import TracedDataJsonIO

from src.lib import PipelineConfiguration

Logger.set_project_name("IMAQAL")
log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generates demog maps for engagement analysis")

    parser.add_argument("user", help="User launching this program")
    parser.add_argument("pipeline_configuration_file_path", metavar="pipeline-configuration-file",
                        help="Path to the pipeline configuration json file"),
    parser.add_argument("messages_json_input_path", metavar="messages-json-input-path",
                        help="Path to a directory with per RQA JSON files to read the TracedData of the messages data from"
                             "This are generated by running the full pipeline with demog raw data file and "
                             "only one RQA raw data file and must have file names in the format <rqa_raw_field>_messages_traced_data.jsonl")
    parser.add_argument("output_dir", metavar="output-dir",
                        help="Path to a directory to write per episode demog map JSON files and the messages per show csv ")

    args = parser.parse_args()

    user = args.user
    pipeline_configuration_file_path = args.pipeline_configuration_file_path
    messages_json_input_path = args.messages_json_input_path
    output_dir = args.output_dir

    # Load the pipeline configuration file
    log.info("Loading Pipeline Configuration File...")
    with open(pipeline_configuration_file_path) as f:
        pipeline_configuration = PipelineConfiguration.from_configuration_file(f)

    # Infer which RQA coding plans to use from the pipeline name.
    if pipeline_configuration.pipeline_name == "q4_pipeline":
        log.info("Extracting Q4 pipeline data")
        PipelineConfiguration.RQA_CODING_PLANS = PipelineConfiguration.Q4_RQA_CODING_PLANS
    elif pipeline_configuration.pipeline_name == "q5_pipeline":
        log.info("Extracting Q5 pipeline data")
        PipelineConfiguration.RQA_CODING_PLANS = PipelineConfiguration.Q5_RQA_CODING_PLANS
    elif pipeline_configuration.pipeline_name == "q6_pipeline":
        log.info("Extracting Q6 pipeline data")
        PipelineConfiguration.RQA_CODING_PLANS = PipelineConfiguration.Q6_RQA_CODING_PLANS
    elif pipeline_configuration.pipeline_name == "q7_pipeline":
        log.info("Extracting Q7 pipeline data")
        PipelineConfiguration.RQA_CODING_PLANS = PipelineConfiguration.Q7_RQA_CODING_PLANS
    elif pipeline_configuration.pipeline_name == "q8_pipeline":
        log.info("Extracting Q8 pipeline data")
        PipelineConfiguration.RQA_CODING_PLANS = PipelineConfiguration.Q8_RQA_CODING_PLANS
    else:
        assert pipeline_configuration.pipeline_name == "full_pipeline", "PipelineName must be either" \
                                                                        " 'a quarterly pipeline name' or 'full pipeline'"
        log.info("Extracting full Pipeline data")
        PipelineConfiguration.RQA_CODING_PLANS = PipelineConfiguration.FULL_PIPELINE_RQA_CODING_PLANS

    rqa_raw_fields =[]
    messages_per_show_with_optins = OrderedDict()  # Of radio show index to messages count for the participants who opted in
    for plan in PipelineConfiguration.RQA_CODING_PLANS:
        rqa_raw_fields.append(plan.raw_field)
        messages_per_show_with_optins[plan.raw_field] = 0

    for rqa_raw_field in rqa_raw_fields:
        # Read the messages dataset
        with open(f'{messages_json_input_path}/{rqa_raw_field}_messages_traced_data.jsonl') as f:
            messages = TracedDataJsonIO.import_jsonl_to_traced_data_iterable(f)
        log.info(f"Loaded {len(messages)} messages")

        log.info(f"Generating demog map and calculating total messages for {rqa_raw_field}...")
        demog_map = dict()
        sys.setrecursionlimit(20000)
        for msg in messages:
            if msg.get(rqa_raw_field, "") != "":
                continue

            if msg["consent_withdrawn"] == Codes.FALSE:
                # Compute the number of messages in each show
                messages_per_show_with_optins[rqa_raw_field] += 1

                if msg["uid"] in demog_map.keys():
                    continue

                uid = msg["uid"]
                demog_data_for_id = {}
                for plan in PipelineConfiguration.DEMOG_CODING_PLANS:
                    for cc in plan.coding_configurations:
                        if cc.analysis_file_key is None:
                            continue
                        key = cc.analysis_file_key
                        demog_data_for_id[key] = cc.code_scheme.get_code_with_code_id(msg[cc.coded_field]["CodeID"]).string_value
                    demog_map[uid] = demog_data_for_id

        log.info(f'Writing demog map for {len(demog_map.keys())} uids for {rqa_raw_field}')
        with open(f'{output_dir}/{rqa_raw_field}_demog_map.json', "w") as f:
            json.dump(demog_map, f)

    log.info(f'Writing messages_per_show with optins csv')
    # Export the no. of messages for consented participants per frequency data to a csv
    with open(f"{output_dir}/messages_per_show_with_optins.csv", "w") as f:
        headers = ["Show", "No. of messages with opt-ins",]
        header_writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        header_writer.writeheader()

        writer = csv.writer(f, lineterminator="\n")
        for row in messages_per_show_with_optins.items():
            writer.writerow(row)
