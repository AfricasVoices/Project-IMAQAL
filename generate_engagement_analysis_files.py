from collections import OrderedDict
import argparse
import json
import csv

from core_data_modules.logging import Logger

from src.lib import PipelineConfiguration

Logger.set_project_name("IMAQAL")
log = Logger(__name__)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Computes engagement data and stores it in multiple CSV sheets for"
                                                 "analysis and visualization")

    parser.add_argument("user", help="User launching this program")
    parser.add_argument("pipeline_configuration_file_path", metavar="pipeline-configuration-file",
                        help="Path to the pipeline configuration json file"),
    parser.add_argument("demog_map_json_input_dir", metavar="demog-map-json-input-dir",
                        help="Path to a directory to read per episode demog map .json files. "
                             "The files should be in the format <rqa_raw_field>_demog_map.json")
    parser.add_argument("engagement_csv_output_dir", metavar="engagement-csv-output-dir",
                        help="Directory to write engagement CSV files to")


    args = parser.parse_args()

    user = args.user
    pipeline_configuration_file_path = args.pipeline_configuration_file_path
    demog_map_json_input_dir = args.demog_map_json_input_dir
    engagement_csv_output_dir = args.engagement_csv_output_dir

    # Load the pipeline configuration file
    log.info("Loading Pipeline Configuration File...")
    with open(pipeline_configuration_file_path) as f:
        pipeline_configuration = PipelineConfiguration.from_configuration_file(f)

    # Infer which RQA coding plans to use from the pipeline name.
    # Starting from q4 pipeline since the pipeline was refactored into quarterly pipelines when the project had reached
    # q4 reporting. Earlier episodes are covered in the full pipeline configurations
    if pipeline_configuration.pipeline_name == "q4_pipeline":
        log.info("Extracting Q4 pipeline data")
        PipelineConfiguration.RQA_CODING_PLANS = PipelineConfiguration.Q4_RQA_CODING_PLANS
    elif pipeline_configuration.pipeline_name == "q5_pipeline":
        log.info("Extracting Q5 pipeline data")
        PipelineConfiguration.RQA_CODING_PLANS = PipelineConfiguration.Q5_RQA_CODING_PLANS
    elif pipeline_configuration.pipeline_name == "q6_pipeline":
        log.info("Extracting Q6 pipeline data")
        PipelineConfiguration.RQA_CODING_PLANS = PipelineConfiguration.Q6_RQA_CODING_PLANS
    else:
        assert pipeline_configuration.pipeline_name == "full_pipeline", "PipelineName must be either 'a quartely pipeline name' or 'full pipeline'"
        log.info("Extracting full pipeline data")
        PipelineConfiguration.RQA_CODING_PLANS = PipelineConfiguration.FULL_PIPELINE_RQA_CODING_PLANS

    # Create a list of rqa_raw_fields
    rqa_raw_fields = []
    for plan in PipelineConfiguration.RQA_CODING_PLANS:
        rqa_raw_fields.append(plan.raw_field)

    log.info(f'Computing unique, lifetime_activations_per_show and per-show participants ...' )
    engagement_map = {}  # of uid -> name of shows participated in and their demographics data
    unique_uids = set()  # unique uids that participated in the entire project
    lifetime_activations_per_show = []  # uids that participated in each radio show for the entire project
    participants_per_show = OrderedDict()  # of rqa_raw_field -> sum of total uids who participated
    for rqa_raw_field in rqa_raw_fields:
        with open(f'{demog_map_json_input_dir}/{rqa_raw_field}_demog_map.json') as f:
            data = json.load(f)
        log.info(f"Loaded {len(data)} {rqa_raw_field} uids ")

        participants_per_show[f"{rqa_raw_field}"] = len(data.keys())

        for uid, demogs in data.items():
            demog = data[uid]

            if uid not in engagement_map:
                engagement_map[uid] = {
                    "demog": demog,
                    "shows": []
                }
                assert demogs == engagement_map[uid]['demog'] , f"{demogs} not equal to {engagement_map[uid]['demog']}" \
                    f" for {uid}"

            engagement_map[uid]["shows"].append(rqa_raw_field)
            unique_uids.add(uid)
            lifetime_activations_per_show.append(uid)

    # Export the engagement counts to their respective csv file.
    log.info(f'Writing unique participants csv ...')
    with open(f"{engagement_csv_output_dir}/unique_participants.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow([len(unique_uids)])

    log.info(f'Writing lifetime activations per show csv ...')
    with open(f"{engagement_csv_output_dir}/lifetime_activations_per_show.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow([len(lifetime_activations_per_show)])

    log.info(f'Writing show participation csv ...')
    with open(f"{engagement_csv_output_dir}/show_participation.csv", "w") as f:
        writer = csv.writer(f)

        for row in participants_per_show.items():
            writer.writerow(row)
