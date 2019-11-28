import argparse
from collections import OrderedDict
import altair
import glob
import json
import csv
import sys

from core_data_modules.logging import Logger
from core_data_modules.traced_data.io import TracedDataJsonIO
from core_data_modules.util import IOUtils
from core_data_modules.cleaners import Codes
from core_data_modules.data_models.code_scheme import CodeTypes
from storage.google_cloud import google_cloud_utils
from storage.google_drive import drive_client_wrapper

from src.lib import PipelineConfiguration
from src.lib.pipeline_configuration import CodingModes

Logger.set_project_name("IMAQAL")
log = Logger(__name__)

IMG_SCALE_FACTOR = 10  # Increase this to increase the resolution of the outputted PNGs

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generates graphs for analysis")

    parser.add_argument("user", help="User launching this program")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("pipeline_configuration_file_path", metavar="pipeline-configuration-file",
                        help="Path to the pipeline configuration json file")

    parser.add_argument("messages_json_input_path", metavar="messages-json-input-path",
                        help="Path to a JSONL file to read the TracedData of the messages data from")
    parser.add_argument("individuals_json_input_path", metavar="individuals-json-input-path",
                        help="Path to a JSONL file to read the TracedData of the messages data from")
    parser.add_argument("output_dir", metavar="output-dir",
                        help="Directory to write the output graphs to")

    args = parser.parse_args()

    user = args.user
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    pipeline_configuration_file_path = args.pipeline_configuration_file_path

    messages_json_input_path = args.messages_json_input_path
    individuals_json_input_path = args.individuals_json_input_path
    output_dir = args.output_dir

    IOUtils.ensure_dirs_exist(output_dir)

    # Load the pipeline configuration file
    log.info("Loading Pipeline Configuration File...")
    with open(pipeline_configuration_file_path) as f:
        pipeline_configuration = PipelineConfiguration.from_configuration_file(f)

    if pipeline_configuration.drive_upload is not None:
        log.info(f"Downloading Google Drive service account credentials...")
        credentials_info = json.loads(google_cloud_utils.download_blob_to_string(
            google_cloud_credentials_file_path, pipeline_configuration.drive_upload.drive_credentials_file_url))
        drive_client_wrapper.init_client_from_info(credentials_info)

    # Read the individuals dataset
    log.info(f"Loading the individuals dataset from {individuals_json_input_path}...")
    with open(individuals_json_input_path) as f:
        individuals = TracedDataJsonIO.import_jsonl_to_traced_data_iterable(f)
    log.info(f"Loaded {len(individuals)} individuals")

    # Infer which RQA coding plans to use from the pipeline name.
    if pipeline_configuration.pipeline_name == "q4_pipeline":
        log.info("Running Q4 pipeline")
        PipelineConfiguration.RQA_CODING_PLANS = PipelineConfiguration.Q4_RQA_CODING_PLANS
    elif pipeline_configuration.pipeline_name == "q5_pipeline":
        log.info("Running Q5 pipeline")
        PipelineConfiguration.RQA_CODING_PLANS = PipelineConfiguration.Q5_RQA_CODING_PLANS
    elif pipeline_configuration.pipeline_name == "q6_pipeline":
        log.info("Running Q6 pipeline")
        PipelineConfiguration.RQA_CODING_PLANS = PipelineConfiguration.Q6_RQA_CODING_PLANS
    else:
        assert pipeline_configuration.pipeline_name == "full_pipeline", "PipelineName must be either 'a quartely pipeline name' or 'full pipeline'"
        log.info("Running full Pipeline")
        PipelineConfiguration.RQA_CODING_PLANS = PipelineConfiguration.FULL_PIPELINE_RQA_CODING_PLANS

    # Compute the number of messages, individuals, and relevant messages per episode and overall.
    log.info("Computing the per-episode and per-season engagement counts...")
    sys.setrecursionlimit(20000)
    engagement_counts = OrderedDict()
    for plan in PipelineConfiguration.RQA_CODING_PLANS:
        # TODO: Add another field to CodingPlan so that we can give the weeks better names than the raw_field
        engagement_counts[plan.raw_field] = {
            "Episode": plan.raw_field,
            "Total Participants": 0,
        }
    engagement_counts["Total"] = {
        "Episode": "Total",
        "Total Participants": 0,
    }

    # Compute, per episode and across the season:
    #  - Total Participants, by counting the number of consenting individuals objects that contain the raw_field key
    #    each week.
    for ind in individuals:
        if ind["consent_withdrawn"] == Codes.FALSE:
            engagement_counts["Total"]["Total Participants"] += 1
            for plan in PipelineConfiguration.RQA_CODING_PLANS:
                if ind.get(plan.raw_field, "") == "":
                    continue

                engagement_counts[plan.raw_field]["Total Participants"] += 1

    # Export the engagement counts to a csv.
    with open(f"{output_dir}/individuals_engagement_counts.csv", "w") as f:
        headers = ["Episode", "Total Participants",]
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        writer.writeheader()

        for row in engagement_counts.values():
            writer.writerow(row)

    # Plot the per-season distribution of responses for each survey question, per individual
    for plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.DEMOG_CODING_PLANS + \
                PipelineConfiguration.FOLLOW_UP_CODING_PLANS:
        for cc in plan.coding_configurations:
            if cc.analysis_file_key is None:
                continue

            log.info(f"Graphing the distribution of codes for {cc.analysis_file_key}...")
            label_counts = OrderedDict()
            for code in cc.code_scheme.codes:
                label_counts[code.string_value] = 0

            if cc.coding_mode == CodingModes.SINGLE:
                for ind in individuals:
                    label_counts[ind[cc.analysis_file_key]] += 1
            else:
                assert cc.coding_mode == CodingModes.MULTIPLE
                for ind in individuals:
                    for code in cc.code_scheme.codes:
                        if ind[f"{cc.analysis_file_key}{code.string_value}"] == Codes.MATRIX_1:
                            label_counts[code.string_value] += 1

            chart = altair.Chart(
                altair.Data(values=[{"label": k, "count": v} for k, v in label_counts.items()])
            ).mark_bar().encode(
                x=altair.X("label:N", title="Label", sort=list(label_counts.keys())),
                y=altair.Y("count:Q", title="Number of Individuals")
            ).properties(
                title=f"Season Distribution: {cc.analysis_file_key}"
            )
            chart.save(f"{output_dir}/season_distribution_{cc.analysis_file_key}.html")
            chart.save(f"{output_dir}/season_distribution_{cc.analysis_file_key}.png",
                       scale_factor=IMG_SCALE_FACTOR)
    
    if pipeline_configuration.drive_upload is not None:
        # TODO : upload engagement csvs to an engagement csv folder
        log.info("Uploading graphs to Drive...")
        paths_to_upload = glob.glob(f"{output_dir}/*.png")
        for i, path in enumerate(paths_to_upload):
            log.info(f"Uploading graph {i + 1}/{len(paths_to_upload)}: {path}...")
            drive_client_wrapper.update_or_create(path, pipeline_configuration.drive_upload.analysis_graphs_dir,
                                                  target_folder_is_shared_with_me=True)
    else:
        log.info(
            "Skipping uploading to Google Drive (because the pipeline configuration json does not contain the key "
            "'DriveUploadPaths')")
