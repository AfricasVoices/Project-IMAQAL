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
    engagement_map = {}  # of uid -> name of shows participated in and their demographics data.
    opted_in_participants= set()  # total of participants who sent a message and also opted in.
    opted_in_activations = []  # sum of total of every time consented participants interacts throughout a project.
    opted_in_uids_per_show = OrderedDict()  # of rqa_raw_field -> sum of total of every time consented participants interact in an episode.
    for rqa_raw_field in rqa_raw_fields:
        with open(f'{demog_map_json_input_dir}/{rqa_raw_field}_demog_map.json') as f:
            data = json.load(f)
        log.info(f"Loaded {len(data)} {rqa_raw_field} uids ")

        opted_in_uids_per_show[f"{rqa_raw_field}"] = len(data.keys())

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
            opted_in_participants.add(uid)
            opted_in_activations.append(uid)

    # Export the engagement counts to their respective csv file.
    log.info(f'Writing total participants with optins csv ...')
    with open(f"{engagement_csv_output_dir}/total_participants_with_optins.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow([len(opted_in_participants)])

    log.info(f'Writing total activations with opt-ins csv ...')
    with open(f"{engagement_csv_output_dir}/total_activations_with_optins.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow([len(opted_in_activations)])

    log.info(f'Writing uids_per_show_with_optins ...')
    with open(f"{engagement_csv_output_dir}/uids_per_show_with_optins.csv", "w") as f:
        writer = csv.writer(f)

        for row in opted_in_uids_per_show.items():
            writer.writerow(row)

    log.info(f'Computing new and repeat participation for TV episodes ...')
    # Computes the number of new and repeat consented individuals who participated in the TV_radio shows vis-à-vis
    # prior radio shows only.
    # Repeat participants are consented individuals who participated in previous shows prior to the target show.
    # New participants are consented individuals who participated in target show but din't participate in previous shows.
    # of rqa_raw_field to participation metrics.
    tv_radio_shows = ["rqa_s02mag23_raw", "rqa_s02mag24_raw", "rqa_s02mag25_raw",
                      "rqa_s02mag26_raw", "rqa_s02mag27_raw", "rqa_s02mag28_raw"]
    tv_radio_shows_uids = set()  # uids of individuals who participated in tv_radio shows.
    for rqa_raw_field in tv_radio_shows:
        with open(f'{demog_map_json_input_dir}/{rqa_raw_field}_demog_map.json') as f:
            tv_radio_shows_data = json.load(f)
            for uid in tv_radio_shows_data:
                tv_radio_shows_uids.add(uid)

    previous_radio_shows = [] # raw_fields of shows before the tv_radio shows
    for rqa_raw_field in rqa_raw_fields:
        if f"{rqa_raw_field}" == "rqa_s02mag23_raw":
            break
        previous_radio_shows.append(rqa_raw_field)

    previous_shows_uids = set()  # uids of individuals who participated in previous radio shows.
    for rqa_raw_field in previous_radio_shows:
        with open(f'{demog_map_json_input_dir}/{rqa_raw_field}_demog_map.json') as f:
            previous_radio_shows_data = json.load(f)
            for uid in previous_radio_shows_data:
                previous_shows_uids.add(uid)

    tv_radio_repeat_uids = set()  # uids of individuals who participated in tv_radio and previous shows.
    tv_radio_new_uids = set()  # uids of individuals who participated in tv_radio show but din't participate in previous shows.
    for uid in tv_radio_shows_uids:
        if uid in previous_shows_uids:
            tv_radio_repeat_uids.add(uid)
        else:
            tv_radio_new_uids.add(uid)

    tv_radio_repeat_new_participation_map = {"No. of previous shows participants": len(previous_shows_uids),
                                             "No. of tv_radio shows participants": len(tv_radio_shows_uids),
                                             "No. of repeat participants":len(tv_radio_repeat_uids),
                                             "No. of new participants":len(tv_radio_new_uids)}

    log.info(f'Writing tv_radio repeat and new participation participation csv ...')
    with open(f"{engagement_csv_output_dir}/tv_radio_show_repeat_and_new_participation.csv", "w") as f:
        headers = ["No. of previous shows participants", "No. of tv_radio shows participants",
                   "No. of repeat participants", "No. of new participants"]
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        writer.writeheader()

        for data in [tv_radio_repeat_new_participation_map]:
            writer.writerow(data)
