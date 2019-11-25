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

    # Create a list of rqa_raw_fields
    rqa_raw_fields = []
    for plan in PipelineConfiguration.RQA_CODING_PLANS:
        rqa_raw_fields.append(plan.raw_field)

    log.info(f'Computing unique, lifetime-active and per-show participants ...' )
    engagement_map = {}  # of uid -> name of shows participated in and their demographics data
    uuids = set()  # unique uids that participated in the entire project
    lifetime_active_uuids = []  # uids that participated in each radio show
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
            uuids.add(uid)
            lifetime_active_uuids.append(uid)

    # Export the engagement counts to their respective csv file.
    log.info(f'Writing unique participants csv ...')
    with open(f"{engagement_csv_output_dir}/unique_participants.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow([len(uuids)])

    log.info(f'Writing lifetime active participants csv ...')
    with open(f"{engagement_csv_output_dir}/lifetime_active_participants.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow([len(lifetime_active_uuids)])

    log.info(f'Writing show participation csv ...')
    with open(f"{engagement_csv_output_dir}/show_participation.csv", "w") as f:
        writer = csv.writer(f)

        for row in participants_per_show.items():
            writer.writerow(row)

    # For each uid generate a sustained engagement map that contains : no of shows participated , their manually
    # coded demographics and a matrix representation of the shows they have participated in
    # (1 for show they participated and 0 otherwise.)
    sustained_engagement_map = {}
    for uid in engagement_map.keys():
        basic_str = f" {len(engagement_map[uid]['shows'])}, {engagement_map[uid]['demog']['gender']}," \
            f" {engagement_map[uid]['demog']['age']}, {engagement_map[uid]['demog']['recently_displaced']}, "

        show_mapping = ""
        for show_name in rqa_raw_fields:
            if show_name in engagement_map[uid]["shows"]:
                show_mapping = show_mapping + "1, "
            else:
                show_mapping = show_mapping + "0, "

        compound_str = basic_str + show_mapping
        sustained_engagement_map[uid] = [compound_str]

    log.info(f'Computing exact repeat engagement / uuids ...' )
    # Compute the number of individuals who participated exactly 1 to <number of RQAs> times.
    # An individual is considered to have participated if they sent a message and didn't opt-out, regardless of the
    # relevance of any of their messages.
    exact_repeat_engagement = OrderedDict()
    for i in range(1, len(rqa_raw_fields) + 1):
        exact_repeat_engagement[i] = {
            "Repeat No": i,
            "No. of participants": 0,
            "% of participants": None
        }
        for k, v in sustained_engagement_map.items():
            for item in v:
                if item[1] == f'{i}':
                    exact_repeat_engagement[i]["No. of participants"] += 1

    # Compute the percentage of individuals who participated exactly 1 to <number of RQAs> times.
    for rp in exact_repeat_engagement.values():
        rp["% of participants"] = round(rp["No. of participants"] / len(uuids) * 100, 1)

    log.info(f'Writing exact_repeat_engagement csv ...' )
    # Write exact repeat engagement CSV
    with open(f"{engagement_csv_output_dir}/exact_repeat_engagement.csv", "w") as f:
        headers = ["Repeat No", "No. of participants", "% of participants"]
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        writer.writeheader()

        for row in exact_repeat_engagement.values():
            writer.writerow(row)
