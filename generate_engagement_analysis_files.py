from collections import OrderedDict
import argparse
import json
import csv

from core_data_modules.logging import Logger

from src.lib import PipelineConfiguration

Logger.set_project_name("IMAQAL")
log = Logger(__name__)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generates engagement data stored in multiple CSV sheets for analysis"
                                                 " and visualization")

    parser.add_argument("user", help="User launching this program")
    parser.add_argument("pipeline_configuration_file_path", metavar="pipeline-configuration-file",
                        help="Path to the pipeline configuration json file"),
    parser.add_argument("demog_map_json_input_dir", metavar="demog-map-json-input-dir",
                        help="Path to a directory to read per episode demog map .json files ")
    parser.add_argument("engagement_csv_output_dir", metavar="engagement-csv-output-dir",
                        help="Directory to write engagement CSV files to")


    args = parser.parse_args()

    user = args.user
    pipeline_configuration_file_path = args.pipeline_configuration_file_path
    demog_map_json_input_dir = args.demog_map_json_input_dir
    engagement_csv_output_dir = args.engagement_csv_output_dir

    engagement_map = {} # Participants shows and demogs data
    all_show_names = [] # All project show names
    participants_per_show = OrderedDict()
    unique_participants = set()
    cumulative_participants = []

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

    log.info(f'Generating engagement map, unique participants and cumulative participants ...' )
    for rqa_raw_field in rqa_raw_fields:
        with open(f'{demog_map_json_input_dir}/{rqa_raw_field}_demog_map.json') as f:
            data = json.load(f)
        log.info(f"Loaded {len(data)} {rqa_raw_field} uids ")

        if rqa_raw_field not in all_show_names: # Not using a set to preserve the sort order of the file names
            all_show_names.append(rqa_raw_field)
        participants_per_show[f"{rqa_raw_field}"] = len(data.keys())

        for uid in data.keys():
            demog = data[uid]

            if uid not in engagement_map:
                engagement_map[uid] = {}
                engagement_map[uid]["demog"] = demog
                engagement_map[uid]["shows"] = []

            engagement_map[uid]["shows"].append(rqa_raw_field)
            unique_participants.add(uid)
            cumulative_participants.append(uid)

    # Export the engagement counts to their respective csv file.
    log.info(f'Writing Total Unique Participants Csv ...')
    with open(f"{engagement_csv_output_dir}/unique_participants.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow([len(unique_participants)])

    log.info(f'Writing Total Cumulative Participants Csv ...')
    with open(f"{engagement_csv_output_dir}/cumulative_participants.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow([len(cumulative_participants)])

    log.info(f'Writing participants per show CSV ...')
    with open(f"{engagement_csv_output_dir}/participants_per_show.csv", "w") as f:
        writer = csv.writer(f,)
        writer.writerows(participants_per_show.items())

    # For each uid generate a sustained engagement map that contains : no of shows participated , their manually
    # coded demographics and a matrix representation of the shows they have participated in
    # (1 for show they participated and 0 otherwise.)
    sustained_engagement_map = {}
    for uid in engagement_map.keys():
        basic_str = f" {len(engagement_map[uid]['shows'])}, {engagement_map[uid]['demog']['gender']}," \
            f" {engagement_map[uid]['demog']['age']}, {engagement_map[uid]['demog']['recently_displaced']}, "

        show_mapping = ""
        for show_name in all_show_names:
            if show_name in engagement_map[uid]["shows"]:
                show_mapping = show_mapping + "1, "
            else:
                show_mapping = show_mapping + "0, "

        compound_str = basic_str + show_mapping
        sustained_engagement_map[uid] = [compound_str]

    log.info(f'Computing repeat_non_cumulative_engagement ...' )
    # Compute the number of individuals who participated exactly 1 to <number of RQAs> times.
    # An individual is considered to have participated if they sent a message and didn't opt-out, regardless of the
    # relevance of any of their messages.
    repeat_non_cumulative_engagement = OrderedDict()
    for i in range(1, len(all_show_names) + 1):
        repeat_non_cumulative_engagement[i] = {
            "Repeat No": i,
            "No. of participants": 0,
            "% of participants": None
        }
        for k, v in sustained_engagement_map.items():
            for item in v:
                if item[1] == f'{i}':
                    repeat_non_cumulative_engagement[i]["No. of participants"] += 1

    # Compute the percentage of individuals who participated exactly 1 to <number of RQAs> times.
    for rp in repeat_non_cumulative_engagement.values():
        rp["% of participants"] = round(rp["No. of participants"] / len(cumulative_participants) * 100, 1)

    log.info(f'Writing repeat_non_cumulative_engagement csv ...' )
    # Write repeat non cumulative engagement in the engagement CSV
    with open(f"{engagement_csv_output_dir}/rp_non_cumulative_engagement.csv", "w") as f:
        headers = ["Repeat No", "No. of participants", "% of participants"]
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        writer.writeheader()

        for row in repeat_non_cumulative_engagement.values():
            writer.writerow(row)
