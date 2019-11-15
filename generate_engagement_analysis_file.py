from collections import OrderedDict
import argparse
import json
import xlsxwriter

from core_data_modules.logging import Logger

from src.lib import PipelineConfiguration

Logger.set_project_name("IMAQAL")
log = Logger(__name__)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generates engagement data stored in an excel workbook for analysis"
                                                 " and visualization")

    parser.add_argument("user", help="User launching this program")
    parser.add_argument("demog_map_json_input_dir", metavar="demog-map-json-input-dir",
                        help="Path to a directory to read per episode demog map .json files ")
    parser.add_argument("engagement_csv_output_dir", metavar="engagement-csv-output-dir",
                        help="Directory to write engagement .xlsx files to")


    args = parser.parse_args()

    user = args.user
    demog_map_json_input_dir = args.demog_map_json_input_dir
    engagement_csv_output_dir = args.engagement_csv_output_dir

    engagement_map = {} # Participants shows and demogs data
    all_show_names = [] # All project show names
    participants_per_show = OrderedDict()
    unique_participants = set()
    cumulative_participants = []

    # Create a list of rqa_raw_fields
    rqa_raw_fields = []
    for plan in PipelineConfiguration.RQA_CODING_PLANS:
        rqa_raw_fields.append(plan.raw_field)

    log.info(f'Generating engagement map,unique participants and cumulative participants ...' )
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

    log.info(f'Writing participation work sheet ...')
    # Write participants per show in the engagement excel work-book under uids_per_show sheet
    # Create engagement workbook
    engagement_workbook = xlsxwriter.Workbook(f'{engagement_csv_output_dir}/engagement.xlsx')
    bold_headers = engagement_workbook.add_format({'bold': True})
    participation_ws = engagement_workbook.add_worksheet('participation')
    participation_ws.write('A1', 'No. of Unique participants', bold_headers)
    participation_ws.write('B1', 'No. of Cumulative participants', bold_headers)
    row = 1
    col = 0
    participation_ws.write(row, col, len(unique_participants))
    participation_ws.write(row, col + 1, len(cumulative_participants))

    log.info(f'Writing participants_per_show work sheet ...' )
    # Write participants per show in the engagement excel work-book under uids_per_show sheet
    participants_per_show_ws = engagement_workbook.add_worksheet('participants_per_show')
    participants_per_show_ws.write('A1', 'Episode', bold_headers)
    participants_per_show_ws.write('B1', 'No of Participants', bold_headers)
    row = 1
    col = 0
    for show, participants in participants_per_show.items():
        participants_per_show_ws.write(row, col, show)
        participants_per_show_ws.write(row, col + 1, participants)
        row += 1

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

    log.info(f'Computing repeat_non_cumulative_engagement work sheet ...' )
    # Compute the number of individuals who participated exactly 1 to <number of RQAs> times.
    # An individual is considered to have participated if they sent a message and didn't opt-out, regardless of the
    # relevance of any of their messages.
    repeat_non_cumulative_engagement = OrderedDict()
    for i in range(1, len(all_show_names) + 1):
        repeat_non_cumulative_engagement[i] = {
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

    log.info(f'Writing repeat_non_cumulative_engagement work sheet ...' )
    # Write repeat non cumulative engagement in the engagement excel work-book under uids_per_show sheet
    repeat_non_cumulative_engagement_ws = engagement_workbook.add_worksheet('rp_non_cumulative_engagement')
    repeat_non_cumulative_engagement_ws.write('A1', 'No. of participants', bold_headers)
    repeat_non_cumulative_engagement_ws.write('B1', '% of participants', bold_headers)
    row = 1
    col = 0
    for k, v in repeat_non_cumulative_engagement.items():
        repeat_non_cumulative_engagement_ws.write(row, col, v['No. of participants'])
        repeat_non_cumulative_engagement_ws.write(row, col + 1, v['% of participants'])
        row += 1

    # Close workbook
    engagement_workbook.close()
