import json
from urllib.parse import urlparse

from core_data_modules.cleaners import somali, Codes
from core_data_modules.data_models import Scheme, validators
from dateutil.parser import isoparse


def _open_scheme(filename):
    with open(f"code_schemes/{filename}", "r") as f:
        firebase_map = json.load(f)
        return Scheme.from_firebase_map(firebase_map)


class CodeSchemes(object):
    S01E01 = _open_scheme("s01e01.json")
    S01E02 = _open_scheme("s01e02.json")
    S01E02_YES_NO_AMB = _open_scheme("s01e02_yes_no_amb.json")
    S01E03 = _open_scheme("s01e03.json")
    S01E03_YES_NO_AMB = _open_scheme("s01e03_yes_no_amb.json")
    S01E04 = _open_scheme("s01e04.json")
    S01E05 = _open_scheme("s01e05.json")
    S01E05_YES_NO_AMB = _open_scheme("s01e05_yes_no_amb.json")
    S01E06 = _open_scheme("s01e06.json")
    S01E07 = _open_scheme("s01e07.json")
    S01E07_YES_NO_AMB = _open_scheme("s01e07_yes_no_amb.json")
    S01MAG03 = _open_scheme("s01mag03.json")
    S01MAG03_YES_NO_AMB = _open_scheme("s01mag03_yes_no_amb.json")
    S01MAG04 = _open_scheme("s01mag04.json")
    S01MAG05 = _open_scheme("s01mag05.json")
    S01MAG05_YES_NO_AMB = _open_scheme("s01mag05_yes_no_amb.json")
    S01MAG06 = _open_scheme("s01mag06.json")
    S01MAG06_YES_NO_AMB = _open_scheme("s01mag06_yes_no_amb.json")
    S01MAG07 = _open_scheme("s01mag07.json")
    S01MAG07_YES_NO_AMB = _open_scheme("s01mag07_yes_no_amb.json")
    S01MAG08 = _open_scheme("s01mag08.json")
    S01MAG09 = _open_scheme("s01mag09.json")
    S01MAG09_YES_NO_AMB = _open_scheme("s01mag09_yes_no_amb.json")
    # TODO Update this once data structure doc is updated.
    S01MAG10 = _open_scheme("s01mag10.json")
    S01MAG11 = _open_scheme("s01mag11.json")
    S01MAG12 = _open_scheme("s01mag12.json")
    S01MAG13 = _open_scheme("s01mag13.json")
    S01MAG14 = _open_scheme("s01mag14.json")
    S01MAG15 = _open_scheme("s01mag15.json")
    S01MAG16 = _open_scheme("s01mag16.json")
    S01MAG17 = _open_scheme("s01mag17.json")
    S01MAG18 = _open_scheme("s01mag18.json")
    S01MAG19 = _open_scheme("s01mag19.json")
    S01MAG20 = _open_scheme("s01mag20.json")

    WOMEN_PARTICIPATION = _open_scheme("women_participation.json")
    WOMEN_PARTICIPATION_YES_NO_AMB = _open_scheme("women_participation_yes_no_amb.json")
    MINORITY_CLAN_ISSUES = _open_scheme("minority_clan_issues.json")
    MINORITY_CLAN_ISSUES_YES_NO_AMB = _open_scheme("minority_clan_issues_yes_no_amb.json")
    YOUNG_PEOPLE_ISSUES = _open_scheme("young_people_issues.json")
    YOUNG_PEOPLE_ISSUES_YES_NO_AMB = _open_scheme("young_people_issues_yes_no_amb.json")

    AGE = _open_scheme("age.json")
    RECENTLY_DISPLACED = _open_scheme("recently_displaced.json")
    HOUSEHOLD_LANGUAGE = _open_scheme("household_language.json")
    GENDER = _open_scheme("gender.json")

    SOMALIA_OPERATOR = _open_scheme("somalia_operator.json")
    SOMALIA_DISTRICT = _open_scheme("somalia_district.json")
    MOGADISHU_SUB_DISTRICT = _open_scheme("mogadishu_sub_district.json")
    SOMALIA_REGION = _open_scheme("somalia_region.json")
    SOMALIA_STATE = _open_scheme("somalia_state.json")
    SOMALIA_ZONE = _open_scheme("somalia_zone.json")

    WS_CORRECT_DATASET = _open_scheme("ws_correct_dataset.json")


class CodingPlan(object):
    def __init__(self, raw_field, coded_field, coda_filename, cleaner=None, code_scheme=None, time_field=None,
                 run_id_field=None, icr_filename=None, analysis_file_key=None, id_field=None, ws_code=None,
                 binary_code_scheme=None, binary_coded_field=None, binary_analysis_file_key=None):
        self.raw_field = raw_field
        self.coded_field = coded_field
        self.coda_filename = coda_filename
        self.icr_filename = icr_filename
        self.cleaner = cleaner
        self.code_scheme = code_scheme
        self.time_field = time_field
        self.run_id_field = run_id_field
        self.ws_code = ws_code
        self.analysis_file_key = analysis_file_key
        self.binary_code_scheme = binary_code_scheme
        self.binary_coded_field = binary_coded_field
        self.binary_analysis_file_key = binary_analysis_file_key

        if id_field is None:
            id_field = "{}_id".format(self.raw_field)
        self.id_field = id_field


class PipelineConfiguration(object):
    DEV_MODE = False

    SUBSAMPLING_THRESHOLD = 4  # /16 (a fraction of hex) subsample of data

    PROJECT_START_DATE = isoparse("2019-04-19T00:00:00+03:00")
    # TODO revise this as the project nears the end
    PROJECT_END_DATE = isoparse("2020-02-15T09:00:00+03:00")

    RQA_CODING_PLANS = None

    FULL_PIPELINE_RQA_CODING_PLANS = [
        CodingPlan(raw_field="rqa_s01e01_raw",
                   coded_field="rqa_s01e01_coded",
                   time_field="sent_on",
                   coda_filename="s01e01.json",
                   icr_filename="s01e01.csv",
                   run_id_field="rqa_s01e01_run_id",
                   analysis_file_key="rqa_s01e01_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01e01"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01E01),

        CodingPlan(raw_field="rqa_s01e02_raw",
                   coded_field="rqa_s01e02_coded",
                   time_field="sent_on",
                   coda_filename="s01e02.json",
                   icr_filename="s01e02.csv",
                   run_id_field="rqa_s01e02_run_id",
                   analysis_file_key="rqa_s01e02_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01e02"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01E02,
                   binary_code_scheme=CodeSchemes.S01E02_YES_NO_AMB,
                   binary_coded_field="rqa_s01e02_yes_no_amb_coded",
                   binary_analysis_file_key="rqa_s01e02_yes_no_amb"),

        CodingPlan(raw_field="rqa_s01e03_raw",
                   coded_field="rqa_s01e03_coded",
                   time_field="sent_on",
                   coda_filename="s01e03.json",
                   icr_filename="s01e03.csv",
                   run_id_field="rqa_s01e03_run_id",
                   analysis_file_key="rqa_s01e03_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01e03"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01E03,
                   binary_code_scheme=CodeSchemes.S01E03_YES_NO_AMB,
                   binary_coded_field="rqa_s01e03_yes_no_amb_coded",
                   binary_analysis_file_key="rqa_s01e03_yes_no_amb"),

        CodingPlan(raw_field="rqa_s01e04_raw",
                   coded_field="rqa_s01e04_coded",
                   time_field="sent_on",
                   coda_filename="s01e04.json",
                   icr_filename="s01e04.csv",
                   run_id_field="rqa_s01e04_run_id",
                   analysis_file_key="rqa_s01e04_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01e04"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01E04),

        CodingPlan(raw_field="rqa_s01e05_raw",
                   coded_field="rqa_s01e05_coded",
                   time_field="sent_on",
                   coda_filename="s01e05.json",
                   icr_filename="s01e05.csv",
                   run_id_field="rqa_s01e05_run_id",
                   analysis_file_key="rqa_s01e05_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01e05"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01E05,
                   binary_code_scheme=CodeSchemes.S01E05_YES_NO_AMB,
                   binary_coded_field="rqa_s01e05_yes_no_amb_coded",
                   binary_analysis_file_key="rqa_s01e05_yes_no_amb"),

        CodingPlan(raw_field="rqa_s01e06_raw",
                   coded_field="rqa_s01e06_coded",
                   time_field="sent_on",
                   coda_filename="s01e06.json",
                   icr_filename="s01e06.csv",
                   run_id_field="rqa_s01e06_run_id",
                   analysis_file_key="rqa_s01e06_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01e06"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01E06),

        CodingPlan(raw_field="rqa_s01e07_raw",
                   coded_field="rqa_s01e07_coded",
                   time_field="sent_on",
                   coda_filename="s01e07.json",
                   icr_filename="s01e07.csv",
                   run_id_field="rqa_s01e07_run_id",
                   analysis_file_key="rqa_s01e07_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01e07"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01E07,
                   binary_code_scheme=CodeSchemes.S01E07_YES_NO_AMB,
                   binary_coded_field="rqa_s01e07_yes_no_amb_coded",
                   binary_analysis_file_key="rqa_s01e07_yes_no_amb"),

        CodingPlan(raw_field="rqa_s01mag03_raw",
                   coded_field="rqa_s01mag03_coded",
                   time_field="sent_on",
                   coda_filename="s01mag03.json",
                   icr_filename="s01mag03.csv",
                   run_id_field="rqa_s01mag03_run_id",
                   analysis_file_key="rqa_s01mag03_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag03"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01MAG03,
                   binary_code_scheme=CodeSchemes.S01MAG03_YES_NO_AMB,
                   binary_coded_field="rqa_s01mag03_yes_no_amb_coded",
                   binary_analysis_file_key="rqa_s01mag03_yes_no_amb"),

        CodingPlan(raw_field="rqa_s01mag04_raw",
                   coded_field="rqa_s01mag04_coded",
                   time_field="sent_on",
                   coda_filename="s01mag04.json",
                   icr_filename="s01mag04.csv",
                   run_id_field="rqa_s01mag04_run_id",
                   analysis_file_key="rqa_s01mag04_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag04"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01MAG04),

        CodingPlan(raw_field="rqa_s01mag05_raw",
                   coded_field="rqa_s01mag05_coded",
                   time_field="sent_on",
                   coda_filename="s01mag05.json",
                   icr_filename="s01mag05.csv",
                   run_id_field="rqa_s01mag05_run_id",
                   analysis_file_key="rqa_s01mag05_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag05"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01MAG05,
                   binary_code_scheme=CodeSchemes.S01MAG05_YES_NO_AMB,
                   binary_coded_field="rqa_s01mag05_yes_no_amb_coded",
                   binary_analysis_file_key="rqa_s01mag05_yes_no_amb"),

        CodingPlan(raw_field="rqa_s01mag06_raw",
                   coded_field="rqa_s01mag06_coded",
                   time_field="sent_on",
                   coda_filename="s01mag06.json",
                   icr_filename="s01mag06.csv",
                   run_id_field="rqa_s01mag06_run_id",
                   analysis_file_key="rqa_s01mag06_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag06"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01MAG06,
                   binary_code_scheme=CodeSchemes.S01MAG06_YES_NO_AMB,
                   binary_coded_field="rqa_s01mag06_yes_no_amb_coded",
                   binary_analysis_file_key="rqa_s01mag06_yes_no_amb"),

        CodingPlan(raw_field="rqa_s01mag07_raw",
                   coded_field="rqa_s01mag07_coded",
                   time_field="sent_on",
                   coda_filename="s01mag07.json",
                   icr_filename="s01mag07.csv",
                   run_id_field="rqa_s01mag07_run_id",
                   analysis_file_key="rqa_s01mag07_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag07"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01MAG07,
                   binary_code_scheme=CodeSchemes.S01MAG07_YES_NO_AMB,
                   binary_coded_field="rqa_s01mag07_yes_no_amb_coded",
                   binary_analysis_file_key="rqa_s01mag07_yes_no_amb"),

        CodingPlan(raw_field="rqa_s01mag08_raw",
                   coded_field="rqa_s01mag08_coded",
                   time_field="sent_on",
                   coda_filename="s01mag08.json",
                   icr_filename="s01mag08.csv",
                   run_id_field="rqa_s01mag08_run_id",
                   analysis_file_key="rqa_s01mag08_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag08"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01MAG08),

        CodingPlan(raw_field="rqa_s01mag09_raw",
                   coded_field="rqa_s01mag09_coded",
                   time_field="sent_on",
                   coda_filename="s01mag09.json",
                   icr_filename="s01mag09.csv",
                   run_id_field="rqa_s01mag09_run_id",
                   analysis_file_key="rqa_s01mag09_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag09"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01MAG09,
                   binary_code_scheme=CodeSchemes.S01MAG09_YES_NO_AMB,
                   binary_coded_field="rqa_s01mag09_yes_no_amb_coded",
                   binary_analysis_file_key="rqa_s01mag09_yes_no_amb"),

        CodingPlan(raw_field="rqa_s01mag10_raw",
                   coded_field="rqa_s01mag10_coded",
                   time_field="sent_on",
                   coda_filename="s01mag10.json",
                   icr_filename="s01mag10.csv",
                   run_id_field="rqa_s01mag10_run_id",
                   analysis_file_key="rqa_s01mag10_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag10"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01MAG10),

        CodingPlan(raw_field="rqa_s01mag11_raw",
                   coded_field="rqa_s01mag11_coded",
                   time_field="sent_on",
                   coda_filename="s01mag11.json",
                   icr_filename="s01mag11.csv",
                   run_id_field="rqa_s01mag11_run_id",
                   analysis_file_key="rqa_s01mag11_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag11"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01MAG11),

        CodingPlan(raw_field="rqa_s01mag12_raw",
                   coded_field="rqa_s01mag12_coded",
                   time_field="sent_on",
                   coda_filename="s01mag12.json",
                   icr_filename="s01mag12.csv",
                   run_id_field="rqa_s01mag12_run_id",
                   analysis_file_key="rqa_s01mag12_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag12"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01MAG12),

        CodingPlan(raw_field="rqa_s01mag13_raw",
                   coded_field="rqa_s01mag13_coded",
                   time_field="sent_on",
                   coda_filename="s01mag13.json",
                   icr_filename="s01mag13.csv",
                   run_id_field="rqa_s01mag13_run_id",
                   analysis_file_key="rqa_s01mag13_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag13"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01MAG13),

        CodingPlan(raw_field="rqa_s01mag14_raw",
                   coded_field="rqa_s01mag14_coded",
                   time_field="sent_on",
                   coda_filename="s01mag14.json",
                   icr_filename="s01mag14.csv",
                   run_id_field="rqa_s01mag14_run_id",
                   analysis_file_key="rqa_s01mag14_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag14"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01MAG14),

        CodingPlan(raw_field="rqa_s01mag15_raw",
                   coded_field="rqa_s01mag15_coded",
                   time_field="sent_on",
                   coda_filename="s01mag15.json",
                   icr_filename="s01mag15.csv",
                   run_id_field="rqa_s01mag15_run_id",
                   analysis_file_key="rqa_s01mag15_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag15"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01MAG15),

        CodingPlan(raw_field="rqa_s01mag16_raw",
                   coded_field="rqa_s01mag16_coded",
                   time_field="sent_on",
                   coda_filename="s01mag16.json",
                   icr_filename="s01mag16.csv",
                   run_id_field="rqa_s01mag16_run_id",
                   analysis_file_key="rqa_s01mag16_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag16"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01MAG16),

        CodingPlan(raw_field="rqa_s01mag17_raw",
                   coded_field="rqa_s01mag17_coded",
                   time_field="sent_on",
                   coda_filename="s01mag17.json",
                   icr_filename="s01mag17.csv",
                   run_id_field="rqa_s01mag17_run_id",
                   analysis_file_key="rqa_s01mag17_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag17"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01MAG17),

        CodingPlan(raw_field="rqa_s01mag18_raw",
                   coded_field="rqa_s01mag18_coded",
                   time_field="sent_on",
                   coda_filename="s01mag18.json",
                   icr_filename="s01mag18.csv",
                   run_id_field="rqa_s01mag18_run_id",
                   analysis_file_key="rqa_s01mag18_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag18"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01MAG18),

        CodingPlan(raw_field="rqa_s01mag19_raw",
                   coded_field="rqa_s01mag19_coded",
                   time_field="sent_on",
                   coda_filename="s01mag19.json",
                   icr_filename="s01mag19.csv",
                   run_id_field="rqa_s01mag19_run_id",
                   analysis_file_key="rqa_s01mag19_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag19"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01MAG19),

        CodingPlan(raw_field="rqa_s01mag20_raw",
                   coded_field="rqa_s01mag20_coded",
                   time_field="sent_on",
                   coda_filename="s01mag20.json",
                   icr_filename="s01mag20.csv",
                   run_id_field="rqa_s01mag20_run_id",
                   analysis_file_key="rqa_s01mag20_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag20"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01MAG20)
    ]

    Q4_RQA_CODING_PLANS =[
        CodingPlan(raw_field="rqa_s01mag08_raw",
                   coded_field="rqa_s01mag08_coded",
                   time_field="sent_on",
                   coda_filename="s01mag08.json",
                   icr_filename="s01mag08.csv",
                   run_id_field="rqa_s01mag08_run_id",
                   analysis_file_key="rqa_s01mag08_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag08"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01MAG08),

        CodingPlan(raw_field="rqa_s01mag09_raw",
                   coded_field="rqa_s01mag09_coded",
                   time_field="sent_on",
                   coda_filename="s01mag09.json",
                   icr_filename="s01mag09.csv",
                   run_id_field="rqa_s01mag09_run_id",
                   analysis_file_key="rqa_s01mag09_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag09"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01MAG09,
                   binary_code_scheme=CodeSchemes.S01MAG09_YES_NO_AMB,
                   binary_coded_field="rqa_s01mag09_yes_no_amb_coded",
                   binary_analysis_file_key="rqa_s01mag09_yes_no_amb"),

        CodingPlan(raw_field="rqa_s01mag10_raw",
                   coded_field="rqa_s01mag10_coded",
                   time_field="sent_on",
                   coda_filename="s01mag10.json",
                   icr_filename="s01mag10.csv",
                   run_id_field="rqa_s01mag10_run_id",
                   analysis_file_key="rqa_s01mag10_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag10"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01MAG10),

        CodingPlan(raw_field="rqa_s01mag11_raw",
                   coded_field="rqa_s01mag11_coded",
                   time_field="sent_on",
                   coda_filename="s01mag11.json",
                   icr_filename="s01mag11.csv",
                   run_id_field="rqa_s01mag11_run_id",
                   analysis_file_key="rqa_s01mag11_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag11"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01MAG11),

        CodingPlan(raw_field="rqa_s01mag12_raw",
                   coded_field="rqa_s01mag12_coded",
                   time_field="sent_on",
                   coda_filename="s01mag12.json",
                   icr_filename="s01mag12.csv",
                   run_id_field="rqa_s01mag12_run_id",
                   analysis_file_key="rqa_s01mag12_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag12"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01MAG12),

        CodingPlan(raw_field="rqa_s01mag13_raw",
                   coded_field="rqa_s01mag13_coded",
                   time_field="sent_on",
                   coda_filename="s01mag13.json",
                   icr_filename="s01mag13.csv",
                   run_id_field="rqa_s01mag13_run_id",
                   analysis_file_key="rqa_s01mag13_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag13"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01MAG13)
    ]

    Q5_RQA_CODING_PLANS = [
        CodingPlan(raw_field="rqa_s01mag14_raw",
                   coded_field="rqa_s01mag14_coded",
                   time_field="sent_on",
                   coda_filename="s01mag14.json",
                   icr_filename="s01mag14.csv",
                   run_id_field="rqa_s01mag14_run_id",
                   analysis_file_key="rqa_s01mag14_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag14"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01MAG14),

        CodingPlan(raw_field="rqa_s01mag15_raw",
                   coded_field="rqa_s01mag15_coded",
                   time_field="sent_on",
                   coda_filename="s01mag15.json",
                   icr_filename="s01mag15.csv",
                   run_id_field="rqa_s01mag15_run_id",
                   analysis_file_key="rqa_s01mag15_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag15"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01MAG15),

        CodingPlan(raw_field="rqa_s01mag16_raw",
                   coded_field="rqa_s01mag16_coded",
                   time_field="sent_on",
                   coda_filename="s01mag16.json",
                   icr_filename="s01mag16.csv",
                   run_id_field="rqa_s01mag16_run_id",
                   analysis_file_key="rqa_s01mag16_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag16"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01MAG16),

        CodingPlan(raw_field="rqa_s01mag17_raw",
                   coded_field="rqa_s01mag17_coded",
                   time_field="sent_on",
                   coda_filename="s01mag17.json",
                   icr_filename="s01mag17.csv",
                   run_id_field="rqa_s01mag17_run_id",
                   analysis_file_key="rqa_s01mag17_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag17"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01MAG17),

        CodingPlan(raw_field="rqa_s01mag18_raw",
                   coded_field="rqa_s01mag18_coded",
                   time_field="sent_on",
                   coda_filename="s01mag18.json",
                   icr_filename="s01mag18.csv",
                   run_id_field="rqa_s01mag18_run_id",
                   analysis_file_key="rqa_s01mag18_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag18"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01MAG18),

        CodingPlan(raw_field="rqa_s01mag19_raw",
                   coded_field="rqa_s01mag19_coded",
                   time_field="sent_on",
                   coda_filename="s01mag19.json",
                   icr_filename="s01mag19.csv",
                   run_id_field="rqa_s01mag19_run_id",
                   analysis_file_key="rqa_s01mag19_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag19"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01MAG19),

        CodingPlan(raw_field="rqa_s01mag20_raw",
                   coded_field="rqa_s01mag20_coded",
                   time_field="sent_on",
                   coda_filename="s01mag20.json",
                   icr_filename="s01mag20.csv",
                   run_id_field="rqa_s01mag20_run_id",
                   analysis_file_key="rqa_s01mag20_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag20"),
                   cleaner=None,
                   code_scheme=CodeSchemes.S01MAG20)
    ]

    FOLLOW_UP_CODING_PLANS = [

        CodingPlan(raw_field="women_participation_raw",
                   coded_field="women_participation_coded",
                   time_field="sent_on",
                   coda_filename="women_participation.json",
                   icr_filename="women_participation.csv",
                   run_id_field="women_participation_run_id",
                   analysis_file_key="women_participation_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("women_participation"),
                   cleaner=None,
                   code_scheme=CodeSchemes.WOMEN_PARTICIPATION,
                   binary_code_scheme=CodeSchemes.WOMEN_PARTICIPATION_YES_NO_AMB,
                   binary_coded_field="women_participation_yes_no_amb_coded",
                   binary_analysis_file_key="women_participation_yes_no_amb"),

        CodingPlan(raw_field="minority_clan_issues_raw",
                   coded_field="minority_clan_issues_coded",
                   time_field="sent_on",
                   coda_filename="minority_clan_issues.json",
                   icr_filename="minority_clan_issues.csv",
                   run_id_field="minority_clan_issues_run_id",
                   analysis_file_key="minority_clan_issues_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("minority_clan_issues"),
                   cleaner=None,
                   code_scheme=CodeSchemes.MINORITY_CLAN_ISSUES,
                   binary_code_scheme=CodeSchemes.MINORITY_CLAN_ISSUES_YES_NO_AMB,
                   binary_coded_field="minority_clan_issues_yes_no_amb_coded",
                   binary_analysis_file_key="minority_clan_issues_yes_no_amb"),

        CodingPlan(raw_field="young_people_issues_raw",
                   coded_field="young_people_issues_coded",
                   time_field="sent_on",
                   coda_filename="young_people_issues.json",
                   icr_filename="young_people_issues.csv",
                   run_id_field="young_people_issues_run_id",
                   analysis_file_key="young_people_issues_",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("young_people_issues"),
                   cleaner=None,
                   code_scheme=CodeSchemes.YOUNG_PEOPLE_ISSUES,
                   binary_code_scheme=CodeSchemes.YOUNG_PEOPLE_ISSUES_YES_NO_AMB,
                   binary_coded_field="young_people_issues_yes_no_amb_coded",
                   binary_analysis_file_key="young_people_issues_yes_no_amb"),
    ]

    LOCATION_CODING_PLANS = [

        CodingPlan(raw_field="location_raw",
                   id_field="location_raw_id",
                   coded_field="mogadishu_sub_district_coded",
                   time_field="location_time",
                   coda_filename="location.json",
                   analysis_file_key=None,
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("location"),
                   cleaner=None,
                   code_scheme=CodeSchemes.MOGADISHU_SUB_DISTRICT),

        CodingPlan(raw_field="location_raw",
                   id_field="location_raw_id",
                   coded_field="district_coded",
                   time_field="location_time",
                   coda_filename="location.json",
                   analysis_file_key="district",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("location"),
                   cleaner=somali.DemographicCleaner.clean_somalia_district,
                   code_scheme=CodeSchemes.SOMALIA_DISTRICT),

        CodingPlan(raw_field="location_raw",
                   id_field="location_raw_id",
                   coded_field="region_coded",
                   time_field="location_time",
                   coda_filename="location.json",
                   analysis_file_key="region",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("location"),
                   cleaner=None,
                   code_scheme=CodeSchemes.SOMALIA_REGION),

        CodingPlan(raw_field="location_raw",
                   id_field="location_raw_id",
                   coded_field="state_coded",
                   time_field="location_time",
                   coda_filename="location.json",
                   analysis_file_key="state",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("location"),
                   cleaner=None,
                   code_scheme=CodeSchemes.SOMALIA_STATE),

        CodingPlan(raw_field="location_raw",
                   id_field="location_raw_id",
                   coded_field="zone_coded",
                   time_field="location_time",
                   coda_filename="location.json",
                   analysis_file_key="zone",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("location"),
                   cleaner=None,
                   code_scheme=CodeSchemes.SOMALIA_ZONE),
    ]

    @staticmethod
    def clean_age_with_range_filter(text):
        """
        Cleans age from the given `text`, setting to NC if the cleaned age is not in the range 10 <= age < 100.
        """
        age = somali.DemographicCleaner.clean_age(text)
        if type(age) == int and 10 <= age < 100:
            return str(age)
            # TODO: Once the cleaners are updated to not return Codes.NOT_CODED, this should be updated to still return
            #       NC in the case where age is an int but is out of range
        else:
            return Codes.NOT_CODED

    DEMOG_CODING_PLANS = [
        CodingPlan(raw_field="gender_raw",
                   coded_field="gender_coded",
                   time_field="gender_time",
                   coda_filename="gender.json",
                   analysis_file_key="gender",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("gender"),
                   cleaner=somali.DemographicCleaner.clean_gender,
                   code_scheme=CodeSchemes.GENDER),

        CodingPlan(raw_field="age_raw",
                   coded_field="age_coded",
                   time_field="age_time",
                   coda_filename="age.json",
                   analysis_file_key="age",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("age"),
                   cleaner=lambda text: PipelineConfiguration.clean_age_with_range_filter(text),
                   code_scheme=CodeSchemes.AGE),

        CodingPlan(raw_field="recently_displaced_raw",
                   coded_field="recently_displaced_coded",
                   time_field="recently_displaced_time",
                   coda_filename="recently_displaced.json",
                   analysis_file_key="recently_displaced",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("recently displaced"),
                   cleaner=somali.DemographicCleaner.clean_yes_no,
                   code_scheme=CodeSchemes.RECENTLY_DISPLACED),

        CodingPlan(raw_field="household_language_raw",
                   coded_field="household_language_coded",
                   time_field="household_language_time",
                   coda_filename="household_language.json",
                   analysis_file_key="household_language",
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("hh language"),
                   cleaner=None,
                   code_scheme=CodeSchemes.HOUSEHOLD_LANGUAGE),
    ]

    DEMOG_CODING_PLANS.extend(LOCATION_CODING_PLANS)

    def __init__(self, rapid_pro_domain, rapid_pro_token_file_url, activation_flow_names, demog_flow_names,
                 follow_up_flow_names, rapid_pro_test_contact_uuids, phone_number_uuid_table, rapid_pro_key_remappings,
                 memory_profile_upload_url_prefix, move_ws_messages, generate_analysis_files, recovery_csv_urls=None, pipeline_name=None, drive_upload=None):
        """
        :param rapid_pro_domain: URL of the Rapid Pro server to download data from.
        :type rapid_pro_domain: str
        :param rapid_pro_token_file_url: GS URL of a text file containing the authorisation token for the Rapid Pro
                                         server.
        :type rapid_pro_token_file_url: str
        :param activation_flow_names: The names of the RapidPro flows that contain the radio show responses.
        :type: activation_flow_names: list of str
        :param survey_flow_names: The names of the RapidPro flows that contain the survey responses.
        :type: survey_flow_names: list of str
        :param rapid_pro_test_contact_uuids: Rapid Pro contact UUIDs of test contacts.
                                             Runs for any of those test contacts will be tagged with {'test_run': True},
                                             and dropped when the pipeline is in production mode.
        :type rapid_pro_test_contact_uuids: list of str
        :param phone_number_uuid_table: Configuration for the Firestore phone number <-> uuid table.
        :type phone_number_uuid_table: PhoneNumberUuidTable
        :param memory_profile_upload_url_prefix:The prefix of the GS URL to upload the memory profile log to.
                                                 This prefix will be appended by the id of the pipeline run (provided
                                                 as a command line argument), and the ".profile" file extension.
        :type memory_profile_upload_url_prefix: str
        :param move_ws_messages: Whether to move messages labelled as Wrong Scheme to the correct dataset.
        :type move_ws_messages: bool
        :param generate_analysis_files: Whether to run post labelling pipeline stages.
        :type analysis_files_mode: bool
        :param rapid_pro_key_remappings: List of rapid_pro_key -> pipeline_key remappings.
        :type rapid_pro_key_remappings: list of RapidProKeyRemapping
        :param pipeline_name: The name of the pipeline to run.
        :type pipeline_name: str | None
        :param drive_upload: Configuration for uploading to Google Drive, or None.
                             If None, does not upload to Google Drive.
        :type drive_upload: DriveUploadPaths | None
        """
        self.rapid_pro_domain = rapid_pro_domain
        self.rapid_pro_token_file_url = rapid_pro_token_file_url
        self.activation_flow_names = activation_flow_names
        self.demog_flow_names = demog_flow_names
        self.follow_up_flow_names = follow_up_flow_names
        self.rapid_pro_test_contact_uuids = rapid_pro_test_contact_uuids
        self.phone_number_uuid_table = phone_number_uuid_table
        self.rapid_pro_key_remappings = rapid_pro_key_remappings
        self.memory_profile_upload_url_prefix = memory_profile_upload_url_prefix
        self.move_ws_messages = move_ws_messages
        self.generate_analysis_files = generate_analysis_files
        self.recovery_csv_urls = recovery_csv_urls
        self.pipeline_name = pipeline_name
        self.drive_upload = drive_upload

        self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        rapid_pro_domain = configuration_dict["RapidProDomain"]
        rapid_pro_token_file_url = configuration_dict["RapidProTokenFileURL"]
        activation_flow_names = configuration_dict["ActivationFlowNames"]
        demog_flow_names = configuration_dict["DemogFlowNames"]
        follow_up_flow_names = configuration_dict["FollowUpFlowNames"]
        rapid_pro_test_contact_uuids = configuration_dict["RapidProTestContactUUIDs"]

        phone_number_uuid_table = PhoneNumberUuidTable.from_configuration_dict(configuration_dict["PhoneNumberUuidTable"])

        rapid_pro_key_remappings = []
        for remapping_dict in configuration_dict["RapidProKeyRemappings"]:
            rapid_pro_key_remappings.append(RapidProKeyRemapping.from_configuration_dict(remapping_dict))

        memory_profile_upload_url_prefix = configuration_dict["MemoryProfileUploadURLPrefix"]

        move_ws_messages = configuration_dict["MoveWSMessages"]

        generate_analysis_files = configuration_dict["GenerateAnalysisFiles"]

        recovery_csv_urls = configuration_dict.get("RecoveryCSVURLs")

        pipeline_name = configuration_dict.get("PipelineName")
        drive_upload_paths = None
        if "DriveUpload" in configuration_dict:
            drive_upload_paths = DriveUpload.from_configuration_dict(configuration_dict["DriveUpload"])

        return cls(rapid_pro_domain, rapid_pro_token_file_url, activation_flow_names, demog_flow_names,
                   follow_up_flow_names, rapid_pro_test_contact_uuids, phone_number_uuid_table, rapid_pro_key_remappings,
                   memory_profile_upload_url_prefix, move_ws_messages, generate_analysis_files, recovery_csv_urls, pipeline_name, drive_upload_paths)

    @classmethod
    def from_configuration_file(cls, f):
        return cls.from_configuration_dict(json.load(f))

    def validate(self):
        validators.validate_string(self.rapid_pro_domain, "rapid_pro_domain")
        validators.validate_string(self.rapid_pro_token_file_url, "rapid_pro_token_file_url")

        validators.validate_list(self.activation_flow_names, "activation_flow_names")
        for i, activation_flow_name in enumerate(self.activation_flow_names):
            validators.validate_string(activation_flow_name, f"activation_flow_names[{i}]")

        validators.validate_list(self.follow_up_flow_names, "follow_up_flow_names")
        for i, follow_up_flow_name in enumerate(self.follow_up_flow_names):
            validators.validate_string(follow_up_flow_name, f"survey_follow_up_flow_names[{i}")

        validators.validate_list(self.follow_up_flow_names, "demog_flow_names")
        for i, demog_flow_name in enumerate(self.follow_up_flow_names):
            validators.validate_string(demog_flow_name, f"demog_flow_names[{i}")

        validators.validate_list(self.rapid_pro_test_contact_uuids, "rapid_pro_test_contact_uuids")
        for i, contact_uuid in enumerate(self.rapid_pro_test_contact_uuids):
            validators.validate_string(contact_uuid, f"rapid_pro_test_contact_uuids[{i}]")

        assert isinstance(self.phone_number_uuid_table, PhoneNumberUuidTable)
        self.phone_number_uuid_table.validate()

        validators.validate_list(self.rapid_pro_key_remappings, "rapid_pro_key_remappings")
        for i, remapping in enumerate(self.rapid_pro_key_remappings):
            assert isinstance(remapping, RapidProKeyRemapping), \
                f"{remapping} is not of type RapidProKeyRemapping"

            remapping.validate()

        validators.validate_string(self.memory_profile_upload_url_prefix, "memory_profile_upload_url_prefix")

        if self.recovery_csv_urls is not None:
            validators.validate_list(self.recovery_csv_urls, "recovery_csv_urls")
            for i, recovery_csv_url in enumerate(self.recovery_csv_urls):
                validators.validate_string(recovery_csv_url, f"{recovery_csv_url}")

        if self.pipeline_name is not None:
            validators.validate_string(self.pipeline_name, "pipeline_name")

        validators.validate_bool(self.move_ws_messages, "move_ws_messages")
        validators.validate_bool(self.generate_analysis_files, "generate_analysis_files")

        if self.drive_upload is not None:
            assert isinstance(self.drive_upload, DriveUpload), \
                "drive_upload is not of type DriveUpload"
            self.drive_upload.validate()


class PhoneNumberUuidTable(object):
    def __init__(self, firebase_credentials_file_url, table_name):
        """
        :param firebase_credentials_file_url: GS URL to the private credentials file for the Firebase account where
                                                 the phone number <-> uuid table is stored.
        :type firebase_credentials_file_url: str
        :param table_name: Name of the data <-> uuid table in Firebase to use.
        :type table_name: str
        """
        self.firebase_credentials_file_url = firebase_credentials_file_url
        self.table_name = table_name

        self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        firebase_credentials_file_url = configuration_dict["FirebaseCredentialsFileURL"]
        table_name = configuration_dict["TableName"]

        return cls(firebase_credentials_file_url, table_name)

    def validate(self):
        validators.validate_url(self.firebase_credentials_file_url, "firebase_credentials_file_url", scheme="gs")
        validators.validate_string(self.table_name, "table_name")


class RapidProKeyRemapping(object):
    def __init__(self, is_activation_message, rapid_pro_key, pipeline_key):
        """
        :param is_activation_message: Whether this re-mapping contains an activation message (activation messages need
                                   to be handled differently because they are not always in the correct flow)
        :type is_activation_message: bool
        :param rapid_pro_key: Name of key in the dataset exported via RapidProTools.
        :type rapid_pro_key: str
        :param pipeline_key: Name to use for that key in the rest of the pipeline.
        :type pipeline_key: str
        """
        self.is_activation_message = is_activation_message
        self.rapid_pro_key = rapid_pro_key
        self.pipeline_key = pipeline_key

        self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        is_activation_message = configuration_dict.get("IsActivationMessage", False)
        rapid_pro_key = configuration_dict["RapidProKey"]
        pipeline_key = configuration_dict["PipelineKey"]

        return cls(is_activation_message, rapid_pro_key, pipeline_key)

    def validate(self):
        validators.validate_bool(self.is_activation_message, "is_activation_message")
        validators.validate_string(self.rapid_pro_key, "rapid_pro_key")
        validators.validate_string(self.pipeline_key, "pipeline_key")


class DriveUpload(object):
    def __init__(self, drive_credentials_file_url, production_upload_path, messages_upload_path,
                 individuals_upload_path, messages_traced_data_upload_path, individuals_traced_data_upload_path):
        """
        :param drive_credentials_file_url: GS URL to the private credentials file for the Drive service account to use
                                           to upload the output files.
        :type drive_credentials_file_url: str
        :param production_upload_path: Path in the Drive service account's "Shared with Me" directory to upload the
                                       production CSV to.
        :type production_upload_path: str
        :param messages_upload_path: Path in the Drive service account's "Shared with Me" directory to upload the
                                     messages analysis CSV to.
        :type messages_upload_path: str
        :param individuals_upload_path: Path in the Drive service account's "Shared with Me" directory to upload the
                                        individuals analysis CSV to.
        :type individuals_upload_path: str
        :param messages_traced_data_upload_path: Path in the Drive service account's "Shared with Me" directory to
                                                 upload the serialized messages TracedData from this pipeline run to.
        :type messages_traced_data_upload_path: str
        :param individuals_traced_data_upload_path: Path in the Drive service account's "Shared with Me" directory to
                                                    upload the serialized individuals TracedData from this pipeline
                                                    run to.
        :type individuals_traced_data_upload_path: str
        """
        self.drive_credentials_file_url = drive_credentials_file_url
        self.production_upload_path = production_upload_path
        self.messages_upload_path = messages_upload_path
        self.individuals_upload_path = individuals_upload_path
        self.messages_traced_data_upload_path = messages_traced_data_upload_path
        self.individuals_traced_data_upload_path = individuals_traced_data_upload_path

        self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        drive_credentials_file_url = configuration_dict["DriveCredentialsFileURL"]
        production_upload_path = configuration_dict["ProductionUploadPath"]
        messages_upload_path = configuration_dict["MessagesUploadPath"]
        individuals_upload_path = configuration_dict["IndividualsUploadPath"]
        messages_traced_data_upload_path = configuration_dict["MessagesTracedDataUploadPath"]
        individuals_traced_data_upload_path = configuration_dict["IndividualsTracedDataUploadPath"]

        return cls(drive_credentials_file_url, production_upload_path, messages_upload_path,
                   individuals_upload_path, messages_traced_data_upload_path, individuals_traced_data_upload_path)

    def validate(self):
        validators.validate_string(self.drive_credentials_file_url, "drive_credentials_file_url")
        assert urlparse(self.drive_credentials_file_url).scheme == "gs", "DriveCredentialsFileURL needs to be a gs " \
                                                                         "URL (i.e. of the form gs://bucket-name/file)"

        validators.validate_string(self.production_upload_path, "production_upload_path")
        validators.validate_string(self.messages_upload_path, "messages_upload_path")
        validators.validate_string(self.individuals_upload_path, "individuals_upload_path")
        validators.validate_string(self.messages_traced_data_upload_path, "messages_traced_data_upload_path")
        validators.validate_string(self.individuals_traced_data_upload_path, "individuals_traced_data_upload_path")
