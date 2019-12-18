import json
from urllib.parse import urlparse

from core_data_modules.cleaners import somali, Codes
from core_data_modules.data_models import validators
from dateutil.parser import isoparse

from src.lib import CodeSchemes, code_imputation_functions


class CodingModes(object):
    SINGLE = "SINGLE"
    MULTIPLE = "MULTIPLE"


class FoldingModes(object):
    ASSERT_EQUAL = "ASSERT_EQUAL"
    YES_NO_AMB = "YES_NO_AMB"
    CONCATENATE = "CONCATENATE"
    MATRIX = "MATRIX"


class CodingConfiguration(object):
    def __init__(self, coding_mode, code_scheme, coded_field, folding_mode, analysis_file_key=None, cleaner=None):
        assert coding_mode in {CodingModes.SINGLE, CodingModes.MULTIPLE}

        self.coding_mode = coding_mode
        self.code_scheme = code_scheme
        self.coded_field = coded_field
        self.analysis_file_key = analysis_file_key
        self.folding_mode = folding_mode
        self.cleaner = cleaner


class CodingPlan(object):
    def __init__(self, raw_field, coding_configurations, raw_field_folding_mode, coda_filename=None, ws_code=None,
                 time_field=None, run_id_field=None, icr_filename=None, id_field=None, code_imputation_function=None):
        self.raw_field = raw_field
        self.time_field = time_field
        self.run_id_field = run_id_field
        self.coda_filename = coda_filename
        self.icr_filename = icr_filename
        self.coding_configurations = coding_configurations
        self.code_imputation_function = code_imputation_function
        self.ws_code = ws_code
        self.raw_field_folding_mode = raw_field_folding_mode

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

    Q4_RQA_CODING_PLANS = [
        CodingPlan(raw_field="rqa_s01mag08_raw",
                   time_field="sent_on",
                   coda_filename="s01mag08.json",
                   icr_filename="s01mag08.csv",
                   run_id_field="rqa_s01mag08_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                            coding_mode=CodingModes.MULTIPLE,
                            code_scheme=CodeSchemes.S01MAG08,
                            folding_mode=FoldingModes.MATRIX,
                            coded_field="rqa_s01mag08_coded",
                            analysis_file_key="rqa_s01mag08_",
                       )
                  ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag08"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),


        CodingPlan(raw_field="rqa_s01mag09_raw",
                   time_field="sent_on",
                   coda_filename="s01mag09.json",
                   icr_filename="s01mag09.csv",
                   run_id_field="rqa_s01mag09_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.S01MAG09_YES_NO_AMB,
                           folding_mode=FoldingModes.YES_NO_AMB,
                           coded_field="rqa_s01mag09_yes_no_amb_coded",
                           analysis_file_key="rqa_s01mag09_yes_no_amb",
                       ),
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.S01MAG09,
                           folding_mode=FoldingModes.MATRIX,
                           analysis_file_key="rqa_s01mag09_",
                           coded_field="rqa_s01mag09_coded",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag09"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),

        CodingPlan(raw_field="rqa_s01mag10_raw",
                   time_field="sent_on",
                   coda_filename="s01mag10.json",
                   icr_filename="s01mag10.csv",
                   run_id_field="rqa_s01mag10_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.S01MAG10,
                           folding_mode=FoldingModes.MATRIX,
                           coded_field="rqa_s01mag10_coded",
                           analysis_file_key="rqa_s01mag10_",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag10"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),

        CodingPlan(raw_field="rqa_s01mag11_raw",
                   time_field="sent_on",
                   coda_filename="s01mag11.json",
                   icr_filename="s01mag11.csv",
                   run_id_field="rqa_s01mag11_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.S01MAG11,
                           folding_mode=FoldingModes.MATRIX,
                           coded_field="rqa_s01mag11_coded",
                           analysis_file_key="rqa_s01mag11_",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag11"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),

        CodingPlan(raw_field="rqa_s01mag12_raw",
                   time_field="sent_on",
                   coda_filename="s01mag12.json",
                   icr_filename="s01mag12.csv",
                   run_id_field="rqa_s01mag12_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.S01MAG12,
                           folding_mode=FoldingModes.MATRIX,
                           coded_field="rqa_s01mag12_coded",
                           analysis_file_key="rqa_s01mag12_",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag12"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),

        CodingPlan(raw_field="rqa_s01mag13_raw",
                   time_field="sent_on",
                   coda_filename="s01mag13.json",
                   icr_filename="s01mag13.csv",
                   run_id_field="rqa_s01mag13_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.S01MAG13,
                           folding_mode=FoldingModes.MATRIX,
                           coded_field="rqa_s01mag13_coded",
                           analysis_file_key="rqa_s01mag13_",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag13"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE)
    ]

    Q5_RQA_CODING_PLANS = [
        CodingPlan(raw_field="rqa_s01mag14_raw",
                   time_field="sent_on",
                   coda_filename="s01mag14.json",
                   icr_filename="s01mag14.csv",
                   run_id_field="rqa_s01mag14_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.S01MAG14,
                           folding_mode=FoldingModes.MATRIX,
                           coded_field="rqa_s01mag14_coded",
                           analysis_file_key="rqa_s01mag14_",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag14"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),

        CodingPlan(raw_field="rqa_s01mag15_raw",
                   time_field="sent_on",
                   coda_filename="s01mag15.json",
                   icr_filename="s01mag15.csv",
                   run_id_field="rqa_s01mag15_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.S01MAG15,
                           folding_mode=FoldingModes.MATRIX,
                           coded_field="rqa_s01mag15_coded",
                           analysis_file_key="rqa_s01mag15_",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag15"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),

        CodingPlan(raw_field="rqa_s01mag16_raw",
                   time_field="sent_on",
                   coda_filename="s01mag16.json",
                   icr_filename="s01mag16.csv",
                   run_id_field="rqa_s01mag16_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.S01MAG16,
                           folding_mode=FoldingModes.MATRIX,
                           coded_field="rqa_s01mag16_coded",
                           analysis_file_key="rqa_s01mag16_",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag16"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),

        CodingPlan(raw_field="rqa_s01mag17_raw",
                   time_field="sent_on",
                   coda_filename="s01mag17.json",
                   icr_filename="s01mag17.csv",
                   run_id_field="rqa_s01mag17_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.S01MAG17,
                           folding_mode=FoldingModes.MATRIX,
                           coded_field="rqa_s01mag17_coded",
                           analysis_file_key="rqa_s01mag17_",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag17"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),

        CodingPlan(raw_field="rqa_s01mag18_raw",
                   time_field="sent_on",
                   coda_filename="s01mag18.json",
                   icr_filename="s01mag18.csv",
                   run_id_field="rqa_s01mag18_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.S01MAG18,
                           folding_mode=FoldingModes.MATRIX,
                           coded_field="rqa_s01mag18_coded",
                           analysis_file_key="rqa_s01mag18_",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag18"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),

        CodingPlan(raw_field="rqa_s01mag19_raw",
                   time_field="sent_on",
                   coda_filename="s01mag19.json",
                   icr_filename="s01mag19.csv",
                   run_id_field="rqa_s01mag19_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.S01MAG19,
                           folding_mode=FoldingModes.MATRIX,
                           coded_field="rqa_s01mag19_coded",
                           analysis_file_key="rqa_s01mag19_",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag19"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),

        CodingPlan(raw_field="rqa_s01mag20_raw",
                   time_field="sent_on",
                   coda_filename="s01mag20.json",
                   icr_filename="s01mag20.csv",
                   run_id_field="rqa_s01mag20_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.S01MAG20_YES_NO_AMB,
                           folding_mode=FoldingModes.YES_NO_AMB,
                           coded_field="rqa_s01mag20_yes_no_amb_coded",
                           analysis_file_key="rqa_s01mag20_yes_no_amb",
                       ),
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.S01MAG20,
                           folding_mode=FoldingModes.MATRIX,
                           coded_field="rqa_s01mag20_coded",
                           analysis_file_key="rqa_s01mag20_",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag20"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),
    ]

    Q6_RQA_CODING_PLANS = [
        CodingPlan(raw_field="rqa_s02e08_raw",
                   time_field="sent_on",
                   coda_filename="s02e08.json",
                   icr_filename="s02e08.csv",
                   run_id_field="rqa_s02e08_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.S02E08,
                           folding_mode=FoldingModes.MATRIX,
                           coded_field="rqa_s02e08_coded",
                           analysis_file_key="rqa_s02e08_",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s02e08"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),


    ]

    Q7_RQA_CODING_PLANS = [
        CodingPlan(raw_field="rqa_s02mag21_raw",
                   time_field="sent_on",
                   coda_filename="s02mag21.json",
                   icr_filename="s02mag21.csv",
                   run_id_field="rqa_s02mag21_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.S02MAG21,
                           folding_mode=FoldingModes.MATRIX,
                           coded_field="rqa_s02mag21_coded",
                           analysis_file_key="rqa_s02mag21_",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s02mag21"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),

        CodingPlan(raw_field="rqa_s02mag22_raw",
                   time_field="sent_on",
                   coda_filename="s02mag22.json",
                   icr_filename="s02mag22.csv",
                   run_id_field="rqa_s02mag22_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.S02MAG22,
                           folding_mode=FoldingModes.MATRIX,
                           coded_field="rqa_s02mag22_coded",
                           analysis_file_key="rqa_s02mag22_",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s02mag22"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),

        CodingPlan(raw_field="rqa_s02mag23_raw",
                   time_field="sent_on",
                   coda_filename="s02mag23.json",
                   icr_filename="s02mag23.csv",
                   run_id_field="rqa_s02mag23_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.S02MAG23,
                           folding_mode=FoldingModes.MATRIX,
                           coded_field="rqa_s02mag23_coded",
                           analysis_file_key="rqa_s02mag23_",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s02mag23"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),

        CodingPlan(raw_field="rqa_s02mag24_raw",
                   time_field="sent_on",
                   coda_filename="s02mag24.json",
                   icr_filename="s02mag24.csv",
                   run_id_field="rqa_s02mag24_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.S02MAG24,
                           folding_mode=FoldingModes.MATRIX,
                           coded_field="rqa_s02mag24_coded",
                           analysis_file_key="rqa_s02mag24_",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s02mag24"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),

        CodingPlan(raw_field="rqa_s02mag25_raw",
                   time_field="sent_on",
                   coda_filename="s02mag25.json",
                   icr_filename="s02mag25.csv",
                   run_id_field="rqa_s02mag25_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.S02MAG25,
                           folding_mode=FoldingModes.MATRIX,
                           coded_field="rqa_s02mag25_coded",
                           analysis_file_key="rqa_s02mag25_",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s02mag25"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),

        CodingPlan(raw_field="rqa_s02mag26_raw",
                   time_field="sent_on",
                   coda_filename="s02mag26.json",
                   icr_filename="s02mag26.csv",
                   run_id_field="rqa_s02mag26_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.S02MAG26,
                           folding_mode=FoldingModes.MATRIX,
                           coded_field="rqa_s02mag26_coded",
                           analysis_file_key="rqa_s02mag26_",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s02mag26"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),

        CodingPlan(raw_field="rqa_s02mag27_raw",
                   time_field="sent_on",
                   coda_filename="s02mag27.json",
                   icr_filename="s02mag27.csv",
                   run_id_field="rqa_s02mag27_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.S02MAG27,
                           folding_mode=FoldingModes.MATRIX,
                           coded_field="rqa_s02mag27_coded",
                           analysis_file_key="rqa_s02mag27_",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s02mag27"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),

        CodingPlan(raw_field="rqa_s02mag28_raw",
                   time_field="sent_on",
                   coda_filename="s02mag28.json",
                   icr_filename="s02mag28.csv",
                   run_id_field="rqa_s02mag28_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.S02MAG28,
                           folding_mode=FoldingModes.MATRIX,
                           coded_field="rqa_s02mag28_coded",
                           analysis_file_key="rqa_s02mag28_",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s02mag28"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),

        CodingPlan(raw_field="rqa_s02mag29_raw",
                   time_field="sent_on",
                   coda_filename="s02mag29.json",
                   icr_filename="s02mag29.csv",
                   run_id_field="rqa_s02mag29_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.S02MAG29,
                           folding_mode=FoldingModes.MATRIX,
                           coded_field="rqa_s02mag29_coded",
                           analysis_file_key="rqa_s02mag29_",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s02mag29"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),

        CodingPlan(raw_field="rqa_s02mag30_raw",
                   time_field="sent_on",
                   coda_filename="s02mag30.json",
                   icr_filename="s02mag30.csv",
                   run_id_field="rqa_s02mag30_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.S02MAG30,
                           folding_mode=FoldingModes.MATRIX,
                           coded_field="rqa_s02mag30_coded",
                           analysis_file_key="rqa_s02mag30_",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s02mag30"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),
    ]

    FULL_PIPELINE_RQA_CODING_PLANS = [
        CodingPlan(raw_field="rqa_s01e01_raw",
                   time_field="sent_on",
                   coda_filename="s01e01.json",
                   icr_filename="s01e01.csv",
                   run_id_field="rqa_s01e01_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.S01E01,
                           folding_mode=FoldingModes.MATRIX,
                           coded_field="rqa_s01e01_coded",
                           analysis_file_key="rqa_s01e01_",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01e01"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),

        CodingPlan(raw_field="rqa_s01e02_raw",
                   time_field="sent_on",
                   coda_filename="s01e02.json",
                   icr_filename="s01e02.csv",
                   run_id_field="rqa_s01e02_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.S01E02_YES_NO_AMB,
                           folding_mode=FoldingModes.YES_NO_AMB,
                           coded_field="rqa_s01e02_yes_no_amb_coded",
                           analysis_file_key="rqa_s01e02_yes_no_amb",
                       ),
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.S01E02,
                           folding_mode=FoldingModes.MATRIX,
                           coded_field="rqa_s01e02_coded",
                           analysis_file_key="rqa_s01e02_",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01e02"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),

        CodingPlan(raw_field="rqa_s01e03_raw",
                   time_field="sent_on",
                   coda_filename="s01e03.json",
                   icr_filename="s01e03.csv",
                   run_id_field="rqa_s01e03_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.S01E03_YES_NO_AMB,
                           folding_mode=FoldingModes.YES_NO_AMB,
                           coded_field="rqa_s01e03_yes_no_amb_coded",
                           analysis_file_key="rqa_s01e03_yes_no_amb",
                       ),
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.S01E03,
                           folding_mode=FoldingModes.MATRIX,
                           analysis_file_key="rqa_s01e03_",
                           coded_field="rqa_s01e03_coded",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01e03"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),

        CodingPlan(raw_field="rqa_s01e04_raw",
                   time_field="sent_on",
                   coda_filename="s01e04.json",
                   icr_filename="s01e04.csv",
                   run_id_field="rqa_s01e04_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.S01E04,
                           folding_mode=FoldingModes.MATRIX,
                           coded_field="rqa_s01e04_coded",
                           analysis_file_key="rqa_s01e04_",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01e04"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),

        CodingPlan(raw_field="rqa_s01e05_raw",
                   time_field="sent_on",
                   coda_filename="s01e05.json",
                   icr_filename="s01e05.csv",
                   run_id_field="rqa_s01e05_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.S01E05_YES_NO_AMB,
                           folding_mode=FoldingModes.YES_NO_AMB,
                           coded_field="rqa_s01e05_yes_no_amb_coded",
                           analysis_file_key="rqa_s01e05_yes_no_amb",
                       ),
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.S01E05,
                           folding_mode=FoldingModes.MATRIX,
                           analysis_file_key="rqa_s01e05_",
                           coded_field="rqa_s01e05_coded",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01e05"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),

        CodingPlan(raw_field="rqa_s01e06_raw",
                   time_field="sent_on",
                   coda_filename="s01e06.json",
                   icr_filename="s01e06.csv",
                   run_id_field="rqa_s01e06_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.S01E06,
                           folding_mode=FoldingModes.MATRIX,
                           coded_field="rqa_s01e06_coded",
                           analysis_file_key="rqa_s01e06_",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01e06"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),

        CodingPlan(raw_field="rqa_s01e07_raw",
                   time_field="sent_on",
                   coda_filename="s01e07.json",
                   icr_filename="s01e07.csv",
                   run_id_field="rqa_s01e07_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.S01E07_YES_NO_AMB,
                           folding_mode=FoldingModes.YES_NO_AMB,
                           coded_field="rqa_s01e07_yes_no_amb_coded",
                           analysis_file_key="rqa_s01e07_yes_no_amb",
                       ),
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.S01E07,
                           folding_mode=FoldingModes.MATRIX,
                           analysis_file_key="rqa_s01e07_",
                           coded_field="rqa_s01e07_coded",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01e07"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),

        CodingPlan(raw_field="rqa_s01mag03_raw",
                   time_field="sent_on",
                   coda_filename="s01mag03.json",
                   icr_filename="s01mag03.csv",
                   run_id_field="rqa_s01mag03_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.S01MAG03_YES_NO_AMB,
                           folding_mode=FoldingModes.YES_NO_AMB,
                           coded_field="rqa_s01mag03_yes_no_amb_coded",
                           analysis_file_key="rqa_s01mag03_yes_no_amb",
                       ),
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.S01MAG03,
                           folding_mode=FoldingModes.MATRIX,
                           analysis_file_key="rqa_s01mag03_",
                           coded_field="rqa_s01mag03_coded",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag03"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),

        CodingPlan(raw_field="rqa_s01mag04_raw",
                   time_field="sent_on",
                   coda_filename="s01mag04.json",
                   icr_filename="s01mag04.csv",
                   run_id_field="rqa_s01mag04_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.S01MAG04,
                           folding_mode=FoldingModes.MATRIX,
                           analysis_file_key="rqa_s01mag04_",
                           coded_field="rqa_s01mag04_coded",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag04"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),

        CodingPlan(raw_field="rqa_s01mag05_raw",
                   time_field="sent_on",
                   coda_filename="s01mag05.json",
                   icr_filename="s01mag05.csv",
                   run_id_field="rqa_s01mag05_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           folding_mode=FoldingModes.YES_NO_AMB,
                           code_scheme=CodeSchemes.S01MAG05_YES_NO_AMB,
                           coded_field="rqa_s01mag05_yes_no_amb_coded",
                           analysis_file_key="rqa_s01mag05_yes_no_amb",
                       ),
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.S01MAG05,
                           analysis_file_key="rqa_s01mag05_",
                           folding_mode=FoldingModes.MATRIX,
                           coded_field="rqa_s01mag05_coded",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag05"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),

        CodingPlan(raw_field="rqa_s01mag06_raw",
                   time_field="sent_on",
                   coda_filename="s01mag06.json",
                   icr_filename="s01mag06.csv",
                   run_id_field="rqa_s01mag06_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.S01MAG06_YES_NO_AMB,
                           folding_mode=FoldingModes.YES_NO_AMB,
                           coded_field="rqa_s01mag06_yes_no_amb_coded",
                           analysis_file_key="rqa_s01mag06_yes_no_amb",
                       ),
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.S01MAG06,
                           folding_mode=FoldingModes.MATRIX,
                           analysis_file_key="rqa_s01mag06_",
                           coded_field="rqa_s01mag06_coded",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag06"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),

        CodingPlan(raw_field="rqa_s01mag07_raw",
                   time_field="sent_on",
                   coda_filename="s01mag07.json",
                   icr_filename="s01mag07.csv",
                   run_id_field="rqa_s01mag07_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.S01MAG07_YES_NO_AMB,
                           folding_mode=FoldingModes.YES_NO_AMB,
                           coded_field="rqa_s01mag07_yes_no_amb_coded",
                           analysis_file_key="rqa_s01mag07_yes_no_amb",
                       ),
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.S01MAG07,
                           analysis_file_key="rqa_s01mag07_",
                           folding_mode=FoldingModes.MATRIX,
                           coded_field="rqa_s01mag07_coded",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s01mag07"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE)
    ]
    FULL_PIPELINE_RQA_CODING_PLANS += Q4_RQA_CODING_PLANS
    FULL_PIPELINE_RQA_CODING_PLANS += Q5_RQA_CODING_PLANS
    FULL_PIPELINE_RQA_CODING_PLANS += Q6_RQA_CODING_PLANS
    FULL_PIPELINE_RQA_CODING_PLANS += Q7_RQA_CODING_PLANS

    FOLLOW_UP_CODING_PLANS = [
        CodingPlan(raw_field="women_participation_raw",
                   time_field="sent_on",
                   coda_filename="women_participation.json",
                   icr_filename="women_participation.csv",
                   run_id_field="women_participation_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.WOMEN_PARTICIPATION_YES_NO_AMB,
                           folding_mode=FoldingModes.YES_NO_AMB,
                           coded_field="women_participation_yes_no_amb_coded",
                           analysis_file_key="women_participation_yes_no_amb",
                       ),
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.WOMEN_PARTICIPATION,
                           folding_mode=FoldingModes.MATRIX,
                           coded_field="women_participation_coded",
                           analysis_file_key="women_participation_",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("women_participation"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),

        CodingPlan(raw_field="minority_clan_issues_raw",
                   time_field="sent_on",
                   coda_filename="minority_clan_issues.json",
                   icr_filename="minority_clan_issues.csv",
                   run_id_field="minority_clan_issues_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.MINORITY_CLAN_ISSUES_YES_NO_AMB,
                           folding_mode=FoldingModes.YES_NO_AMB,
                           coded_field="minority_clan_issues_yes_no_amb_coded",
                           analysis_file_key="minority_clan_issues_yes_no_amb",
                       ),
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.MINORITY_CLAN_ISSUES,
                           folding_mode=FoldingModes.MATRIX,
                           coded_field="minority_clan_issues_coded",
                           analysis_file_key="minority_clan_issues_",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("minority_clan_issues"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),

        CodingPlan(raw_field="young_people_issues_raw",
                   time_field="sent_on",
                   coda_filename="young_people_issues.json",
                   icr_filename="young_people_issues.csv",
                   run_id_field="young_people_issues_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.YOUNG_PEOPLE_ISSUES_YES_NO_AMB,
                           folding_mode=FoldingModes.YES_NO_AMB,
                           coded_field="young_people_issues_yes_no_amb_coded",
                           analysis_file_key="young_people_issues_yes_no_amb",
                       ),
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.YOUNG_PEOPLE_ISSUES,
                           folding_mode=FoldingModes.MATRIX,
                           coded_field="young_people_issues_coded",
                           analysis_file_key="young_people_issues_",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("young_people_issues"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),

        CodingPlan(raw_field="decisions_minority_clan_raw",
                   time_field="sent_on",
                   coda_filename="decisions_minority_clan.json",
                   icr_filename="decisions_minority_clan.csv",
                   run_id_field="decisions_minority_clan_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.DECISIONS_MINORITY_CLAN_YES_NO_AMB,
                           folding_mode=FoldingModes.YES_NO_AMB,
                           coded_field="decisions_minority_clan_yes_no_amb_coded",
                           analysis_file_key="decisions_minority_clan_yes_no_amb",
                       ),
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.DECISIONS_MINORITY_CLAN,
                           folding_mode=FoldingModes.MATRIX,
                           coded_field="decisions_minority_clan_coded",
                           analysis_file_key="decisions_minority_clan_",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("decisions_minority_clan"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),

        CodingPlan(raw_field="women_decision_making_opportunities_raw",
                   time_field="sent_on",
                   coda_filename="women_decision_making_opportunities.json",
                   icr_filename="women_decision_making_opportunities.csv",
                   run_id_field="women_decision_making_opportunities_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.WOMEN_PARTICIPATION_YES_NO_AMB,
                           folding_mode=FoldingModes.YES_NO_AMB,
                           coded_field="women_decision_making_opportunities_yes_no_amb_coded",
                           analysis_file_key="women_decision_making_opportunities_yes_no_amb",
                       ),
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.WOMEN_PARTICIPATION,
                           folding_mode=FoldingModes.MATRIX,
                           coded_field="women_decision_making_opportunities_clan_coded",
                           analysis_file_key="women_decision_making_opportunities_",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value(
                       "women_decision_making_opportunities"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),

        CodingPlan(raw_field="idps_decision_making_opportunities_raw",
                   time_field="sent_on",
                   coda_filename="idps_decision_making_opportunities.json",
                   icr_filename="idps_decision_making_opportunities.csv",
                   run_id_field="idps_decision_making_opportunities_run_id",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.IDPS_DECISION_MAKING_OPPORTUNITIES_YES_NO_AMB,
                           folding_mode=FoldingModes.YES_NO_AMB,
                           coded_field="idps_decision_making_opportunities_yes_no_amb_coded",
                           analysis_file_key="idps_decision_making_opportunities_yes_no_amb",
                       ),
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.IDPS_DECISION_MAKING_OPPORTUNITIES,
                           folding_mode=FoldingModes.MATRIX,
                           coded_field="idps_decision_making_opportunities_clan_coded",
                           analysis_file_key="idps_decision_making_opportunities_",
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("idps_decision_making_opportunities"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),
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

    @staticmethod
    def clean_district_if_no_mogadishu_sub_district(text):
        mogadishu_sub_district = somali.DemographicCleaner.clean_mogadishu_sub_district(text)
        if mogadishu_sub_district == Codes.NOT_CODED:
            return somali.DemographicCleaner.clean_somalia_district(text)
        else:
            return Codes.NOT_CODED

    DEMOG_CODING_PLANS = [
        CodingPlan(raw_field="gender_raw",
                   time_field="gender_time",
                   coda_filename="gender.json",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.GENDER,
                           cleaner=somali.DemographicCleaner.clean_gender,
                           coded_field="gender_coded",
                           analysis_file_key="gender",
                           folding_mode=FoldingModes.ASSERT_EQUAL
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("gender"),
                   raw_field_folding_mode=FoldingModes.ASSERT_EQUAL),

        CodingPlan(raw_field="age_raw",
                   time_field="age_time",
                   coda_filename="age.json",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.AGE,
                           cleaner=lambda text: PipelineConfiguration.clean_age_with_range_filter(text),
                           coded_field="age_coded",
                           analysis_file_key="age",
                           folding_mode=FoldingModes.ASSERT_EQUAL
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("age"),
                   raw_field_folding_mode=FoldingModes.ASSERT_EQUAL),

        CodingPlan(raw_field="recently_displaced_raw",
                   time_field="recently_displaced_time",
                   coda_filename="recently_displaced.json",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.RECENTLY_DISPLACED,
                           cleaner=somali.DemographicCleaner.clean_yes_no,
                           coded_field="recently_displaced_coded",
                           analysis_file_key="recently_displaced",
                           folding_mode=FoldingModes.ASSERT_EQUAL
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("recently displaced"),
                   raw_field_folding_mode=FoldingModes.ASSERT_EQUAL),

        CodingPlan(raw_field="household_language_raw",
                   time_field="household_language_time",
                   coda_filename="household_language.json",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.HOUSEHOLD_LANGUAGE,
                           coded_field="household_language_coded",
                           analysis_file_key="household_language",
                           folding_mode=FoldingModes.ASSERT_EQUAL
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("hh language"),
                   raw_field_folding_mode=FoldingModes.ASSERT_EQUAL),

        CodingPlan(raw_field="location_raw",
                   time_field="location_time",
                   coda_filename="location.json",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.MOGADISHU_SUB_DISTRICT,
                           folding_mode=FoldingModes.ASSERT_EQUAL,
                           cleaner=somali.DemographicCleaner.clean_mogadishu_sub_district,
                           coded_field="mogadishu_sub_district_coded"
                       ),
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.SOMALIA_DISTRICT,
                           cleaner=lambda text: PipelineConfiguration.clean_district_if_no_mogadishu_sub_district(text),
                           folding_mode=FoldingModes.ASSERT_EQUAL,
                           coded_field="district_coded",
                           analysis_file_key="district"
                       ),
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.SOMALIA_REGION,
                           folding_mode=FoldingModes.ASSERT_EQUAL,
                           coded_field="region_coded",
                           analysis_file_key="region",
                       ),
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.SOMALIA_STATE,
                           folding_mode=FoldingModes.ASSERT_EQUAL,
                           coded_field="state_coded",
                           analysis_file_key="state"
                       ),
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.SOMALIA_ZONE,
                           folding_mode=FoldingModes.ASSERT_EQUAL,
                           coded_field="zone_coded",
                           analysis_file_key="zone",
                       )
                   ],
                   code_imputation_function=code_imputation_functions.impute_somalia_location_codes,
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("location"),
                   raw_field_folding_mode=FoldingModes.ASSERT_EQUAL)
    ]


    def __init__(self, rapid_pro_domain, rapid_pro_token_file_url, activation_flow_names, survey_flow_names,
                 rapid_pro_test_contact_uuids, phone_number_uuid_table, rapid_pro_key_remappings,
                 memory_profile_upload_url_prefix, data_archive_upload_url_prefix, move_ws_messages, recovery_csv_urls=None, pipeline_name=None,
                 drive_upload=None):
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
        :param data_archive_upload_url_prefix:The prefix of the GS URL to upload the data backup file to.
        :type data_archive_upload_url_prefix: str
        :param move_ws_messages: Whether to move messages labelled as Wrong Scheme to the correct dataset.
        :type move_ws_messages: bool
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
        self.survey_flow_names = survey_flow_names
        self.rapid_pro_test_contact_uuids = rapid_pro_test_contact_uuids
        self.phone_number_uuid_table = phone_number_uuid_table
        self.rapid_pro_key_remappings = rapid_pro_key_remappings
        self.memory_profile_upload_url_prefix = memory_profile_upload_url_prefix
        self.data_archive_upload_url_prefix = data_archive_upload_url_prefix
        self.move_ws_messages = move_ws_messages
        self.recovery_csv_urls = recovery_csv_urls
        self.pipeline_name = pipeline_name
        self.drive_upload = drive_upload

        self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        rapid_pro_domain = configuration_dict["RapidProDomain"]
        rapid_pro_token_file_url = configuration_dict["RapidProTokenFileURL"]
        activation_flow_names = configuration_dict["ActivationFlowNames"]
        survey_flow_names = configuration_dict["SurveyFlowNames"]
        rapid_pro_test_contact_uuids = configuration_dict["RapidProTestContactUUIDs"]

        phone_number_uuid_table = PhoneNumberUuidTable.from_configuration_dict(configuration_dict["PhoneNumberUuidTable"])

        rapid_pro_key_remappings = []
        for remapping_dict in configuration_dict["RapidProKeyRemappings"]:
            rapid_pro_key_remappings.append(RapidProKeyRemapping.from_configuration_dict(remapping_dict))

        memory_profile_upload_url_prefix = configuration_dict["MemoryProfileUploadURLPrefix"]

        data_archive_upload_url_prefix = configuration_dict["DataArchiveUploadURLPrefix"]

        move_ws_messages = configuration_dict["MoveWSMessages"]

        recovery_csv_urls = configuration_dict.get("RecoveryCSVURLs")

        pipeline_name = configuration_dict.get("PipelineName")
        drive_upload_paths = None
        if "DriveUpload" in configuration_dict:
            drive_upload_paths = DriveUpload.from_configuration_dict(configuration_dict["DriveUpload"])

        return cls(rapid_pro_domain, rapid_pro_token_file_url, activation_flow_names, survey_flow_names,
                   rapid_pro_test_contact_uuids, phone_number_uuid_table, rapid_pro_key_remappings,
                   memory_profile_upload_url_prefix, data_archive_upload_url_prefix, move_ws_messages, recovery_csv_urls, pipeline_name,
                   drive_upload_paths)

    @classmethod
    def from_configuration_file(cls, f):
        return cls.from_configuration_dict(json.load(f))

    def validate(self):
        validators.validate_string(self.rapid_pro_domain, "rapid_pro_domain")
        validators.validate_string(self.rapid_pro_token_file_url, "rapid_pro_token_file_url")

        validators.validate_list(self.activation_flow_names, "activation_flow_names")
        for i, activation_flow_name in enumerate(self.activation_flow_names):
            validators.validate_string(activation_flow_name, f"activation_flow_names[{i}]")

        validators.validate_list(self.survey_flow_names, "survey_flow_names")
        for i, survey_flow_name in enumerate(self.survey_flow_names):
            validators.validate_string(survey_flow_name, f"survey_flow_names[{i}")

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

        validators.validate_string(self.data_archive_upload_url_prefix, "data_archive_upload_url_prefix")

        if self.recovery_csv_urls is not None:
            validators.validate_list(self.recovery_csv_urls, "recovery_csv_urls")
            for i, recovery_csv_url in enumerate(self.recovery_csv_urls):
                validators.validate_string(recovery_csv_url, f"{recovery_csv_url}")

        if self.pipeline_name is not None:
            validators.validate_string(self.pipeline_name, "pipeline_name")

        validators.validate_bool(self.move_ws_messages, "move_ws_messages")

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
                 individuals_upload_path, analysis_graphs_dir):
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
        :param analysis_graphs_dir: Directory in the Drive service account's "Shared with Me" directory to upload the
                                    analysis graphs from this pipeline run to.
        :type analysis_graphs_dir: str
        """
        self.drive_credentials_file_url = drive_credentials_file_url
        self.production_upload_path = production_upload_path
        self.messages_upload_path = messages_upload_path
        self.individuals_upload_path = individuals_upload_path
        self.analysis_graphs_dir = analysis_graphs_dir

        self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        drive_credentials_file_url = configuration_dict["DriveCredentialsFileURL"]
        production_upload_path = configuration_dict["ProductionUploadPath"]
        messages_upload_path = configuration_dict["MessagesUploadPath"]
        individuals_upload_path = configuration_dict["IndividualsUploadPath"]
        analysis_graphs_dir = configuration_dict["AnalysisGraphsDir"]

        return cls(drive_credentials_file_url, production_upload_path, messages_upload_path,
                   individuals_upload_path, analysis_graphs_dir)

    def validate(self):
        validators.validate_string(self.drive_credentials_file_url, "drive_credentials_file_url")
        assert urlparse(self.drive_credentials_file_url).scheme == "gs", "DriveCredentialsFileURL needs to be a gs " \
                                                                         "URL (i.e. of the form gs://bucket-name/file)"

        validators.validate_string(self.production_upload_path, "production_upload_path")
        validators.validate_string(self.messages_upload_path, "messages_upload_path")
        validators.validate_string(self.individuals_upload_path, "individuals_upload_path")
        validators.validate_string(self.analysis_graphs_dir, "analysis_graphs_dir")
