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
    S01E03_YES_NO = _open_scheme("s01e03_yes_no.json")
    S01E04 = _open_scheme("s01e04.json")
    S01E05 = _open_scheme("s01e05.json")
    S01E05_YES_NO = _open_scheme("s01e05_yes_no.json")
    S01E06 = _open_scheme("s01e06.json")
    S01E06_YES_NO = _open_scheme("s01e06_yes_no.json")
    S01E07 = _open_scheme("s01e07.json")
    S01E07_YES_NO = _open_scheme("s01e07_yes_no.json")

    WOMEN_PARTICIPATION = _open_scheme("women_participation.json")
    WOMEN_PARTICIPATION_YES_NO_AMB = _open_scheme("women_participation_yes_no_amb.json")

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
                 run_id_field=None, icr_filename=None, analysis_file_key=None, id_field=None,
                 binary_code_scheme=None, binary_coded_field=None, binary_analysis_file_key=None):
        self.raw_field = raw_field
        self.coded_field = coded_field
        self.coda_filename = coda_filename
        self.icr_filename = icr_filename
        self.cleaner = cleaner
        self.code_scheme = code_scheme
        self.time_field = time_field
        self.run_id_field = run_id_field
        self.analysis_file_key = analysis_file_key
        self.binary_code_scheme = binary_code_scheme
        self.binary_coded_field = binary_coded_field
        self.binary_analysis_file_key = binary_analysis_file_key

        if id_field is None:
            id_field = "{}_id".format(self.raw_field)
        self.id_field = id_field

class PipelineConfiguration(object):
    DEV_MODE = False

    SUBSAMPLING_THRESHOLD = 4 # /16 (a fraction of hex) subsample of data
    
    PROJECT_START_DATE = isoparse("2019-04-19T00:00:00+03:00")
    #TODO revise this as the project nears the end
    PROJECT_END_DATE = isoparse("2020-02-15T09:00:00+03:00")

    RQA_CODING_PLANS = [
        CodingPlan(raw_field="rqa_s01e01_raw",
                   coded_field="rqa_s01e01_coded",
                   time_field="sent_on",
                   coda_filename="s01e01.json",
                   icr_filename="s01e01.csv",
                   run_id_field="rqa_s01e01_run_id",
                   analysis_file_key="rqa_s01e01_",
                   cleaner=None,
                   code_scheme=CodeSchemes.S01E01),

        CodingPlan(raw_field="rqa_s01e02_raw",
                   coded_field="rqa_s01e02_coded",
                   time_field="sent_on",
                   coda_filename="s01e02.json",
                   icr_filename="s01e02.csv",
                   run_id_field="rqa_s01e02_run_id",
                   analysis_file_key="rqa_s01e02_",
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
                   cleaner=None,
                   code_scheme=CodeSchemes.S01E03,
                   binary_code_scheme=CodeSchemes.S01E03_YES_NO,
                   binary_coded_field="rqa_s01e03_yes_no_coded",
                   binary_analysis_file_key="rqa_s01e03_yes_no"),

        CodingPlan(raw_field="rqa_s01e04_raw",
                   coded_field="rqa_s01e04_coded",
                   time_field="sent_on",
                   coda_filename="s01e04.json",
                   icr_filename="s01e04.csv",
                   run_id_field="rqa_s01e04_run_id",
                   analysis_file_key="rqa_s01e04_",
                   cleaner=None,
                   code_scheme=CodeSchemes.S01E04),

        CodingPlan(raw_field="rqa_s01e05_raw",
                   coded_field="rqa_s01e05_coded",
                   time_field="sent_on",
                   coda_filename="s01e05.json",
                   icr_filename="s01e05.csv",
                   run_id_field="rqa_s01e05_run_id",
                   analysis_file_key="rqa_s01e05_",
                   cleaner=None,
                   code_scheme=CodeSchemes.S01E05,
                   binary_code_scheme=CodeSchemes.S01E05_YES_NO,
                   binary_coded_field="rqa_s01e05_yes_no_coded",
                   binary_analysis_file_key="rqa_s01e05_yes_no"),

        CodingPlan(raw_field="rqa_s01e06_raw",
                   coded_field="rqa_s01e06_coded",
                   time_field="sent_on",
                   coda_filename="s01e06.json",
                   icr_filename="s01e06.csv",
                   run_id_field="rqa_s01e06_run_id",
                   analysis_file_key="rqa_s01e06_",
                   cleaner=None,
                   code_scheme=CodeSchemes.S01E06,
                   binary_code_scheme=CodeSchemes.S01E06_YES_NO,
                   binary_coded_field="rqa_s01e06_yes_no_coded",
                   binary_analysis_file_key="rqa_s01e06_yes_no"),
        
        CodingPlan(raw_field="rqa_s01e07_raw",
                   coded_field="rqa_s01e07_coded",
                   time_field="sent_on",
                   coda_filename="s01e07.json",
                   icr_filename="s01e07.csv",
                   run_id_field="rqa_s01e07_run_id",
                   analysis_file_key="rqa_s01e07_",
                   cleaner=None,
                   code_scheme=CodeSchemes.S01E07,
                   binary_code_scheme=CodeSchemes.S01E07_YES_NO,
                   binary_coded_field="rqa_s01e07_yes_no_coded",
                   binary_analysis_file_key="rqa_s01e07_yes_no"),
    ]

    FOLLOW_UP_CODING_PLANS = [

        CodingPlan(raw_field="women_participation_raw",
                   coded_field="women_participation_coded",
                   time_field="sent_on",
                   coda_filename="women_participation.json",
                   icr_filename="women_participation.csv",
                   run_id_field="women_participation_run_id",
                   analysis_file_key="women_participation_",
                   cleaner=None,
                   code_scheme=CodeSchemes.WOMEN_PARTICIPATION,
                   binary_code_scheme=CodeSchemes.WOMEN_PARTICIPATION_YES_NO_AMB,
                   binary_coded_field="women_participation_yes_no_amb_coded",
                   binary_analysis_file_key="women_participation_yes_no_amb"),
    ]

    LOCATION_CODING_PLANS = [

        CodingPlan(raw_field="location_raw",
                   id_field="location_raw_id",
                   coded_field="mogadishu_sub_district_coded",
                   time_field="location_time",
                   coda_filename="location.json",
                   analysis_file_key=None,
                   cleaner=None,
                   code_scheme=CodeSchemes.MOGADISHU_SUB_DISTRICT),

        CodingPlan(raw_field="location_raw",
                   id_field="location_raw_id",
                   coded_field="district_coded",
                   time_field="location_time",
                   coda_filename="location.json",
                   analysis_file_key="district",
                   cleaner=somali.DemographicCleaner.clean_somalia_district,
                   code_scheme=CodeSchemes.SOMALIA_DISTRICT),

        CodingPlan(raw_field="location_raw",
                   id_field="location_raw_id",
                   coded_field="region_coded",
                   time_field="location_time",
                   coda_filename="location.json",
                   analysis_file_key="region",
                   cleaner=None,
                   code_scheme=CodeSchemes.SOMALIA_REGION),

        CodingPlan(raw_field="location_raw",
                   id_field="location_raw_id",
                   coded_field="state_coded",
                   time_field="location_time",
                   coda_filename="location.json",
                   analysis_file_key="state",
                   cleaner=None,
                   code_scheme=CodeSchemes.SOMALIA_STATE),

        CodingPlan(raw_field="location_raw",
                   id_field="location_raw_id",
                   coded_field="zone_coded",
                   time_field="location_time",
                   coda_filename="location.json",
                   analysis_file_key="zone",
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
                   cleaner=somali.DemographicCleaner.clean_gender,
                   code_scheme=CodeSchemes.GENDER),

        CodingPlan(raw_field="age_raw",
                   coded_field="age_coded",
                   time_field="age_time",
                   coda_filename="age.json",
                   analysis_file_key="age",
                   cleaner=lambda text: PipelineConfiguration.clean_age_with_range_filter(text),
                   code_scheme=CodeSchemes.AGE),

        CodingPlan(raw_field="recently_displaced_raw",
                   coded_field="recently_displaced_coded",
                   time_field="recently_displaced_time",
                   coda_filename="recently_displaced.json",
                   analysis_file_key="recently_displaced",
                   cleaner=somali.DemographicCleaner.clean_yes_no,
                   code_scheme=CodeSchemes.RECENTLY_DISPLACED),

        CodingPlan(raw_field="household_language_raw",
                   coded_field="household_language_coded",
                   time_field="household_language_time",
                   coda_filename="household_language.json",
                   analysis_file_key="household_language",
                   cleaner=None,
                   code_scheme=CodeSchemes.HOUSEHOLD_LANGUAGE),

        CodingPlan(raw_field="location_raw",
                   id_field="location_raw_id",
                   coded_field="district_coded",
                   time_field="location_time",
                   coda_filename="location.json",
                   analysis_file_key="district",
                   cleaner=somali.DemographicCleaner.clean_somalia_district,
                   code_scheme=CodeSchemes.SOMALIA_DISTRICT),
    ]

    DEMOG_CODING_PLANS.extend(LOCATION_CODING_PLANS)

    def __init__(self, rapid_pro_domain, rapid_pro_token_file_url, rapid_pro_test_contact_uuids,
                 rapid_pro_key_remappings, drive_upload=None):
        """
        :param rapid_pro_domain: URL of the Rapid Pro server to download data from.
        :type rapid_pro_domain: str
        :param rapid_pro_token_file_url: GS URL of a text file containing the authorisation token for the Rapid Pro
                                         server.
        :type rapid_pro_token_file_url: str
        :param rapid_pro_test_contact_uuids: Rapid Pro contact UUIDs of test contacts.
                                             Runs for any of those test contacts will be tagged with {'test_run': True},
                                             and dropped when the pipeline is in production mode.
        :type rapid_pro_test_contact_uuids: list of str
        :param rapid_pro_key_remappings: List of rapid_pro_key -> pipeline_key remappings.
        :type rapid_pro_key_remappings: list of RapidProKeyRemapping
        :param drive_upload: Configuration for uploading to Google Drive, or None.
                             If None, does not upload to Google Drive.
        :type drive_upload: DriveUploadPaths | None
        """
        self.rapid_pro_domain = rapid_pro_domain
        self.rapid_pro_token_file_url = rapid_pro_token_file_url
        self.rapid_pro_test_contact_uuids = rapid_pro_test_contact_uuids
        self.rapid_pro_key_remappings = rapid_pro_key_remappings
        self.drive_upload = drive_upload

        self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        rapid_pro_domain = configuration_dict["RapidProDomain"]
        rapid_pro_token_file_url = configuration_dict["RapidProTokenFileURL"]
        rapid_pro_test_contact_uuids = configuration_dict["RapidProTestContactUUIDs"]

        rapid_pro_key_remappings = []
        for remapping_dict in configuration_dict["RapidProKeyRemappings"]:
            rapid_pro_key_remappings.append(RapidProKeyRemapping.from_configuration_dict(remapping_dict))

        drive_upload_paths = None
        if "DriveUpload" in configuration_dict:
            drive_upload_paths = DriveUpload.from_configuration_dict(configuration_dict["DriveUpload"])

        return cls(rapid_pro_domain, rapid_pro_token_file_url, rapid_pro_test_contact_uuids,
                   rapid_pro_key_remappings, drive_upload_paths)

    @classmethod
    def from_configuration_file(cls, f):
        return cls.from_configuration_dict(json.load(f))
    
    def validate(self):
        validators.validate_string(self.rapid_pro_domain, "rapid_pro_domain")
        validators.validate_string(self.rapid_pro_token_file_url, "rapid_pro_token_file_url")

        validators.validate_list(self.rapid_pro_test_contact_uuids, "rapid_pro_test_contact_uuids")
        for i, contact_uuid in enumerate(self.rapid_pro_test_contact_uuids):
            validators.validate_string(contact_uuid, f"rapid_pro_test_contact_uuids[{i}]")

        validators.validate_list(self.rapid_pro_key_remappings, "rapid_pro_key_remappings")
        for i, remapping in enumerate(self.rapid_pro_key_remappings):
            assert isinstance(remapping, RapidProKeyRemapping), \
                f"rapid_pro_key_mappings[{i}] is not of type RapidProKeyRemapping"
            remapping.validate()

        if self.drive_upload is not None:
            assert isinstance(self.drive_upload, DriveUpload), \
                "drive_upload is not of type DriveUpload"
            self.drive_upload.validate()


class RapidProKeyRemapping(object):
    def __init__(self, rapid_pro_key, pipeline_key):
        """
        :param rapid_pro_key: Name of key in the dataset exported via RapidProTools.
        :type rapid_pro_key: str
        :param pipeline_key: Name to use for that key in the rest of the pipeline.
        :type pipeline_key: str
        """
        self.rapid_pro_key = rapid_pro_key
        self.pipeline_key = pipeline_key
        
        self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        rapid_pro_key = configuration_dict["RapidProKey"]
        pipeline_key = configuration_dict["PipelineKey"]
        
        return cls(rapid_pro_key, pipeline_key)
    
    def validate(self):
        validators.validate_string(self.rapid_pro_key, "rapid_pro_key")
        validators.validate_string(self.pipeline_key, "pipeline_key")


class DriveUpload(object):
    def __init__(self, drive_credentials_file_url, production_upload_path, messages_upload_path,
                 individuals_upload_path, traced_data_upload_path):
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
        :param traced_data_upload_path: Path in the Drive service account's "Shared with Me" directory to upload the
                                        serialized TracedData from this pipeline run to.
        :type traced_data_upload_path: str
        """
        self.drive_credentials_file_url = drive_credentials_file_url
        self.production_upload_path = production_upload_path
        self.messages_upload_path = messages_upload_path
        self.individuals_upload_path = individuals_upload_path
        self.traced_data_upload_path = traced_data_upload_path

        self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        drive_credentials_file_url = configuration_dict["DriveCredentialsFileURL"]
        production_upload_path = configuration_dict["ProductionUploadPath"]
        messages_upload_path = configuration_dict["MessagesUploadPath"]
        individuals_upload_path = configuration_dict["IndividualsUploadPath"]
        traced_data_upload_path = configuration_dict["TracedDataUploadPath"]

        return cls(drive_credentials_file_url, production_upload_path, messages_upload_path,
                   individuals_upload_path, traced_data_upload_path)

    def validate(self):
        validators.validate_string(self.drive_credentials_file_url, "drive_credentials_file_url")
        assert urlparse(self.drive_credentials_file_url).scheme == "gs", "DriveCredentialsFileURL needs to be a gs " \
                                                                         "URL (i.e. of the form gs://bucket-name/file)"

        validators.validate_string(self.production_upload_path, "production_upload_path")
        validators.validate_string(self.messages_upload_path, "messages_upload_path")
        validators.validate_string(self.individuals_upload_path, "individuals_upload_path")
        validators.validate_string(self.traced_data_upload_path, "traced_data_upload_path")
