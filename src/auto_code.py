import random
import time
from os import path

from core_data_modules.cleaners import Codes, PhoneCleaner
from core_data_modules.cleaners import somali
from core_data_modules.cleaners.cleaning_utils import CleaningUtils
from core_data_modules.logging import Logger
from core_data_modules.traced_data.io import TracedDataCSVIO, TracedDataCodaV2IO
from core_data_modules.util import IOUtils
from core_data_modules.traced_data import Metadata

from src.lib import PipelineConfiguration, MessageFilters, ICRTools, Channels, CodeSchemes

log = Logger(__name__)

class AutoCode(object):

    SENT_ON_KEY = "sent_on"
    NOISE_KEY = "noise"
    ICR_MESSAGES_COUNT = 200
    ICR_SEED = 0

    @staticmethod
    def log_empty_string_stats_for_field(data, raw_fields):
        for raw_field in raw_fields:
            total_messages_count = 0
            empty_string_messages_count = 0

            for td in data:
                if raw_field in td:
                    total_messages_count += 1
                    if td[raw_field] == "":
                        empty_string_messages_count += 1

            log.debug(f"{raw_field}: {empty_string_messages_count} messages were \"\", out "
                      f"of {total_messages_count} total")

    @classmethod
    def log_empty_string_stats(cls, data):
        # Compute the number of RQA messages that were the empty string
        log.debug("Counting the number of empty string messages for each raw radio show field...")
        raw_rqa_fields = []
        for plan in PipelineConfiguration.RQA_CODING_PLANS:
            if plan.raw_field not in raw_rqa_fields:
                raw_rqa_fields.append(plan.raw_field)
        cls.log_empty_string_stats_for_field(data, raw_rqa_fields)

        # Compute the number of survey messages that were the empty string
        log.debug("Counting the number of empty string messages for each survey field...")
        raw_survey_fields = []
        for plan in PipelineConfiguration.FOLLOW_UP_CODING_PLANS + PipelineConfiguration.DEMOG_CODING_PLANS:
            if plan.raw_field not in raw_survey_fields:
                raw_survey_fields.append(plan.raw_field)
        survey_data = dict()
        for td in data:
            survey_data[td["uid"]] = td
        cls.log_empty_string_stats_for_field(survey_data.values(), raw_survey_fields)

    @classmethod
    def filter_messages(cls, user, data):
        # Filter out test messages sent by AVF.
        if not PipelineConfiguration.DEV_MODE:
            data = MessageFilters.filter_test_messages(data)
        else:
            log.debug("Not filtering out test messages (because the pipeline configuration json key "
                      "'FilterTestMessages' was set to false)")

        # Filter for runs which don't contain a response to any week's question
        data = MessageFilters.filter_empty_messages(data,
                                                    [plan.raw_field for plan in PipelineConfiguration.RQA_CODING_PLANS])

        # Filter out runs sent outwith the project start and end dates
        time_keys = {plan.time_field for plan in PipelineConfiguration.RQA_CODING_PLANS}
        data = MessageFilters.filter_time_range(data, time_keys, PipelineConfiguration.PROJECT_START_DATE,
                                                PipelineConfiguration.PROJECT_END_DATE)

        # Tag RQA messages which are noise as being noise
        rqa_keys = []
        for plan in PipelineConfiguration.RQA_CODING_PLANS:
            rqa_keys.append(plan.raw_field)

        for td in data:
            is_noise = True
            for rqa_key in rqa_keys:
                if rqa_key in td and not somali.DemographicCleaner.is_noise(td[rqa_key], min_length=10):
                    is_noise = False
            td.append_data({cls.NOISE_KEY: is_noise}, Metadata(user, Metadata.get_call_location(), time.time()))


        # Filter for messages which aren't noise (in order to export to Coda and export for ICR)
        data = MessageFilters.filter_noise(data, cls.NOISE_KEY, lambda x: x)

        # Label each message with channel keys
        Channels.set_channel_keys(user, data, cls.SENT_ON_KEY)

        return data

    @classmethod
    def run_cleaners(cls, user, phone_number_uuid_table, data):
        for plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.FOLLOW_UP_CODING_PLANS + \
                    PipelineConfiguration.DEMOG_CODING_PLANS:
            for cc in plan.coding_configurations:
                if cc.cleaner is not None:
                    CleaningUtils.apply_cleaner_to_traced_data_iterable(user, data, plan.raw_field, cc.coded_field,
                                                                        cc.cleaner, cc.code_scheme)

        # Create a look-up table of uuids to phone numbers for all the uuids in the dataset
        uuids = set()
        for td in data:
            uuids.add(td["uid"])
        uuid_to_phone_lut = phone_number_uuid_table.uuid_to_data_batch(uuids)

        # Set the operator codes for each message, using the uuid -> phone number look-up table
        for td in data:
            operator_clean = PhoneCleaner.clean_operator(uuid_to_phone_lut[td["uid"]])
            if operator_clean == Codes.NOT_CODED:
                label = CleaningUtils.make_label_from_cleaner_code(
                    CodeSchemes.SOMALIA_OPERATOR,
                    CodeSchemes.SOMALIA_OPERATOR.get_code_with_control_code(Codes.NOT_CODED),
                    Metadata.get_call_location()
                )
            else:
                label = CleaningUtils.make_label_from_cleaner_code(
                    CodeSchemes.SOMALIA_OPERATOR,
                    CodeSchemes.SOMALIA_OPERATOR.get_code_with_match_value(operator_clean),
                    Metadata.get_call_location()
                )
            td.append_data({"operator_coded": label.to_dict()},
                           Metadata(user, Metadata.get_call_location(), time.time()))

    @classmethod
    def export_coda(cls, user, data, coda_output_dir):
        IOUtils.ensure_dirs_exist(coda_output_dir)
        for plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.DEMOG_CODING_PLANS + \
                    PipelineConfiguration.FOLLOW_UP_CODING_PLANS:
            if plan.coda_filename is None:
                continue

            TracedDataCodaV2IO.compute_message_ids(user, data, plan.raw_field, plan.id_field)

            coda_output_path = path.join(coda_output_dir, plan.coda_filename)
            with open(coda_output_path, "w") as f:
                TracedDataCodaV2IO.export_traced_data_iterable_to_coda_2(
                    data, plan.raw_field, plan.time_field, plan.id_field,
                    {cc.coded_field: cc.code_scheme for cc in plan.coding_configurations},
                    f
                )

    @classmethod
    def export_icr(cls, data, icr_output_dir):
        # Output RQA and Follow up messages for ICR
        IOUtils.ensure_dirs_exist(icr_output_dir)
        for plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.FOLLOW_UP_CODING_PLANS:
            rqa_and_follow_up_messages = []
            for td in data:
                if plan.raw_field in td:
                    rqa_and_follow_up_messages.append(td)

            icr_messages = ICRTools.generate_sample_for_icr(
                rqa_and_follow_up_messages, cls.ICR_MESSAGES_COUNT, random.Random(cls.ICR_SEED))

            icr_output_path = path.join(icr_output_dir, plan.icr_filename)
            with open(icr_output_path, "w") as f:
                TracedDataCSVIO.export_traced_data_iterable_to_csv(
                    icr_messages, f, headers=[plan.run_id_field, plan.raw_field]
                )

    @classmethod
    def auto_code(cls, user, data, phone_number_uuid_table, icr_output_dir, coda_output_dir):
        data = cls.filter_messages(user, data,)

        cls.run_cleaners(user,phone_number_uuid_table, data)
        cls.export_coda(user, data, coda_output_dir)
        cls.export_icr(data, icr_output_dir)
        cls.log_empty_string_stats(data)

        return data

