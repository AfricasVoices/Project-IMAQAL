import random
import time
from os import path

from core_data_modules.cleaners import somali
from core_data_modules.logging import Logger
from core_data_modules.traced_data import Metadata
from core_data_modules.traced_data.io import TracedDataCSVIO, TracedDataCodaV2IO
from core_data_modules.util import IOUtils

from src.lib import PipelineConfiguration, MessageFilters, ICRTools, Channels

log = Logger(__name__)

class AutoCodeShowAndFollowupsMessages(object):
    RQA_AND_FOLLOW_UP_KEYS = []
    for plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.FOLLOW_UP_CODING_PLANS:
        RQA_AND_FOLLOW_UP_KEYS.append(plan.raw_field)

    SENT_ON_KEY = "sent_on"
    NOISE_KEY = "noise"
    ICR_MESSAGES_COUNT = 200
    ICR_SEED = 0

    def log_empty_string_stats(data, raw_fields):
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
    def auto_code_show_and_followups_messages(cls, user, data, icr_output_dir, coda_output_dir):
        # Filter out test messages sent by AVF.
        if not PipelineConfiguration.DEV_MODE:
            data = MessageFilters.filter_test_messages(data)
        
        # Filter for runs which don't contain a response to any week's question or the follow-up questions
        data = MessageFilters.filter_empty_messages(data, cls.RQA_AND_FOLLOW_UP_KEYS)

        # Filter out runs sent outwith the project start and end dates
        data = MessageFilters.filter_time_range(
            data, cls.SENT_ON_KEY, PipelineConfiguration.PROJECT_START_DATE, PipelineConfiguration.PROJECT_END_DATE)
        
        # Tag RQA and follow up messages which are noise as being noise
        for td in data:
            is_noise = True
            for rqa_key in cls.RQA_AND_FOLLOW_UP_KEYS:
                if rqa_key in td and not somali.DemographicCleaner.is_noise(td[rqa_key], min_length=10):
                    is_noise = False
            td.append_data({cls.NOISE_KEY: is_noise}, Metadata(user, Metadata.get_call_location(), time.time()))
        
        # Label each message with channel keys
        Channels.set_channel_keys(user, data, cls.SENT_ON_KEY)

        # Filter for messages which aren't noise (in order to export to Coda and export for ICR)
        not_noise = MessageFilters.filter_noise(data, cls.NOISE_KEY, lambda x: x)

        # Compute the number of RQA messages that were the empty string
        log.debug("Counting the number of empty string messages for each raw radio show and follow up survey field...")
        raw_rqa_and_follow_up_fields = []
        for plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.FOLLOW_UP_CODING_PLANS:
            if plan.raw_field not in raw_rqa_and_follow_up_fields:
                raw_rqa_and_follow_up_fields.append(plan.raw_field)
        cls.log_empty_string_stats(data, raw_rqa_and_follow_up_fields)

        # Compute the number of demog messages that were the empty string
        log.debug("Counting the number of empty string messages for each demog field...")
        raw_demog_fields = []
        for plan in PipelineConfiguration.DEMOG_CODING_PLANS:
            if plan.raw_field not in raw_demog_fields:
                raw_demog_fields.append(plan.raw_field)
        demog_data = dict()
        for td in data:
            demog_data[td["uid"]] = td
        cls.log_empty_string_stats(demog_data.values(), raw_demog_fields)
        
        # Output all RQA and Follow Up surveys messages which aren't noise to Coda
        IOUtils.ensure_dirs_exist(coda_output_dir)
        for plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.FOLLOW_UP_CODING_PLANS:
            TracedDataCodaV2IO.compute_message_ids(user, not_noise, plan.raw_field, plan.id_field)

            output_path = path.join(coda_output_dir, plan.coda_filename)
            with open(output_path, "w") as f:
                TracedDataCodaV2IO.export_traced_data_iterable_to_coda_2(
                    not_noise, plan.raw_field, cls.SENT_ON_KEY, plan.id_field, {}, f
                )
        
        #Subsample messages for export to coda
        subsample_data = MessageFilters.subsample_messages_by_uid(not_noise)

        # Output RQA and Follow Up subsample messages to Coda
        IOUtils.ensure_dirs_exist(coda_output_dir)
        for plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.FOLLOW_UP_CODING_PLANS:
            TracedDataCodaV2IO.compute_message_ids(user, subsample_data, plan.raw_field, plan.id_field)

            output_path = path.join(coda_output_dir, f'sub_sample_{plan.coda_filename}')
            with open(output_path, "w") as f:
                TracedDataCodaV2IO.export_traced_data_iterable_to_coda_2(
                    subsample_data, plan.raw_field, cls.SENT_ON_KEY, plan.id_field, {}, f
                )
 
        # Output RQA and Follow Up surveys messages for ICR
        IOUtils.ensure_dirs_exist(icr_output_dir)
        for plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.FOLLOW_UP_CODING_PLANS:
            rqa_and_follow_up_messages = []
            # This test works because the only codes which have been applied at this point are TRUE_MISSING.
            # If any other coding is done above, this test will need to change
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

        return data
