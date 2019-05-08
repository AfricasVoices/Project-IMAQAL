import time
from os import path

from core_data_modules.cleaners import Codes, PhoneCleaner
from core_data_modules.cleaners.cleaning_utils import CleaningUtils
from core_data_modules.traced_data import Metadata
from core_data_modules.traced_data.io import TracedDataCodaV2IO
from core_data_modules.util import IOUtils

from src.lib.pipeline_configuration import CodeSchemes, PipelineConfiguration
from src.lib.message_filters import MessageFilters


class AutoCodeDemogs(object):
    SENT_ON_KEY = "sent_on"

    @classmethod
    def auto_code_demogs(cls, user, data, phone_uuid_table, coda_output_dir):
        # Auto-code surveys
        for plan in PipelineConfiguration.DEMOG_CODING_PLANS:
            if plan.cleaner is not None:
                CleaningUtils.apply_cleaner_to_traced_data_iterable(user, data, plan.raw_field, plan.coded_field,
                                                                    plan.cleaner, plan.code_scheme)

        # For any locations where the cleaners assigned a code to a sub district, set the district code to NC
        # (this is because only one column should have a value set in Coda)
        for td in data:
            if "mogadishu_sub_district_coded" in td:
                mogadishu_code_id = td["mogadishu_sub_district_coded"]["CodeID"]
                if CodeSchemes.MOGADISHU_SUB_DISTRICT.get_code_with_id(mogadishu_code_id).code_type == "Normal":
                    nc_label = CleaningUtils.make_label_from_cleaner_code(
                        CodeSchemes.MOGADISHU_SUB_DISTRICT,
                        CodeSchemes.MOGADISHU_SUB_DISTRICT.get_code_with_control_code(Codes.NOT_CODED),
                        Metadata.get_call_location(),
                    )
                    td.append_data({"district_coded": nc_label.to_dict()},
                                   Metadata(user, Metadata.get_call_location(), time.time()))

        # Set operator from phone number
        for td in data:
            operator_clean = PhoneCleaner.clean_operator(phone_uuid_table.get_phone(td["uid"]))
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

        # Subsample messages for export to coda
        subsample_data = MessageFilters.subsample_messages_by_uid(data)

        # Output single-scheme subsample answers to coda for manual verification + coding
        IOUtils.ensure_dirs_exist(coda_output_dir)
        for plan in PipelineConfiguration.DEMOG_CODING_PLANS:
            if plan.raw_field == "location_raw":
                continue

            TracedDataCodaV2IO.compute_message_ids(user, subsample_data, plan.raw_field, plan.id_field)

            coda_output_path = path.join(coda_output_dir, f'sub_sample_{plan.coda_filename}')
            with open(coda_output_path, "w") as f:
                TracedDataCodaV2IO.export_traced_data_iterable_to_coda_2(
                    subsample_data, plan.raw_field, plan.time_field, plan.id_field,
                    {plan.coded_field: plan.code_scheme}, f
                )

        # Output subsample location scheme to coda for manual verification + coding
        output_path = path.join(coda_output_dir, "sub_sample_location.json")
        TracedDataCodaV2IO.compute_message_ids(user, subsample_data, "location_raw", "location_raw_id")
        with open(output_path, "w") as f:
            TracedDataCodaV2IO.export_traced_data_iterable_to_coda_2(
                subsample_data, "location_raw", "location_time", "location_raw_id",
                {"mogadishu_sub_district_coded": CodeSchemes.MOGADISHU_SUB_DISTRICT,
                 "district_coded": CodeSchemes.SOMALIA_DISTRICT,
                 "region_coded": CodeSchemes.SOMALIA_REGION,
                 "state_coded": CodeSchemes.SOMALIA_STATE,
                 "zone_coded": CodeSchemes.SOMALIA_ZONE}, f
            )

        # Output single-scheme answers to coda for manual verification + coding
        IOUtils.ensure_dirs_exist(coda_output_dir)
        for plan in PipelineConfiguration.DEMOG_CODING_PLANS:
            if plan.raw_field == "location_raw":
                continue

            TracedDataCodaV2IO.compute_message_ids(user, data, plan.raw_field, plan.id_field)

            coda_output_path = path.join(coda_output_dir, plan.coda_filename)
            with open(coda_output_path, "w") as f:
                TracedDataCodaV2IO.export_traced_data_iterable_to_coda_2(
                    data, plan.raw_field, plan.time_field, plan.id_field, {plan.coded_field: plan.code_scheme}, f
                )

        # Output location scheme to coda for manual verification + coding
        output_path = path.join(coda_output_dir, "location.json")
        TracedDataCodaV2IO.compute_message_ids(user, data, "location_raw", "location_raw_id")
        with open(output_path, "w") as f:
            TracedDataCodaV2IO.export_traced_data_iterable_to_coda_2(
                data, "location_raw", "location_time", "location_raw_id",
                {"mogadishu_sub_district_coded": CodeSchemes.MOGADISHU_SUB_DISTRICT,
                 "district_coded": CodeSchemes.SOMALIA_DISTRICT,
                 "region_coded": CodeSchemes.SOMALIA_REGION,
                 "state_coded": CodeSchemes.SOMALIA_STATE,
                 "zone_coded": CodeSchemes.SOMALIA_ZONE}, f
            )

        return data
