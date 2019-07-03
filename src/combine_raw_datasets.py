from core_data_modules.traced_data import TracedData, Metadata
from core_data_modules.util import TimeUtils


class CombineRawDatasets(object):
    @staticmethod
    def coalesce_traced_runs_by_key(user, traced_runs, coalesce_key):
        coalesced_runs = dict()

        for run in traced_runs:
            if run[coalesce_key] not in coalesced_runs:
                coalesced_runs[run[coalesce_key]] = run
            else:
                coalesced_runs[run[coalesce_key]].append_data(
                    dict(run.items()), Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))

        return list(coalesced_runs.values())

    @staticmethod
    def combine_raw_datasets(user, messages_datasets, follow_up_survey_datasets, coalesced_demog_datasets):
        data = []

        for messages_dataset in messages_datasets:
            data.extend(messages_dataset)

        for follow_up_survey_dataset in follow_up_survey_datasets:
            data.extend(follow_up_survey_dataset)

        for demog_dataset in coalesced_demog_datasets:
            TracedData.update_iterable(user, "avf_phone_id", data, demog_dataset, "demog_responses")

        return data
