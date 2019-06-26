from core_data_modules.traced_data import TracedData


class CombineRawDatasets(object):
    @staticmethod
    def combine_raw_datasets(user, messages_datasets, follow_up_survey_datasets, demogs_datasets):
        data = []

        for messages_dataset in messages_datasets:
            data.extend(messages_dataset)

        for follow_up_survey_dataset in follow_up_survey_datasets:
            data.extend(follow_up_survey_dataset)

        for demogs_dataset in demogs_datasets:
            TracedData.update_iterable(user, "avf_phone_id", data, demogs_dataset, "demog_responses")

        return data
