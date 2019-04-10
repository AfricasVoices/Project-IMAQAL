from core_data_modules.traced_data import TracedData


class CombineRawDatasets(object):
    @staticmethod
    def combine_raw_datasets(user, messages_datasets, demogs_datasets, surveys_datasets):
        data = []

        for messages_dataset in messages_datasets:
            data.extend(messages_dataset)

        for demogs_dataset in demogs_datasets:
            TracedData.update_iterable(user, "avf_phone_id", data, demogs_dataset, "demog_responses")
    
        for surveys_dataset in surveys_datasets:
            data.extend(surveys_dataset)
            
        return data
