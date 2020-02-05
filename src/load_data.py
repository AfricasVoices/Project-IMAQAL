from core_data_modules.logging import Logger
from core_data_modules.traced_data import TracedData, Metadata
from core_data_modules.traced_data.io import TracedDataJsonIO
from core_data_modules.util import TimeUtils

log = Logger(__name__)


class LoadData(object):
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
    def combine_raw_datasets(user, messages_datasets, surveys_datasets):
        data = []

        for messages_dataset in messages_datasets:
            data.extend(messages_dataset)

        for surveys_dataset in surveys_datasets:
            TracedData.update_iterable(user, "avf_phone_id", data, surveys_dataset, "survey_responses")

        return data

    @classmethod
    def load_raw_data(cls, user, raw_data_dir, pipeline_configuration):

        # Load Activation datasets
        log.info("Loading activation datasets...")
        activation_datasets = []
        for i, activation_flow_name in enumerate(pipeline_configuration.activation_flow_names):
            raw_activation_path = f"{raw_data_dir}/{activation_flow_name}.jsonl"
            log.info(f"Loading {raw_activation_path}...")
            with open(raw_activation_path, "r") as f:
                messages = TracedDataJsonIO.import_jsonl_to_traced_data_iterable(f)
            log.info(f"Loaded {len(messages)} runs")
            activation_datasets.append(messages)

        recovery_datasets = []
        if pipeline_configuration.recovery_csv_urls is None:
            log.debug(
                "Not loading any recovery datasets (because the pipeline configuration json does not contain the key "
                "'RecoveryCSVURLs')")
        else:
            log.info("Loading recovery datasets...")
            for i, recovery_csv_url in enumerate(pipeline_configuration.recovery_csv_urls):
                raw_recovery_path = f"{raw_data_dir}/{recovery_csv_url.split('/')[-1].split('.')[0]}.json"
                log.info(f"Loading {raw_recovery_path}...")
                with open(raw_recovery_path, "r") as f:
                    messages = TracedDataJsonIO.import_jsonl_to_traced_data_iterable(f)
                log.info(f"Loaded {len(messages)} runs")
                recovery_datasets.append(messages)

        # Load Follow up Surveys
        log.info("Loading demographics and follow up surveys...")
        survey_datasets = []
        for i, survey_flow_name in enumerate(pipeline_configuration.survey_flow_names):
            raw_survey_up_path = f"{raw_data_dir}/{survey_flow_name}.jsonl"
            log.info(f"Loading {raw_survey_up_path}...")
            with open(raw_survey_up_path, "r") as f:
                contacts = TracedDataJsonIO.import_jsonl_to_traced_data_iterable(f)
            log.info(f"Loaded {len(contacts)} contacts")
            survey_datasets.append(contacts)

        # Add survey data to the messages
        log.info("Combining Datasets...")
        coalesced_survey_datasets = []
        for dataset in survey_datasets:
            coalesced_survey_datasets.append(cls.coalesce_traced_runs_by_key(user, dataset, "avf_phone_id"))

        data = cls.combine_raw_datasets(user, activation_datasets + recovery_datasets, coalesced_survey_datasets)

        return data
