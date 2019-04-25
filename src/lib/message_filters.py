from core_data_modules.logging import Logger
from dateutil.parser import isoparse
from core_data_modules.util import SHAUtils 

from src.lib import PipelineConfiguration

log = Logger(__name__)

# TODO: Move to Core once adapted for and tested on a pipeline that supports multiple radio shows
class MessageFilters(object):
    # TODO: Log which data is being dropped?
    @staticmethod
    def filter_test_messages(messages, test_run_key="test_run"):
        filtered = [td for td in messages if not td.get(test_run_key, False)]
        log.info(f"Filtered test messages. Dropped {len(messages) - len(filtered)}/{len(messages)} total.")
        return filtered

    @staticmethod
    def filter_empty_messages(messages, message_keys):
        # TODO: Before using on future projects, consider whether messages which are "" should be considered as empty
        non_empty = []
        for td in messages:
            for message_key in message_keys:
                if message_key in td:
                    non_empty.append(td)
                    continue
        return non_empty

    @staticmethod
    def filter_time_range(messages, time_key, start_time, end_time):
        return [td for td in messages if start_time <= isoparse(td.get(time_key)) <= end_time]

    @staticmethod
    def filter_noise(messages, message_key, noise_fn):
        return [td for td in messages if not noise_fn(td.get(message_key))]
    
    @staticmethod
    def subsample_messages_by_uid(messages):
        '''
        Generates sample messages 
        
        :param: messages: TracedData objects to sample
        :type traced_data: list of TracedData
        :return: sample of the TracedData objects
        :rtype: list of TracedData
        '''

        subsample_data = []
        for td in messages:
            if int(SHAUtils.sha_string(td["uid"])[0], 16) < PipelineConfiguration.SUBSAMPLING_THRESHOLD:
                subsample_data.append(td)
        log.info(f"Sample messages generated "
                 f"{len(subsample_data)}/{len(messages)} total messages.")        
        return subsample_data
