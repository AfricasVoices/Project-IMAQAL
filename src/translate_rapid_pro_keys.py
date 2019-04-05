from datetime import datetime

import pytz
from core_data_modules.traced_data import Metadata
from core_data_modules.util import TimeUtils
from dateutil.parser import isoparse

class TranslateRapidProKeys(object):
    # TODO: Move the constants in this file to configuration json
    SHOW_ID_MAP = {
        "Rqa_S01E01 (Value) - csap_s02e01_activation": 1,
        "Rqa_S01E02 (Value) - csap_s02e02_activation": 2,
        "Rqa_S01E03 (Value) - csap_s02e03_activation": 3,
        "Rqa_S01E04 (Value) - csap_s02e04_activation": 4,
        "Rqa_S01E05 (Value) - csap_s02e05_activation": 5,
        "Rqa_S01E06 (Value) - csap_s02e06_activation": 6
    }

    RAW_ID_MAP = {
        1: "rqa_s01e01_raw",
        2: "rqa_s01e02_raw",
        3: "rqa_s01e03_raw",
        4: "rqa_s01e04_raw",
        5: "rqa_s01e05_raw",
        6: "rqa_s01e06_raw"
    }

    @classmethod
    def set_show_ids(cls, user, data, show_id_map):
        """
        Sets a show_id for each message, using the presence of Rapid Pro value keys to determine which show each message
        belongs to.

        :param user: Identifier of the user running this program, for TracedData Metadata.
        :type user: str
        :param data: TracedData objects to set the show ids of.
        :type data: iterable of TracedData
        :param show_id_map: Dictionary of Rapid Pro value key to show id.
        :type show_id_map: dict of str -> int
        """
        for td in data:
            show_dict = dict()

            for message_key, show_id in show_id_map.items():
                if message_key in td:
                    assert "rqa_message" not in show_dict
                    show_dict["rqa_message"] = td[message_key]
                    show_dict["show_id"] = show_id

            td.append_data(show_dict, Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))

    @classmethod
    def _remap_radio_show_by_time_range(cls, user, data, time_key, show_id_to_remap_to,
                                        range_start=None, range_end=None, time_to_adjust_to=None):
        """
        Remaps radio show messages received in the given time range to another radio show.

        Optionally adjusts the datetime of re-mapped messages to a constant.

        :param user: Identifier of the user running this program, for TracedData Metadata.
        :type user: str
        :param data: TracedData objects to set the show ids of.
        :type data: iterable of TracedData
        :param time_key: Key in each TracedData of an ISO 8601-formatted datetime string to read the message sent on
                         time from.
        :type time_key: str
        :param show_id_to_remap_to: Show id to assign to messages received within the given time range.
        :type show_id_to_remap_to: int
        :param range_start: Start datetime for the time range to remap radio show messages from, inclusive.
                            If None, defaults to the beginning of time.
        :type range_start: datetime | None
        :param range_end: End datetime for the time range to remap radio show messages from, exclusive.
                          If None, defaults to the end of time.
        :type range_end: datetime | None
        :param time_to_adjust_to: Datetime to assign to the 'sent_on' field of re-mapped shows.
                                  If None, re-mapped shows will not have timestamps re-adjusted.
        :type time_to_adjust_to: datetime | None
        """
        if range_start is None:
            range_start = pytz.utc.localize(datetime.min)
        if range_end is None:
            range_end = pytz.utc.localize(datetime.max)

        for td in data:
            if time_key in td and range_start <= isoparse(td[time_key]) < range_end:
                remapped = {
                    "show_id": show_id_to_remap_to
                }
                if time_to_adjust_to is not None:
                    remapped[time_key] = time_to_adjust_to.isoformat()

                td.append_data(remapped,
                               Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))

    @classmethod
    def remap_radio_shows(cls, user, data, coda_input_dir):
        """
        Remaps radio shows which were in the wrong flow, and therefore have the wrong key/values set, to have the
        key/values they would have had if they had been received by the correct flow.

        :param user: Identifier of the user running this program, for TracedData Metadata.
        :type user: str
        :param data: TracedData objects to move the radio show messages in.
        :type data: iterable of TracedData
        :param coda_input_dir: Directory to read coded coda files from.
        :type coda_input_dir: str
        """
        # No implementation needed yet, because no flow is yet to go wrong in production.
        pass

    @classmethod
    def remap_key_names(cls, user, data, pipeline_configuration):
        """
        Remaps key names.
        
        :param user: Identifier of the user running this program, for TracedData Metadata.
        :type user: str
        :param data: TracedData objects to remap the key names of.
        :type data: iterable of TracedData
        :param pipeline_configuration: Pipeline configuration.
        :type pipeline_configuration: PipelineConfiguration
        """
        for td in data:
            remapped = dict()
               
            for remapping in pipeline_configuration.rapid_pro_key_remappings:
                old_key = remapping.rapid_pro_key
                new_key = remapping.pipeline_key
                
                if old_key in td and new_key not in td:
                    remapped[new_key] = td[old_key]

            td.append_data(remapped, Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))

    @classmethod
    def set_rqa_raw_keys_from_show_ids(cls, user, data, raw_id_map):
        """
        Despite the earlier phases of this pipeline stage using a common 'rqa_message' field and then a 'show_id'
        field to identify which radio show a message belonged to, the rest of the pipeline still uses the presence
        of a raw field for each show to determine which show a message belongs to. This function translates from
        the new 'show_id' method back to the old 'raw field presence` method.
        
        TODO: Update the rest of the pipeline to use show_ids, and/or perform remapping before combining the datasets.

        :param user: Identifier of the user running this program, for TracedData Metadata.
        :type user: str
        :param data: TracedData objects to set raw radio show message fields for.
        :type data: iterable of TracedData
        :param raw_id_map: Dictionary of show id to the rqa message key to assign each td[rqa_message} to.
        :type raw_id_map: dict of int -> str
        """
        for td in data:
            for show_id, message_key in raw_id_map.items():
                if "rqa_message" in td and td.get("show_id") == show_id:
                    td.append_data({message_key: td["rqa_message"]},
                                   Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))

    @classmethod
    def translate_rapid_pro_keys(cls, user, data, pipeline_configuration, coda_input_dir):
        """
        Remaps the keys of rqa messages in the wrong flow into the correct one, and remaps all Rapid Pro keys to
        more usable keys that can be used by the rest of the pipeline.
        
        TODO: Break this function such that the show remapping phase happens in one class, and the Rapid Pro remapping
              in another?
        """
        # Set a show id field for each message, using the presence of Rapid Pro value keys in the TracedData.
        # Show ids are necessary in order to be able to remap radio shows and key names separately (because data
        # can't be 'deleted' from TracedData).
        cls.set_show_ids(user, data, cls.SHOW_ID_MAP)

        # Move rqa messages which ended up in the wrong flow to the correct one.
        cls.remap_radio_shows(user, data, coda_input_dir)

        # Remap the keys used by Rapid Pro to more usable key names that will be used by the rest of the pipeline.
        cls.remap_key_names(user, data, pipeline_configuration)

        # Convert from the new show id format to the raw field format still used by the rest of the pipeline.
        cls.set_rqa_raw_keys_from_show_ids(user, data, cls.RAW_ID_MAP)

        return data
