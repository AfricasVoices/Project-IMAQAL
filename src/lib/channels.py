import time

from core_data_modules.cleaners import Codes
from core_data_modules.traced_data import Metadata
from dateutil.parser import isoparse

from src.lib import PipelineConfiguration

class Channels(object):
    DRAMA_PROMO_KEY = "drama_promo"
    MAGAZINE_PROMO_KEY = "magazine_promo"
    RADIO_DRAMA_KEY = "radio_drama"
    RADIO_MAGAZINE_KEY = "radio_magazine"
    NON_LOGICAL_KEY = "non_logical_time"
    DRAMA_S01E01_KEY = "radio_drama_participation_s01e01"
    DRAMA_S01E02_KEY = "radio_drama_participation_s01e02"
    DRAMA_S01E03_KEY = "radio_drama_participation_s01e03"
    DRAMA_S01E04_KEY = "radio_drama_participation_s01e04"
    DRAMA_S01E05_KEY = "radio_drama_participation_s01e05"
    DRAMA_S01E06_KEY = "radio_drama_participation_s01e06"
    DRAMA_S01E07_KEY = "radio_drama_participation_s01e07"
    MAGAZINE_S01E01_KEY = "radio_magazine_participation_s01e01"
    MAGAZINE_S01E02_KEY = "radio_magazine_participation_s01e02"

    # Time ranges expressed in format (start_of_range_inclusive, end_of_range_exclusive)
    SMS_AD_RANGES = [

    ]

    DRAMA_PROMO_RANGES = [

        ("2019-04-28T00:00:00+03:00", "2019-05-01T09:00:00+03:00"),
        ("2019-05-05T00:00:00+03:00", "2019-05-08T09:00:00+03:00"),
        ("2019-05-12T00:00:00+03:00", "2019-05-15T09:00:00+03:00"),
        ("2019-05-19T00:00:00+03:00", "2019-05-22T09:00:00+03:00"),
        ("2019-05-26T00:00:00+03:00", "2019-05-29T09:00:00+0:300")
    ]
    #TODO Update once dates have been agreed
    MAGAZINE_PROMO_RANGES = [
        
    ]

    RADIO_DRAMA_RANGES = [
        ("2019-04-19T00:00:00+03:00", "2019-04-24T09:00:00+03:00"),
        ("2019-04-26T00:00:00+03:00", "2019-04-28T00:00:00+03:00"),
        ("2019-05-03T00:00:00+03:00", "2019-05-05T00:00:00+03:00"),
        ("2019-05-10T00:00:00+03:00", "2019-05-12T00:00:00+03:00"),
        ("2019-05-17T00:00:00+03:00", "2019-05-19T00:00:00+03:00"),
        ("2019-05-24T00:00:00+03:00", "2019-05-26T00:00:00+03:00"),
        ("2019-05-31T00:00:00+03:00", "2019-06-01T00:00:00+03:00")
    ]

    RADIO_MAGAZINE_RANGES = [

        ("2019-05-23T00:00:00+03:00", "2019-05-24T00:00:00+03:00"),
        ("2019-05-30T00:00:00+03:00", "2019-05-31T00:00:00+03:00")
    ]

    DRAMA_S01E01_RANGES = [
        ("2019-04-19T00:00:00+03:00", "2019-04-24T09:00:00+0300")
    ]

    DRAMA_S01E02_RANGES = [
        ("2019-04-26T00:00:00+03:00", "2019-05-01T09:00:00+03:00")
    ]

    DRAMA_S01E03_RANGES = [
        ("2019-05-03T00:00:00+03:00", "2019-05-08T09:00:00+03:00")
    ]

    DRAMA_S01E04_RANGES = [
        ("2019-05-10T00:00:00+03:00", "2019-05-15T09:00:00+03:00")
    ]

    DRAMA_S01E05_RANGES = [
        ("2019-05-17T00:00:00+03:00", "2019-05-22T09:00:00+03:00")
    ]

    DRAMA_S01E06_RANGES = [
        ("2019-05-24T00:00:00+03:00", "2019-05-29T09:00:00+03:00")
    ]

    DRAMA_S01E07_RANGES = [
        ("2019-05-31T00:00:00+03:00", "2019-06-01T00:00:00+03:00")
    ]

    MAGAZINE_S01E01_RANGES = [
        ("2019-05-23T00:00:00+03:00", "2019-05-24T00:00:00+03:00")
    ]

    MAGAZINE_S01E02_RANGES = [
        ("2019-05-30T00:00:00+03:00", "2019-05-31T00:00:00+03:00")
    ]

    CHANNEL_RANGES = {
        DRAMA_PROMO_KEY: DRAMA_PROMO_RANGES,
        #TODO Update and uncomment once dates have been agreed
        #MAGAZINE_PROMO_KEY:MAGAZINE_PROMO_RANGES,
        RADIO_DRAMA_KEY:RADIO_DRAMA_RANGES,
        RADIO_MAGAZINE_KEY:RADIO_MAGAZINE_RANGES
    }

    SHOW_RANGES = {
        DRAMA_S01E01_KEY:DRAMA_S01E01_RANGES,
        DRAMA_S01E02_KEY:DRAMA_S01E02_RANGES,
        DRAMA_S01E03_KEY:DRAMA_S01E03_RANGES,
        DRAMA_S01E04_KEY:DRAMA_S01E04_RANGES,
        DRAMA_S01E05_KEY:DRAMA_S01E05_RANGES,
        DRAMA_S01E06_KEY:DRAMA_S01E06_RANGES,
        DRAMA_S01E07_KEY:DRAMA_S01E07_RANGES,
        MAGAZINE_S01E01_KEY:MAGAZINE_S01E01_RANGES,
        MAGAZINE_S01E02_KEY:MAGAZINE_S01E02_RANGES
    }

    @staticmethod
    def timestamp_is_in_ranges(timestamp, ranges, matching_ranges):
        for range in ranges:
            if isoparse(range[0]) <= timestamp < isoparse(range[1]):
                matching_ranges.append(range)
                return True
        return False

    @classmethod
    def set_channel_keys(cls, user, data, time_key):
        for td in data:
            timestamp = isoparse(td[time_key])

            channel_dict = dict()

            # Set channel ranges
            time_range_matches = 0
            matching_ranges = []
            for key, ranges in cls.CHANNEL_RANGES.items():
                if cls.timestamp_is_in_ranges(timestamp, ranges, matching_ranges):
                    time_range_matches += 1
                    channel_dict[key] = Codes.TRUE
                else:
                    channel_dict[key] = Codes.FALSE

            # Set time as NON_LOGICAL if it doesn't fall in range of the **sms ad/radio promo/radio_show**
            if time_range_matches == 0:
                # Assert in range of project
                assert PipelineConfiguration.PROJECT_START_DATE <= timestamp < PipelineConfiguration.PROJECT_END_DATE, \
                    f"Timestamp {td[time_key]} out of range of project"
                channel_dict[cls.NON_LOGICAL_KEY] = Codes.TRUE
            else:
                assert time_range_matches == 1, f"Time '{td[time_key]}' matches multiple time ranges{matching_ranges}"
                channel_dict[cls.NON_LOGICAL_KEY] = Codes.FALSE

            # Set show ranges
            for key, ranges in cls.SHOW_RANGES.items():
                if cls.timestamp_is_in_ranges(timestamp, ranges, matching_ranges):
                    channel_dict[key] = Codes.TRUE
                else:
                    channel_dict[key] = Codes.FALSE

            td.append_data(channel_dict, Metadata(user, Metadata.get_call_location(), time.time()))
