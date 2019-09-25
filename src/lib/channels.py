import time

from core_data_modules.cleaners import Codes
from core_data_modules.traced_data import Metadata
from dateutil.parser import isoparse

from src.lib import PipelineConfiguration


class Channels(object):
    DRAMA_PROMO_KEY = "drama_promo"
    RADIO_MAGAZINE_PROMO_KEY = "magazine_promo"
    MAGAZINE_SMS_AD_KEY = "magazine_sms_ad"
    FACEBOOK_AD_KEY = "facebook_ad"
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
    RADIO_MAGAZINE_S01E03_KEY = "radio_magazine_participation_s01mag03"
    RADIO_MAGAZINE_S01E04_KEY = "radio_magazine_participation_s01mag04"
    RADIO_MAGAZINE_S01E05_KEY = "radio_magazine_participation_s01mag05"
    RADIO_MAGAZINE_S01E06_KEY = "radio_magazine_participation_s01mag06"
    RADIO_MAGAZINE_S01E07_KEY = "radio_magazine_participation_s01mag07"
    RADIO_MAGAZINE_S01E08_KEY = "radio_magazine_participation_s01mag08"
    RADIO_MAGAZINE_S01E09_KEY = "radio_magazine_participation_s01mag09"
    RADIO_MAGAZINE_S01E10_KEY = "radio_magazine_participation_s01mag10"
    RADIO_MAGAZINE_S01E11_KEY = "radio_magazine_participation_s01mag11"
    RADIO_MAGAZINE_S01E12_KEY = "radio_magazine_participation_s01mag12"
    RADIO_MAGAZINE_S01E13_KEY = "radio_magazine_participation_s01mag13"
    RADIO_MAGAZINE_S01E14_KEY = "radio_magazine_participation_s01mag14"
    RADIO_MAGAZINE_S01E15_KEY = "radio_magazine_participation_s01mag15"
    RADIO_MAGAZINE_S01E16_KEY = "radio_magazine_participation_s01mag16"
    RADIO_MAGAZINE_S01E17_KEY = "radio_magazine_participation_s01mag17"
    RADIO_MAGAZINE_S01E18_KEY = "radio_magazine_participation_s01mag18"
    RADIO_MAGAZINE_S01E19_KEY = "radio_magazine_participation_s01mag19"
    RADIO_MAGAZINE_S01E20_KEY = "radio_magazine_participation_s01mag20"

    # Time ranges expressed in format (start_of_range_inclusive, end_of_range_exclusive)
    MAGAZINE_SMS_AD_RANGES = [
        ("2019-06-05T16:00:00+03:00", "2019-06-06T24:00:00+03:00"),
        ("2019-06-12T16:00:00+03:00", "2019-06-13T24:00:00+03:00"),
        ("2019-06-19T16:00:00+03:00", "2019-06-20T24:00:00+03:00"),
        ("2019-06-26T16:00:00+03:00", "2019-06-27T24:00:00+03:00"),
        ("2019-07-03T16:00:00+03:00", "2019-07-04T24:00:00+03:00"),
        ("2019-07-10T16:00:00+03:00", "2019-07-11T24:00:00+03:00"),
        ("2019-07-17T16:00:00+03:00", "2019-07-18T24:00:00+03:00"),
        ("2019-07-24T16:00:00+03:00", "2019-07-25T24:00:00+03:00"),
        ("2019-07-31T16:00:00+03:00", "2019-08-01T24:00:00+03:00"),
        ("2019-08-07T16:00:00+03:00", "2019-08-08T24:00:00+03:00"),
        ("2019-08-14T16:00:00+03:00", "2019-08-15T24:00:00+03:00"),
        ("2019-08-21T16:00:00+03:00", "2019-08-22T24:00:00+03:00"),
        ("2019-08-28T16:00:00+03:00", "2019-08-29T24:00:00+03:00")
    ]

    FACEBOOK_AD_RANGES = [
        ("2019-06-12T09:30:00+03:00", "2019-06-12T16:00:00+03:00"),  # Posted at 9:30 am
        ("2019-06-19T06:00:00+03:00", "2019-06-19T16:00:00+03:00"),  # Posted at 6:00 am
        ("2019-06-26T00:00:00+03:00", "2019-06-26T16:00:00+03:00"),
        ("2019-07-03T00:00:00+03:00", "2019-07-03T16:00:00+03:00"),
        ("2019-07-10T00:00:00+03:00", "2019-07-10T16:00:00+03:00"),
        ("2019-07-17T00:00:00+03:00", "2019-07-17T16:00:00+03:00"),
        ("2019-07-24T00:00:00+03:00", "2019-07-24T16:00:00+03:00"),
        ("2019-07-31T00:00:00+03:00", "2019-07-31T16:00:00+03:00"),
        ("2019-08-07T00:00:00+03:00", "2019-08-07T16:00:00+03:00"),
        ("2019-08-14T00:00:00+03:00", "2019-08-14T16:00:00+03:00"),
        ("2019-08-21T00:00:00+03:00", "2019-08-21T16:00:00+03:00"),
        ("2019-08-28T00:00:00+03:00", "2019-08-28T16:00:00+03:00")
    ]

    DRAMA_PROMO_RANGES = [
        ("2019-04-28T00:00:00+03:00", "2019-05-02T17:00:00+03:00"),
        ("2019-05-05T00:00:00+03:00", "2019-05-09T17:00:00+03:00"),
        ("2019-05-12T00:00:00+03:00", "2019-05-16T17:00:00+03:00"),
        ("2019-05-19T00:00:00+03:00", "2019-05-23T17:00:00+03:00"),
        ("2019-05-26T00:00:00+03:00", "2019-05-30T17:00:00+03:00")
    ]

    RADIO_MAGAZINE_PROMO_RANGES = [
        ("2019-06-02T00:00:00+03:00", "2019-06-04T24:00:00+03:00"),
        ("2019-06-09T00:00:00+03:00", "2019-06-12T09:30:00+03:00"),
        ("2019-06-16T00:00:00+03:00", "2019-06-19T06:00:00+03:00"),
        ("2019-06-23T00:00:00+03:00", "2019-06-25T24:00:00+03:00"),
        ("2019-06-30T00:00:00+03:00", "2019-07-02T24:00:00+03:00"),
        ("2019-07-07T00:00:00+03:00", "2019-07-09T24:00:00+03:00"),
        ("2019-07-14T00:00:00+03:00", "2019-07-16T24:00:00+03:00"),
        ("2019-07-21T00:00:00+03:00", "2019-07-23T24:00:00+03:00"),
        ("2019-07-28T00:00:00+03:00", "2019-07-30T24:00:00+03:00"),
        ("2019-08-04T00:00:00+03:00", "2019-08-06T24:00:00+03:00"),
        ("2019-08-11T00:00:00+03:00", "2019-08-13T24:00:00+03:00"),
        ("2019-08-18T00:00:00+03:00", "2019-08-20T24:00:00+03:00"),
        ("2019-08-25T00:00:00+03:00", "2019-08-27T24:00:00+03:00")
    ]

    RADIO_DRAMA_RANGES = [
        ("2019-04-19T00:00:00+03:00", "2019-04-24T09:00:00+03:00"),
        ("2019-04-26T00:00:00+03:00", "2019-04-28T00:00:00+03:00"),
        ("2019-05-03T00:00:00+03:00", "2019-05-05T00:00:00+03:00"),
        ("2019-05-10T00:00:00+03:00", "2019-05-12T00:00:00+03:00"),
        ("2019-05-17T00:00:00+03:00", "2019-05-19T00:00:00+03:00"),
        ("2019-05-24T00:00:00+03:00", "2019-05-26T00:00:00+03:00"),
        ("2019-05-31T00:00:00+03:00", "2019-06-02T00:00:00+03:00")
    ]

    RADIO_MAGAZINE_RANGES = [
        ("2019-06-07T00:00:00+03:00", "2019-06-08T24:00:00+03:00"),
        ("2019-06-14T00:00:00+03:00", "2019-06-15T24:00:00+03:00"),
        ("2019-06-21T00:00:00+03:00", "2019-06-22T24:00:00+03:00"),
        ("2019-06-28T00:00:00+03:00", "2019-06-29T24:00:00+03:00"),
        ("2019-07-05T00:00:00+03:00", "2019-07-06T24:00:00+03:00"),
        ("2019-07-12T00:00:00+03:00", "2019-07-13T24:00:00+03:00"),
        ("2019-07-19T00:00:00+03:00", "2019-07-20T24:00:00+03:00"),
        ("2019-07-26T00:00:00+03:00", "2019-07-27T24:00:00+03:00"),
        ("2019-08-02T00:00:00+03:00", "2019-08-03T24:00:00+03:00"),
        ("2019-08-09T00:00:00+03:00", "2019-08-10T24:00:00+03:00"),
        ("2019-08-16T00:00:00+03:00", "2019-08-17T24:00:00+03:00"),
        ("2019-08-23T00:00:00+03:00", "2019-08-24T24:00:00+03:00"),
        ("2019-08-30T00:00:00+03:00", "2019-08-31T24:00:00+03:00"),
        ("2019-09-06T00:00:00+03:00", "2019-09-07T24:00:00+03:00"),
        ("2019-09-13T00:00:00+03:00", "2019-09-14T24:00:00+03:00"),
        ("2019-09-20T00:00:00+03:00", "2019-09-21T24:00:00+03:00"),
        ("2019-09-27T00:00:00+03:00", "2019-09-28T24:00:00+03:00"),
        ("2019-10-04T00:00:00+03:00", "2019-10-05T24:00:00+03:00")

    ]

    DRAMA_S01E01_RANGES = [
        ("2019-04-19T00:00:00+03:00", "2019-04-24T09:00:00+03:00")
    ]

    DRAMA_S01E02_RANGES = [
        ("2019-04-26T00:00:00+03:00", "2019-05-02T17:00:00+03:00")
    ]

    DRAMA_S01E03_RANGES = [
        ("2019-05-03T00:00:00+03:00", "2019-05-09T17:00:00+03:00")
    ]

    DRAMA_S01E04_RANGES = [
        ("2019-05-10T00:00:00+03:00", "2019-05-16T17:00:00+03:00")
    ]

    DRAMA_S01E05_RANGES = [
        ("2019-05-17T00:00:00+03:00", "2019-05-23T17:00:00+03:00")
    ]

    DRAMA_S01E06_RANGES = [
        ("2019-05-24T00:00:00+03:00", "2019-05-30T17:00:00+03:00")
    ]

    DRAMA_S01E07_RANGES = [
        ("2019-05-31T00:00:00+03:00", "2019-06-06T17:00:00+03:00")
    ]

    RADIO_MAGAZINE_S01E03_RANGES = [
        ("2019-06-02T00:00:00+03:00", "2019-06-08T24:00:00+03:00")
    ]

    RADIO_MAGAZINE_S01E04_RANGES = [
        ("2019-06-09T00:00:00+03:00", "2019-06-15T24:00:00+03:00")
    ]

    RADIO_MAGAZINE_S01E05_RANGES = [
        ("2019-06-16T00:00:00+03:00", "2019-06-22T24:00:00+03:00")
    ]

    RADIO_MAGAZINE_S01E06_RANGES = [
        ("2019-06-23T00:00:00+03:00", "2019-06-29T24:00:00+03:00")
    ]

    RADIO_MAGAZINE_S01E07_RANGES = [
        ("2019-06-30T00:00:00+03:00", "2019-07-06T24:00:00+03:00")
    ]

    RADIO_MAGAZINE_S01E08_RANGES = [
        ("2019-07-07T00:00:00+03:00", "2019-07-13T24:00:00+03:00")
    ]

    RADIO_MAGAZINE_S01E09_RANGES = [
        ("2019-07-14T00:00:00+03:00", "2019-07-20T24:00:00+03:00")
    ]

    RADIO_MAGAZINE_S01E10_RANGES = [
        ("2019-07-21T00:00:00+03:00", "2019-07-27T24:00:00+03:00")
    ]

    RADIO_MAGAZINE_S01E11_RANGES = [
        ("2019-07-28T00:00:00+03:00", "2019-08-03T24:00:00+03:00")
    ]

    RADIO_MAGAZINE_S01E12_RANGES = [
        ("2019-08-04T00:00:00+03:00", "2019-08-10T24:00:00+03:00")
    ]

    RADIO_MAGAZINE_S01E13_RANGES = [
        ("2019-08-11T00:00:00+03:00", "2019-08-17T24:00:00+03:00")
    ]

    RADIO_MAGAZINE_S01E14_RANGES = [
        ("2019-08-18T00:00:00+03:00", "2019-08-24T24:00:00+03:00")
    ]

    RADIO_MAGAZINE_S01E15_RANGES = [
        ("2019-08-25T00:00:00+03:00", "2019-08-31T24:00:00+03:00")
    ]

    RADIO_MAGAZINE_S01E16_RANGES = [
        ("2019-09-02T00:00:00+03:00", "2019-09-06T24:00:00+03:00")
    ]

    RADIO_MAGAZINE_S01E17_RANGES = [
        ("2019-09-09T00:00:00+03:00", "2019-09-13T24:00:00+03:00")
    ]

    RADIO_MAGAZINE_S01E18_RANGES = [
        ("2019-09-16T00:00:00+03:00", "2019-09-20T24:00:00+03:00")
    ]

    RADIO_MAGAZINE_S01E19_RANGES = [
        ("2019-09-23T00:00:00+03:00", "2019-09-27T24:00:00+03:00")
    ]

    RADIO_MAGAZINE_S01E20_RANGES = [
        ("2019-09-30T00:00:00+03:00", "2019-10-04T24:00:00+03:00")
    ]

    CHANNEL_RANGES = {
        DRAMA_PROMO_KEY: DRAMA_PROMO_RANGES,
        RADIO_MAGAZINE_PROMO_KEY: RADIO_MAGAZINE_PROMO_RANGES,
        FACEBOOK_AD_KEY: FACEBOOK_AD_RANGES,
        MAGAZINE_SMS_AD_KEY: MAGAZINE_SMS_AD_RANGES,
        RADIO_DRAMA_KEY: RADIO_DRAMA_RANGES,
        RADIO_MAGAZINE_KEY: RADIO_MAGAZINE_RANGES
    }

    SHOW_RANGES = {
        DRAMA_S01E01_KEY: DRAMA_S01E01_RANGES,
        DRAMA_S01E02_KEY: DRAMA_S01E02_RANGES,
        DRAMA_S01E03_KEY: DRAMA_S01E03_RANGES,
        DRAMA_S01E04_KEY: DRAMA_S01E04_RANGES,
        DRAMA_S01E05_KEY: DRAMA_S01E05_RANGES,
        DRAMA_S01E06_KEY: DRAMA_S01E06_RANGES,
        DRAMA_S01E07_KEY: DRAMA_S01E07_RANGES,
        RADIO_MAGAZINE_S01E03_KEY: RADIO_MAGAZINE_S01E03_RANGES,
        RADIO_MAGAZINE_S01E04_KEY: RADIO_MAGAZINE_S01E04_RANGES,
        RADIO_MAGAZINE_S01E05_KEY: RADIO_MAGAZINE_S01E05_RANGES,
        RADIO_MAGAZINE_S01E06_KEY: RADIO_MAGAZINE_S01E06_RANGES,
        RADIO_MAGAZINE_S01E07_KEY: RADIO_MAGAZINE_S01E07_RANGES,
        RADIO_MAGAZINE_S01E08_KEY: RADIO_MAGAZINE_S01E08_RANGES,
        RADIO_MAGAZINE_S01E09_KEY: RADIO_MAGAZINE_S01E09_RANGES,
        RADIO_MAGAZINE_S01E10_KEY: RADIO_MAGAZINE_S01E10_RANGES,
        RADIO_MAGAZINE_S01E11_KEY: RADIO_MAGAZINE_S01E11_RANGES,
        RADIO_MAGAZINE_S01E12_KEY: RADIO_MAGAZINE_S01E12_RANGES,
        RADIO_MAGAZINE_S01E13_KEY: RADIO_MAGAZINE_S01E13_RANGES,
        RADIO_MAGAZINE_S01E14_KEY: RADIO_MAGAZINE_S01E14_RANGES,
        RADIO_MAGAZINE_S01E15_KEY: RADIO_MAGAZINE_S01E15_RANGES,
        RADIO_MAGAZINE_S01E16_KEY: RADIO_MAGAZINE_S01E16_RANGES,
        RADIO_MAGAZINE_S01E17_KEY: RADIO_MAGAZINE_S01E17_RANGES,
        RADIO_MAGAZINE_S01E18_KEY: RADIO_MAGAZINE_S01E18_RANGES,
        RADIO_MAGAZINE_S01E19_KEY: RADIO_MAGAZINE_S01E19_RANGES,
        RADIO_MAGAZINE_S01E20_KEY: RADIO_MAGAZINE_S01E20_RANGES

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
