import json

from core_data_modules.data_models import Scheme

def _open_scheme(filename):
    with open(f"code_schemes/{filename}", "r") as f:
        firebase_map = json.load(f)
        return Scheme.from_firebase_map(firebase_map)


class CodeSchemes(object):
    S01E01 = _open_scheme("s01e01.json")
    S01E02 = _open_scheme("s01e02.json")
    S01E02_YES_NO_AMB = _open_scheme("s01e02_yes_no_amb.json")
    S01E03 = _open_scheme("s01e03.json")
    S01E03_YES_NO_AMB = _open_scheme("s01e03_yes_no_amb.json")
    S01E04 = _open_scheme("s01e04.json")
    S01E05 = _open_scheme("s01e05.json")
    S01E05_YES_NO_AMB = _open_scheme("s01e05_yes_no_amb.json")
    S01E06 = _open_scheme("s01e06.json")
    S01E07 = _open_scheme("s01e07.json")
    S01E07_YES_NO_AMB = _open_scheme("s01e07_yes_no_amb.json")

    S01MAG03 = _open_scheme("s01mag03.json")
    S01MAG03_YES_NO_AMB = _open_scheme("s01mag03_yes_no_amb.json")
    S01MAG04 = _open_scheme("s01mag04.json")
    S01MAG05 = _open_scheme("s01mag05.json")
    S01MAG05_YES_NO_AMB = _open_scheme("s01mag05_yes_no_amb.json")
    S01MAG06 = _open_scheme("s01mag06.json")
    S01MAG06_YES_NO_AMB = _open_scheme("s01mag06_yes_no_amb.json")
    S01MAG07 = _open_scheme("s01mag07.json")
    S01MAG07_YES_NO_AMB = _open_scheme("s01mag07_yes_no_amb.json")
    S01MAG08 = _open_scheme("s01mag08.json")
    S01MAG09 = _open_scheme("s01mag09.json")
    S01MAG09_YES_NO_AMB = _open_scheme("s01mag09_yes_no_amb.json")
    # TODO Update this once data structure doc is updated.
    S01MAG10 = _open_scheme("s01mag10.json")
    S01MAG11 = _open_scheme("s01mag11.json")
    S01MAG12 = _open_scheme("s01mag12.json")
    S01MAG13 = _open_scheme("s01mag13.json")
    S01MAG14 = _open_scheme("s01mag14.json")
    S01MAG15 = _open_scheme("s01mag15.json")
    S01MAG16 = _open_scheme("s01mag16.json")
    S01MAG17 = _open_scheme("s01mag17.json")
    S01MAG18 = _open_scheme("s01mag18.json")
    S01MAG19 = _open_scheme("s01mag19.json")
    S01MAG20 = _open_scheme("s01mag20.json")

    S02E08 = _open_scheme("s02e08.json")
    S02E09 = _open_scheme("s02e09.json")
    S02E10 = _open_scheme("s02e10.json")
    S02E11 = _open_scheme("s02e11.json")
    S02E12 = _open_scheme("s02e12.json")
    S02E13 = _open_scheme("s02e13.json")
    S02E14 = _open_scheme("s02e14.json")

    WOMEN_PARTICIPATION = _open_scheme("women_participation.json")
    WOMEN_PARTICIPATION_YES_NO_AMB = _open_scheme("women_participation_yes_no_amb.json")
    MINORITY_CLAN_ISSUES = _open_scheme("minority_clan_issues.json")
    MINORITY_CLAN_ISSUES_YES_NO_AMB = _open_scheme("minority_clan_issues_yes_no_amb.json")
    YOUNG_PEOPLE_ISSUES = _open_scheme("young_people_issues.json")
    YOUNG_PEOPLE_ISSUES_YES_NO_AMB = _open_scheme("young_people_issues_yes_no_amb.json")
    DECISIONS_MINORITY_CLAN = _open_scheme("decisions_minority_clan.json")
    DECISIONS_MINORITY_CLAN_YES_NO_AMB = _open_scheme("decisions_minority_clan_yes_no_amb.json")

    AGE = _open_scheme("age.json")
    RECENTLY_DISPLACED = _open_scheme("recently_displaced.json")
    HOUSEHOLD_LANGUAGE = _open_scheme("household_language.json")
    GENDER = _open_scheme("gender.json")

    SOMALIA_OPERATOR = _open_scheme("somalia_operator.json")
    SOMALIA_DISTRICT = _open_scheme("somalia_district.json")
    MOGADISHU_SUB_DISTRICT = _open_scheme("mogadishu_sub_district.json")
    SOMALIA_REGION = _open_scheme("somalia_region.json")
    SOMALIA_STATE = _open_scheme("somalia_state.json")
    SOMALIA_ZONE = _open_scheme("somalia_zone.json")

    WS_CORRECT_DATASET = _open_scheme("ws_correct_dataset.json")
