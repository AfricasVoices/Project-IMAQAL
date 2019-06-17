import argparse
import csv

from core_data_modules.logging import Logger
from core_data_modules.util import PhoneNumberUuidTable

log = Logger(__name__)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="De-identifies a CSV by converting the phone numbers in "
                                                 "the specified column to avf phone ids")

    parser.add_argument("csv_input_path", metavar="recovered-csv-input-url",
                        help="Path to a CSV file to de-identify a column of")
    parser.add_argument("phone_number_uuid_table_path", metavar="phone-number-uuid-table-path",
                        help="Path to the phone number <-> uuid table")
    parser.add_argument("column_to_de_identify", metavar="column-to-de-identify",
                        help="Name of the column containing phone numbers to be de-identified")
    parser.add_argument("de_identified_csv_output_path", metavar="de-identified-csv-output-path",
                        help="Path to write the de-identified CSV to")

    args = parser.parse_args()

    csv_input_path = args.csv_input_path
    phone_number_uuid_table_path = args.phone_number_uuid_table_path
    column_to_de_identify = args.column_to_de_identify
    de_identified_csv_output_path = args.de_identified_csv_output_path

    log.info(f"Loading csv from '{csv_input_path}'...")
    with open(csv_input_path, "r", encoding='utf-8-sig') as f:
        raw_data = list(csv.DictReader(f))
    log.info(f"Loaded {len(raw_data)} rows")

    log.info(f"Loading the phone number <-> uuid table from '{phone_number_uuid_table_path}'...")
    with open(phone_number_uuid_table_path) as f:
        phone_number_uuid_table = PhoneNumberUuidTable.load(f)
    log.info(f"Loaded {len(phone_number_uuid_table.numbers())} phone number <-> uuid mappings")

    log.info(f"De-identifying column '{column_to_de_identify}'...")
    for row in raw_data:
        row[column_to_de_identify] = phone_number_uuid_table.add_phone(row[column_to_de_identify])

    log.info(f"Updating the phone number <-> uuid table at '{phone_number_uuid_table_path}' "
             f"with {len(phone_number_uuid_table.numbers())} phone number <-> uuid mappings...")
    with open(phone_number_uuid_table_path, "w") as f:
        phone_number_uuid_table.dump(f)
    log.info(f"Updated the phone number <-> uuid table")

    log.info(f"Exporting {len(raw_data)} de-identified rows to {de_identified_csv_output_path}...")
    with open(de_identified_csv_output_path, "w") as f:
        writer = csv.DictWriter(f, fieldnames=raw_data[0].keys())
        writer.writeheader()

        for row in raw_data:
            writer.writerow(row)
    log.info(f"Exported de-identified csv")
