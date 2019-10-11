import json
import argparse

from core_data_modules.cleaners import somali
from core_data_modules.logging import Logger
from core_data_modules.util import IOUtils

log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Filter out noise messages from Coda Files",
                                     # Support \n and long lines
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("coded_dir_path", metavar="coded-dir-path",
                        help="Directory to read then write coded Coda files to")

    args = parser.parse_args()
    coded_dir_path = args.coded_dir_path

    with open(coded_dir_path) as f:
        messages = json.load(f)

    not_noise = []
    def filter_noise_message(messages):
        for message in messages:
            if somali.DemographicCleaner.is_noise(message['Text'], min_length=10) == False:
                not_noise.append(message)
        log.info(f'Filtered {len(messages) - len(not_noise)} noise messages from {len(messages)} total messages')

        # Output all RQA and Follow Up surveys messages which aren't noise to Coda
        IOUtils.ensure_dirs_exist(coded_dir_path)
        with open(coded_dir_path, 'w') as f:
            json.dump(not_noise,f, indent=2)

    filter_noise_message(messages)
