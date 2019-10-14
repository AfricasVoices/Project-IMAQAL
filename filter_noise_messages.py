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
    parser.add_argument("clean_coded_dir_path", metavar="clean-coded-dir-path",
                        help="Directory to write clean coded Coda files to")

    args = parser.parse_args()
    coded_dir_path = args.coded_dir_path
    clean_coded_dir_path = args.clean_coded_dir_path

    with open(coded_dir_path) as f:
        messages = json.load(f)

    not_noise = []
    for message in messages:
        if not somali.DemographicCleaner.is_noise(message['Text'], min_length=10):
            not_noise.append(message)
    log.info(f'Filtered {len(messages) - len(not_noise)} noise messages from {len(messages)} total messages')

    # Output all RQA and Follow Up surveys messages which aren't noise to Coda
    with open(clean_coded_dir_path, 'w') as f:
        json.dump(not_noise,f, indent=2)
