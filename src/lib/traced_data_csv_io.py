import csv
import time

from src.lib import PipelineConfiguration
from core_data_modules.cleaners import Codes
from core_data_modules.traced_data import Metadata, TracedData

_td_type_error_string = "argument 'data' contains an element which is not of type TracedData"

class TracedDataCSVIO(object):
    @staticmethod
    def export_traced_data_iterable_to_csv(data, f, matrix_keys, headers):
        """
        Writes a collection of TracedData objects to a CSV.
        Columns will be exported in the order declared in headers if that parameter is specified,
        otherwise the output order will be arbitrary.
        :param data: TracedData objects to export.
        :type data: iterable of TracedData
        :param f: File to export to.
        :type f: file-like
        :param matrix_keys:
        :type matrix_keys: list of str
        :param headers: Headers to export.
        :type headers: list of str
        """
        data = list(data)
        for td in data:
            assert isinstance(td, TracedData), _td_type_error_string

        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        writer.writeheader()

        for td in data:
            # Assign Codes.MATRIX_0 to missing header keys except raw_fields which are the only keys where we expect
            # missing values
            row = {key: td.get(key) if key not in matrix_keys else td.get(key, Codes.MATRIX_0) for key in headers}
            writer.writerow(row)
