import json
import gzip
import re
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class DataTransform(beam.PTransform):
    def expand(self, collection):
        """
        Composite transformation function
        """
        data_transform = (
                collection
                | 'Format data type' >> beam.Map(
                    lambda x: (datetime.strptime(x[0], "%Y-%m-%d %H:%M:%S %Z").date(), float(x[1])))
                | 'Remove Trans before 2010' >> beam.Filter(lambda x: x[0].year > 2010)
                | 'Filter Trans above 20' >> beam.Filter(lambda x: x[1] > 20)
                | 'Format date to string' >> beam.Map(
                    lambda x: (str(x[0]), x[1]))
                | 'GroupBy Date and Sum Transactions' >> beam.CombinePerKey(sum)
                | 'Format to Dict' >> beam.Map(lambda x: {'date': x[0], 'total_amount': x[1]})
                | 'Convert to jsonl' >> beam.Map(json.dumps)
        )
        return data_transform


def run_beam(beam_options):
    """
    Task2 - Apache Beam batch pipeline
    """
    with beam.Pipeline(options=beam_options) as pipeline:
        p1 = (
              pipeline
              | 'Read Data' >> beam.io.ReadFromText(
                    'gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv', skip_header_lines=1)
              | 'Split Data' >> beam.Map(lambda x: re.split('[,](?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)', x))
              | 'Get only columns needed' >> beam.Map(lambda x: (x[0], x[3]))
              | 'Do Transformations' >> DataTransform()
              | 'write_to_compressed' >> beam.io.WriteToText('output/results', file_name_suffix='.jsonl.gz',
                                                             compression_type='gzip', shard_name_template='')
        )


if __name__ == '__main__':
    # pipeline options
    options = PipelineOptions()

    # run beam pipeline
    run_beam(options)

    # check contents written to output file
    with gzip.open('../output/results.jsonl.gz', 'rb') as xyz:
        data = xyz.read().decode('utf-8')
    print(data)
