import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from src.task2 import DataTransform


class TransformationTest(unittest.TestCase):

    def test_transformation(self):
        """
        Unit Test to test the Composite Transform function
        """
        expected_data = ['{"date": "2019-05-10", "total_amount": 11144.99}']

        with TestPipeline() as p:
            inp = p | beam.Create([
                ('2009-01-07 03:54:25 UTC', '100.01'),
                ('2019-05-10 11:25:30 UTC', '11123.99'),
                ('2020-02-21 02:10:00 UTC', '10.50'),
                ('2019-05-10 06:34:20 UTC', '21.00'),
            ])

            out = inp | DataTransform()

            assert_that(out, equal_to(expected_data), label="CheckOutput")
