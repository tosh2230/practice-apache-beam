from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.core import Create

from src.side_input import LeftJoin


class TestLeftJoin:
    def test_left_join(self):
        options = PipelineOptions()
        standard_options = options.view_as(StandardOptions)
        standard_options.streaming = False
        standard_options.runner = "DirectRunner"
        with TestPipeline(options=options) as p:
            main_input = p | "Create main input" >> Create(
                [
                    {"id": "a", "num1": 1},
                    {"id": "a", "num1": 2},
                    {"id": "b", "num1": 3},
                    {"id": "b", "num1": 4},
                    {"id": "c", "num1": 5},
                ]
            )
            side_input = p | "Create side input" >> Create(
                [
                    {"id": "a", "num2": 2, "str1": "aaa"},
                    {"id": "b", "num2": 4, "str1": "bbb"},
                    {"id": "d", "num2": 6, "str1": "ddd"},
                ]
            )

            expected = [
                {"id": "a", "num1": 1, "num2": 2, "str1": "aaa"},
                {"id": "a", "num1": 2, "num2": 2, "str1": "aaa"},
                {"id": "b", "num1": 3, "num2": 4, "str1": "bbb"},
                {"id": "b", "num1": 4, "num2": 4, "str1": "bbb"},
                {"id": "c", "num1": 5},
            ]
            actual = main_input | "ApplyLeftJoin" >> LeftJoin(
                key="id", side_input=side_input
            )
            assert_that(
                actual=actual,
                matcher=equal_to(expected=expected),
                reify_windows=False,
            )
