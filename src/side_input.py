from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.pipeline import Pipeline
from apache_beam.pvalue import AsList, PCollection
from apache_beam.transforms.core import FlatMap, PTransform, Create


def left_join(main_element: dict, side_input: list[dict], key: str):
    side_input_keys: list = [element[key] for element in side_input]
    if main_element[key] not in side_input_keys:
        yield main_element
    for side_element in side_input:
        joined_element: dict = {}
        if main_element[key] == side_element[key]:
            joined_element = {**main_element, **side_element}
            yield joined_element


class LeftJoin(PTransform):
    def __init__(self, key: str, side_input: PCollection):
        self.key = key
        self.side_input = side_input

    def expand(self, pcoll: PCollection):
        return pcoll | FlatMap(
            fn=left_join, side_input=AsList(self.side_input), key=self.key
        )


def main():
    options = PipelineOptions()
    standard_options = options.view_as(StandardOptions)
    standard_options.streaming = False
    standard_options.runner = "DirectRunner"
    p = Pipeline(options=options)
    main_input = p | "Create main input" >> Create(
        [{"id": "a", "num1": 1}, {"id": "b", "num1": 3}]
    )
    side_input = p | "Create side input" >> Create(
        [{"id": "a", "num2": 2}, {"id": "b", "num2": 4}]
    )
    _ = main_input | "ApplyLeftJoin" >> LeftJoin(key="id", side_input=side_input)
    p.run()


if __name__ == "__main__":
    main()
