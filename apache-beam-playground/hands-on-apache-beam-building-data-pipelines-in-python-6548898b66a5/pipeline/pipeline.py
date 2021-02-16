import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class Split(beam.DoFn):
    def process(self, element):
        date_, open_, high_, low_, close_, volume_, name_ = element.split(",")
        return [{
            "Open": float(open_),
            "Close": float(close_)
        }]


class CollectOpen(beam.DoFn):
    def process(self, element):
        result = [(1, element["Open"])]
        return result


class CollectClose(beam.DoFn):
    def process(self, element):
        result = [(1, element["Close"])]
        return result


if __name__ == "__main__":
    input_path = "./data/"
    input_filename = "sp500-sample.csv"
    output_path = "./output/"
    output_filename = "results"
    options = PipelineOptions()

    with beam.Pipeline(options=PipelineOptions()) as p:
        _input = (
            p | "Read data" >> beam.io.ReadFromText(input_path+input_filename, skip_header_lines=1)
            | "Split elements and keep Open and Close data" >> beam.ParDo(Split())
        )

        mean_open = (
            _input | "Collect Open values" >> beam.ParDo(CollectOpen())
            | "Group keys Open" >> beam.GroupByKey()
            | "Calculate mean for Open values" >> beam.CombineValues(
                beam.combiners.MeanCombineFn()
            )
        )

        mean_close = (
            _input | "Collect Close values" >> beam.ParDo(CollectClose())
            | "Group keys Close" >> beam.GroupByKey()
            | "Calculate mean for Close values" >> beam.CombineValues(
                beam.combiners.MeanCombineFn()
            )
        )

        _output = (
            {
                "Mean Open": mean_open,
                "Mean Close": mean_close
            }
            | beam.CoGroupByKey()
            | beam.io.WriteToText(output_path+output_filename, file_name_suffix=".txt")
        )