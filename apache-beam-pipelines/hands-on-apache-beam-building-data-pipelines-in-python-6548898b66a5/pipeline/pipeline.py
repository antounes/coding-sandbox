import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input',
                            help='Input for the pipeline',
                            default='./data/')
        parser.add_argument('--output',
                            help='Output for the pipeline',
                            default='./output/')


class Split(beam.DoFn):
    def process(self, element):
        Date, Open, High, Low, Close, Volume = element.split(",")
        return[{
            "Open": float(Open),
            "Close": float(Close)
        }]


class CollectOpen(beam.DoFn):
    def process(self, element):
        # Returns a list of tuples containing Date and Open value
        result = [(1, element["Open"])]
        return result


class CollectClose(beam.ParDo):
    def process(self, element):
        # Returns a list of tuples containing Date and Close value
        result = [(1, element["Close"])]
        return result


if __name__ == "__main__":
    MyOptions = PipelineOptions()
    input_filename = "../data/all_stocks_5yr.csv"
    output_filename = "../output/results"

    with beam.Pipeline(options=MyOptions) as p:
        r_input = (
                p | "Read input file" >> beam.io.ReadFromText(input_filename, skip_header_lines=1)
                | "Split elements of each line" >> beam.ParDo(Split())
        )

        mean_open = (
                r_input | "Collect only elements relative to the Open data" >> beam.ParDo(CollectOpen())
                | "Group keys Open" >> beam.GroupByKey()
                | "Calculate mean for Open data" >> beam.CombineValues(
                    beam.combiners.MeanCombineFn()
                )
        )

        mean_close = (
                r_input | "Collect only elements relative to the Close data" >> beam.ParDo(CollectClose())
                | "Group keys Close" >> beam.GroupByKey()
                | "Calculate mean for Close data" >> beam.CombineValues(
                    beam.combiners.MeanCombineFn()
                )
        )

        r_output = (
            {
                "Mean Open": mean_open,
                "Mean Close": mean_close
            }
            | "Group output by keys" >> beam.CoGroupByKey()
            | "Write output to text file" >> beam.io.WriteToText(output_filename, file_name_suffix=".txt")
        )
