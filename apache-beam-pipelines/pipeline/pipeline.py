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


# Pipeline is to be written in blocks rather than in chain
# so that we can add future transformations

# Init pipeline
options = PipelineOptions()
p = beam.Pipeline(options=options)

# Read input csv file
csv_lines = (p | ReadFromText(input_filename, skip_header_lines=1)
             | beam.ParDo(Split())
             | beam.io.WriteToText(output_filename)
             )

# The GroupByKey function allows to create a PCollection of all
# elements for which the key (ie the left side of the tuples) is the same.
mean_open = (
    csv_lines | beam.ParDo(CollectOpen())
    | "Grouping keys Open" >> beam.GroupByKey()
    | "Calculating mean for Open" >> beam.CombineValues(
        beam.combiners.MeanCombineFn()
        )
)

# Let's apply a different transform on read files
mean_close = (
    csv_lines | beam.ParDo(CollectClose())
    | "Grouping keys Close" >> beam.GroupByKey()
    | "Calculating mean for Close" >> beam.CombineValues(
        beam.combiners.MeanCombineFn()
        )
)

# We have two PCollection mean_open and mean_close
# Let's combine them before we write them to an output

output = (
    {
        "Mean Open": mean_open,
        "Mean Close": mean_close
    } | beam.CoGroupByKey()
    | beam.io.WriteToText(output_filename))
)
