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
    def process(self, element, *args, **kwargs):
        Date, Open, High, Low, Close, Volume = element.split(",")
        return[{
            "Open": float(Open),
            "Close": float(Close)
        }]


class CollectOpen(beam.DoFn):
    def process(self, element, *args, **kwargs):
        # Returns a list of tuples containing Date and Open value
        result = [(1, element["Open"])]
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

mean_open = (csv_lines | beam.ParDo(CollectOpen())
             | "Grouping keys open" >> beam.GroupByKey()
             | "Calculating means for open" >> beam.CombineValues(
            beam.combiners.MeanCombineFn()
        )
)


