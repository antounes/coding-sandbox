import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

options = PipelineOptions()
p = beam.PipelineOptions(options)

class PipeOptions(PipelineOptions):

    @classmethod
        def _add_argparse_args(cls, parser):
            parser.add_argument("--input",
                                help="Input for the pipeline",
                                default="./data")
            parser.add_argument("--output",
                                help="Output for the pipeline",
                                default="./output")
