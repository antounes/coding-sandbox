import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from options import PipeOptions

options = PipelineOptions()
p = beam.PipelineOptions(options)

