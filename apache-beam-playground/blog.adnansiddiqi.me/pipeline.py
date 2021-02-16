import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class ToLower(beam.DoFn):
    def process(self, element):
        return [{"Data": element.lower()}]


class ToReverse(beam.DoFn):
    def process(self, element):
        d = element["Data"]
        return [d[::-1]]


if __name__ == "__main__":
    input_file = "news-covid19.txt"
    output_file = "results"
    options = PipelineOptions()

    with beam.Pipeline(options=PipelineOptions()) as p:
        r = (
            p | "Read input file" >> beam.io.ReadFromText(input_file)
            | "Lowercase each line of the PCollection" >> beam.ParDo(ToLower())
            | "Reverse each line of the PCollection" >> beam.ParDo(ToReverse())
            | "Write output file" >> beam.io.WriteToText(output_file, file_name_suffix=".txt")
        )

        result = p.run()