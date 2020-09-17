import requests
import logging
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class CustomOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument("--input")
        parser.add_value_provider_argument("--output")


def parse_lines(element):
    return element.split(",")


class CalcVisitDuration(beam.DoFn):
    def process(self, element):
        dt_format = "%Y-%m-%dT%H:%M:%S"
        start_dt = datetime.strptime(element[1], dt_format)
        end_dt = datetime.strptime(element[2], dt_format)

        diff = end_dt - start_dt

        yield [element[0], diff.total_seconds()]


class GetIpCountryOrigin(beam.DoFn):
    def process(self, element):
        ip = element[0]
        response = requests.get(f"http://ip-api.com/json/{ip}?fields=country")
        coutry = response.json()["country"]

        yield [ip, coutry]


def map_country_to_ip(element, ip_map):
    ip = element[0]
    return [ip_map[ip], element[1]]


def run():
    pipeline_options = PipelineOptions()

    with beam.Pipeline(options=pipeline_options) as p:
        custom_options = pipeline_options.view_as(CustomOptions)

        lines = (
            p
            | "ReadFile"
            >> beam.io.ReadFromText(custom_options.input, skip_header_lines=1)
            | "ParseLines" >> beam.Map(parse_lines)
        )

        duration = lines | "CalcVisitDuration" >> beam.ParDo(CalcVisitDuration())
        ip_map = lines | "GetIpCountryOrigin" >> beam.ParDo(GetIpCountryOrigin())

        result = (
            duration
            | "MapIpToCountry"
            >> beam.Map(map_country_to_ip, ip_map=beam.pvalue.AsDict(ip_map))
            | "AverageByCountry" >> beam.CombinePerKey(beam.combiners.MeanCombineFn())
            | "FormatOutput" >> beam.Map(lambda element: ",".join(map(str, element)))
        )

        result | "WriteOutput" >> beam.io.WriteToText(
            custom_options.output, file_name_suffix=".csv"
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.WARNING)
    run()
