# -*- coding: utf-8 -*-
#Author: Liuxin YANG
#Date: 2025-06-18


import datetime
import apache_beam as beam
from google.cloud import bigquery
from config.configuration import DatasetConfig
from apache_beam.options.pipeline_options import PipelineOptions

from pipeline.transforms_sql import (
    generate_clean_query
)
from config.utils import (
    load_yaml
)
from pipeline.transforms_beam import (
    InitializeErrorReason,
    stringify_error_reason,
    CheckDuplicateRows,
    CheckDuplicateKeys,
    CheckBarcodeLength,
    CheckDateFormat,
    StandardizeByFrequence,
    drop_error_reason
)

from config.utils import do_query_job
import logging
logging.getLogger("apache_beam").setLevel(logging.ERROR)

def bq_schema_to_beam(schema, extra_fields=None):
    beam_schema = []
    for field in schema:
        beam_schema.append(f"{field.name}:{field.field_type.lower()}")
    if extra_fields:
        for name, typ in extra_fields:
            beam_schema.append(f"{name}:{typ}")
    return ",".join(beam_schema)


def run():
    cfg = DatasetConfig()
    client = bigquery.Client()
    yaml_path = "config/dataset.yaml"
    raw_table = f"{cfg.project}.{cfg.dataset}.{cfg.raw_table}"
    clean_table = f"{cfg.project}.{cfg.dataset}.{cfg.clean_table}"
    excluded_table = f"{cfg.project}.{cfg.dataset}.{cfg.excluded_table}"

    schema = client.get_table(raw_table).schema
    all_columns = [col.name for col in schema]
    std_cols, rules = load_yaml(cfg.dataset_type, yaml_path)

    options = PipelineOptions(
        runner="DirectRunner",
        project=cfg.project,
        temp_location=f"gs://{cfg.dataflow}/temp",
        region="europe-west1",
        job_name=f"QA-{cfg.dataset_type}-{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}",
    )


    print("Step 1: Data Cleaning...")
    clean_query = generate_clean_query(cfg, client, "raw")
    do_query_job(cfg, client, "clean", clean_query)
    
    print("Step 2: Data validation...")
    with beam.Pipeline(options=options) as pcoll:
        data = (
            pcoll
            | "Read dataset from BigQuery" >> beam.io.ReadFromBigQuery(table = clean_table)
            | "Initialize error reason" >> beam.ParDo(InitializeErrorReason()) 
            | "Standardize columns" >> StandardizeByFrequence(std_cols)
        )

        if "duplicate_row" in rules:
            data = data | "Check duplicate rows" >> CheckDuplicateRows(all_columns)
        if "duplicate_key" in rules:
            data = data | "Check duplicate keys" >> CheckDuplicateKeys(cfg.key_columns.split(", "))
        if "barcode_length" in rules:
            data = data | "Check barcode length" >> beam.ParDo(CheckBarcodeLength(cfg.barcode_columns))
        if "barcode_format" in rules:
            data = data | "Check date format" >> beam.ParDo(CheckDateFormat(cfg.date_columns))
        
        clean, excluded = data | "Split data into clean and excluded" >> beam.Partition(lambda x, _: 0 if not x["error_reason"] else 1, 2)

        print("Update Clean table...")
        clean = clean | "Drop error_reason from clean" >> beam.Map(drop_error_reason)
        clean = clean | "Write clean data to BigQuery" >> beam.io.WriteToBigQuery(
            clean_table,
            #schema="SCHEMA_AUTODETECT",
            schema=bq_schema_to_beam(schema),
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        )

        print("Create Excluded table...")
        excluded_schema = bq_schema_to_beam(
            schema,
            extra_fields=[("error_reason", "string")]
        )
        excluded = excluded | "Excluded : Convert error_reason list to string" >> beam.Map(stringify_error_reason)
        excluded = excluded | "Write excluded data to BigQuery" >> beam.io.WriteToBigQuery(
                excluded_table,
                schema=excluded_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            )
        


if __name__ == "__main__":
    run()