# -*- coding: utf-8 -*-
#Author: Liuxin YANG
#Date: 2025-05-13

from config.configuration import DatasetConfig
from google.cloud import bigquery
import pandas as pd

def obtain_table_name(cfg:DatasetConfig, tableType: str) -> str:
    
    if tableType == "raw":
        table = cfg.raw_table
    elif tableType == "clean":
        table = cfg.clean_table
    elif tableType == "excluded":
        table = cfg.excluded_table
    else:
        raise ValueError("Invalid table type. Choose from 'raw', 'clean', or 'excluded'.")
    return table

def obtain_dataframe(cfg:DatasetConfig,client:bigquery.Client, tableType: str) -> pd.DataFrame:

    table = obtain_table_name(cfg, tableType)
    data = client.query(f"""
                        SELECT * FROM `{cfg.project}.{cfg.dataset}.{table}`
                        """).to_dataframe()
    
    return data

def load_yaml(dataset_type: str, yaml_path: str = "config/dataset.yaml") -> tuple[list,list]:
    
    import yaml
    with open(yaml_path, 'r') as file:
        data = yaml.safe_load(file)

    dataset = data.get(dataset_type, {})
    rules = dataset.get("rules", [])
    cols = dataset.get("standard_columns", [])

    return cols, rules


def do_query_job(cfg:DatasetConfig,client:bigquery.Client, tableType:str, query:str) -> None:
    """
    Execute the query and save the result in the specified table
    """
    table = obtain_table_name(cfg, tableType)
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            destination = f"{cfg.project}.{cfg.dataset}.{table}",
            write_disposition = "WRITE_TRUNCATE",
        )
    )
    job.result()