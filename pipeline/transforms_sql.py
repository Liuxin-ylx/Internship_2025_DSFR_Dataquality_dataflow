# -*- coding: utf-8 -*-
#Author: Liuxin YANG
#Date: 2025-06-18

from google.cloud import bigquery
from config.configuration import DatasetConfig
from config.utils import (
    obtain_table_name
)


def generate_clean_clause(schema, prefix: str = None) -> str:
    """Generate a cleaning clause to normalize all STRING columns.
        1. Multiple space --> single space
        2. Upper case
        3. Delete spaces before and after
        4. Remplace diacritics (accents)
        5. Handle NULL values

        Args:
        schema: BigQuery schema (list of SchemaField)
        prefix: Optional table alias prefix, e.g. "r"
    """
    clean_clause = []
    for field in schema:
        field_ref = f"{prefix}.{field.name}" if prefix else field.name

        if field.field_type == "STRING":
            clean_expr = (
                f"TRIM(UPPER(REGEXP_REPLACE("
                f"REGEXP_REPLACE(NORMALIZE({field_ref}, NFD), r'\pM', ''), "
                f"r'\\s+', ' ')))"
            )
            full_expr = (
                f"CASE WHEN UPPER(TRIM({field_ref})) IN ('', 'NULL', 'N/A', 'NAN', 'NA', 'N.A!')"
                f"THEN NULL ELSE {clean_expr} END AS {field.name}"
            )
            clean_clause.append(full_expr)
        else:
            clean_clause.append(f"{field_ref} AS {field.name}")
    return ",\n    ".join(clean_clause)

def generate_clean_query(cfg:DatasetConfig, client:bigquery.Client, tableType:str) -> str:
    """
    Apply the cleaning clause to the table of the specified type (tableType).

    Args:
        tableType: Type of table to clean (raw, clean, excluded)
    """
    table = obtain_table_name(cfg, tableType)
    data = client.get_table(f"{cfg.project}.{cfg.dataset}.{table}")
    clean_clause_str = generate_clean_clause(data.schema, None)

    clean_query = f"""
    SELECT {clean_clause_str} 
    FROM `{cfg.project}.{cfg.dataset}.{table}`
    """
    return clean_query

