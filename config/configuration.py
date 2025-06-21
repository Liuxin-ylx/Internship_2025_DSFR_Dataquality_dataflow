from dataclasses import dataclass

@dataclass
class DatasetConfig:
    dataflow: str = "liuxin_dataflow_temp"
    project: str = "ds-fra-eu-non-pii-prod"
    dataset: str = "LIUXIN"
    
    raw_table: str = "crf_item_actif_avec_CA_1609"
    clean_table: str = "crf_item_actif_avec_CA_1609_cleaned"
    excluded_table: str = "crf_item_actif_avec_CA_1609_excluded"

    dataset_type: str = "supermarket" # or "cinema"
    key_columns: str = "country_id, barcode"
    barcode_columns: str = "barcode"
    date_columns: str = "non_active_date"