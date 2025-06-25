from dataclasses import dataclass,field
from typing import List

@dataclass
class DatasetConfig:
    project: str = "ds-fra-eu-non-pii-prod"
    dataset: str = "LIUXIN"
    
    raw_table: str = "crf_item_actif_avec_CA_1609"
    clean_table: str = "crf_item_actif_avec_CA_1609_cleaned"
    excluded_table: str = "crf_item_actif_avec_CA_1609_excluded"

    dataset_type: str = "supermarket" # or "cinema"
    primary_keys: List[str] = field(default_factory=lambda: ["country_id", "barcode"])
    barcode_columns: List[str] = field(default_factory=lambda: ["barcode"])
    date_columns: List[str] = field(default_factory=lambda: ["non_active_date"])