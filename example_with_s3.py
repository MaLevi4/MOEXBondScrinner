# -*- coding: utf-8 -*-
import os
import logging
from datetime import datetime
from MOEXBondScrinner import BondsMOEXFilter, BondsCustomCalculationAndFilter, BondsCSVWriter
from YacloudS3 import load_from_s3

# Set logging variables
logging_level = logging.DEBUG if 'DEBUG' in os.environ else logging.INFO
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging_level)
logging.root.name = "BondsSearch"

# Get S3 variables
aws_access_key_id = os.environ["s3_key_id"]
aws_secret_access_key = os.environ["s3_key_secret"]
s3_bucket_name = os.environ["s3_bucket_name"]

# Set personal parameters
filter_description_dict = {'max_bond_value': 100000,
                           'max_expiration_date': datetime(2030, 1, 1),
                           'is_offert_interesting': True,
                           'is_amortization_interesting': True}
min_profit_ratio = 0.03
commission_ratio = 0.01 / 100

# Set black lists
isin_black_list = ['RU000A0DY8K8', 'RU000A0JXPQ1']

# If you have configured YacloudS3.Dockerfile to run daily in Yandex cloud, just load cached data
bonds_list = load_from_s3(aws_access_key_id, aws_secret_access_key, s3_bucket_name)

# Filter out bonds based on personal parameters
bonds_list = BondsMOEXFilter.filter_bonds_advanced(bonds_list, filter_description_dict)
# Black list ISIN
bonds_list = BondsMOEXFilter.filter_bonds_by_isin_blacklist(bonds_list, isin_black_list)

# Calculate profit ratio
BondsCustomCalculationAndFilter.calculate_bonds_profit(bonds_list, commission_ratio)
# Filter out by profit ratio
bonds_list = BondsCustomCalculationAndFilter.filter_bonds_by_profit_ratio(bonds_list, min_profit_ratio)
# Add information about emitters
bonds_list = BondsCustomCalculationAndFilter.enrich_bonds_emitter_local(bonds_list)
# Filter out emitters with 'exclude' risk
bonds_list = BondsCustomCalculationAndFilter.filter_bonds_by_emitter(bonds_list)
# Fix mistakes
BondsCustomCalculationAndFilter.force_moex_mistakes(bonds_list)
# Output to CSV
BondsCSVWriter.output_csv(bonds_list)
