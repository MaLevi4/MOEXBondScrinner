# -*- coding: utf-8 -*-
import logging
from datetime import datetime
from MOEXBondScrinner import BondsMOEXDataRetriever, BondsMOEXFilter, BondsCustomCalculationAndFilter, BondsCSVWriter

# Set logging variables
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logging.root.name = "BondsSearch"

# Set personal parameters
# Important
max_bond_value = 10000
max_expiration_date = datetime(2030, 1, 1)
is_offert_interesting = True
is_amortization_interesting = True
min_profit_ratio = 0.05
max_offert_date = max_expiration_date
commission_ratio = 0.01 / 100
# Common
is_qualified = False
is_noliquid_interesting = False
is_infinity_interesting = False
min_bond_value = None
max_profit_ratio = None
min_expiration_date = None
min_offert_date = min_expiration_date

# Set black lists
isin_black_list = ['RU000A0DY8K8', 'RU000A0JXPQ1']

# Start searching
bonds_list = BondsMOEXDataRetriever.load_or_retrieve()

# Common filters
# Filtering out bonds for qualified investors
if not is_qualified:
    bonds_list = BondsMOEXFilter.filter_bonds_by_qualification(bonds_list)
# Filter out bonds without information about last price
bonds_list = BondsMOEXFilter.filter_bonds_by_null_price(bonds_list)
# Filter out bonds with low amout of sales recently
if not is_noliquid_interesting:
    bonds_list = BondsMOEXFilter.filter_bonds_without_sales(bonds_list)

# Personal filters
# Filter out bonds by bond value
bonds_list = BondsMOEXFilter.filter_bonds_by_value(bonds_list, max_bond_value, bottom_bound=min_bond_value)
# Filter out bonds with offert or based of offert date
if not is_offert_interesting:
    bonds_list = BondsMOEXFilter.filter_bonds_by_offer(bonds_list)
else:
    bonds_list = BondsMOEXFilter.filter_bonds_by_expiration_date(bonds_list, max_offert_date, bottom_bound=min_offert_date,
                                                 filter_infinity=False, use_offer_date=True)
# Filter out bonds by expiration date
bonds_list = BondsMOEXFilter.filter_bonds_by_expiration_date(bonds_list, max_expiration_date, bottom_bound=min_expiration_date,
                                             filter_infinity=not is_infinity_interesting)
# Filter out bonds with amortization
if not is_amortization_interesting:
    bonds_list = BondsMOEXFilter.filter_bonds_by_amortization(bonds_list)

# Black list ISIN
bonds_list = BondsMOEXFilter.filter_bonds_by_isin_blacklist(bonds_list, isin_black_list)
# Calculate profit ratio
BondsCustomCalculationAndFilter.calculate_bonds_profit(bonds_list, commission_ratio)
# Filter out by profit ratio
bonds_list = BondsCustomCalculationAndFilter.filter_bonds_by_profit_ratio(bonds_list, min_profit_ratio,
                                                                          upper_bound=max_profit_ratio)
# Add information about emitters
bonds_list = BondsCustomCalculationAndFilter.enrich_bonds_emitter_from_db(bonds_list)
# Filter out emitters with 'exclude' risk
bonds_list = BondsCustomCalculationAndFilter.filter_bonds_by_emitter(bonds_list)
# Fix mistakes
BondsCustomCalculationAndFilter.force_moex_mistakes(bonds_list)
# Output to CSV
BondsCSVWriter.output_csv(bonds_list, is_offert_interesting)
