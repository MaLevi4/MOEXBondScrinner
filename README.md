# MOEXBondScrinner

MOEXBondScrinner is a python-library to grab info about russian bonds from MOEX web-site (using API), filter this data based on user-provided parameters and output filtered and enreached data as CSV-file.

### Key features:
  - Custom profit ratio calculation (including broker commission and taxes).
  - Ability to enrich data with your subjective opinion about emitters for further filtering or sorting.
  - Resulting csv-file is ready to one-click open in MS Excel (for russian localized Windows).

### Quick start
Copy MOEXBondScrinner.py to your project's folder and start use library the way as it shown in example.py.

### Most commonly used functiouns
- `BondsMOEXDataRetriever.load_or_retrieve()` - Function that loads full data about bonds from MOEX. Received data will be cached in local .json file for future use. If data was already retrieved today, this function will load it from cached .json file. Returns list of dicts with info about bonds: every dict corresponds to one bond.

- `BondsMOEXFilter.filter_bonds_advanced(bonds_list, filter_description_dict)` - Function that filters list of bonds based on parameters that can be received from MOEX API. Returns list of dicts with info about bonds.

Input parameter `bonds_list` - list of dicts with info about bonds, that should be filtered.

Input parameter `filter_description_dict` - dict of parameters which should be used while filtering.

Most commonly used parameters in `filter_description_dict`. (Detailed list can be found in documentation)

| Parameter | Type | Description |
| ------ | ------ | ------ |
| max_bond_value | integer | Maximum value of bond |
| max_expiration_date | datetime | Maximum date of expiration of bond.  |
| is_offert_interesting | boolean | If set to *False* bonds with offer will be filtered out |
| is_amortization_interesting | boolean | If set to *False* bonds with amortization will be filtered out |
- `BondsCustomCalculationAndFilter.calculate_bonds_profit(bonds_list, commission_ratio)` - Function that calculates profit ratio for all bonds in bonds_list. Returns nothing: input list `bonds_list` is updated.

Input parameter `bonds_list` - list of dicts with info about bonds, that should be updated.

Input parameter `commission_ratio` - float value of your brokerage commission.

- `BondsCustomCalculationAndFilter.filter_bonds_by_profit_ratio(bonds_list, min_profit_ratio)` - Function that filters input dict based on profit ratio. Returns list of dicts with info about bonds.

Input parameter `bonds_list` - list of dicts with info about bonds, that should be filtered.

Input parameter `min_profit_ratio` - float value that will be used as bottom border in filtering.
- `BondsCustomCalculationAndFilter.enrich_bonds_emitter_local(bonds_list)` - Function that adds info about emitter to every bond. Info about emitters is stored localy in 'emitters.db' SQLite3 database. Returns list of dicts with info about bonds.

Input parameter `bonds_list` - list of dicts with info about bonds, that should be saved.
- `BondsCSVWriter.output_csv(bonds_list)` - Function that saves input list of bonds to .csv file. By default filename 'result.csv' is used, but it can be set as optional input paramater `filename`. Returns nothing.

Input parameter `bonds_list` - list of dicts with info about bonds, that should be saved.
### Additional installation steps
This steps should be used if you are interesting in adding info about emitters in resulting output. Since emitters analysis is quite subjective this data are not provided with this library and should be filled by yourself.
1. Fill 'emitters.json' file with your information about emitters.
2. Run init_emitter_db.py to create emitter.db SQLite3 file which will be used to enrich data about emitters.

### Library files description
| File | Description |
| ------ | ------ |
| MOEXBondScrinner.py | Main lib file. Contains 4 classes. |
| init_emitter_db.py | Script for SQLite3 database creation. This database is used to store emitters description |
| emitters.json | Example of file with emitters info for **init_emitter_db.py** script |
| example.py | Example of lib usage |
| example_advanced.py | Extended example of lib usage |

### License: MIT