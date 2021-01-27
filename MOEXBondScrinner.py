# -*- coding: utf-8 -*-
import urllib.request
import json
import logging
import os
import csv
import sqlite3
from datetime import datetime, timedelta


class BondsMOEXDataRetriever:
    @staticmethod
    def load_or_retrieve(bonds_group_list=(7, 58)):
        cache_filename = datetime.strftime(datetime.today(), "%Y-%m-%d") + ".json"
        if not os.path.isfile(cache_filename):
            logging.info("No cached data is found. Please wait until current data will be retrieved.")
            bonds_list = BondsMOEXDataRetriever.get_bonds_info(bonds_group_list)
            cached_status = "list_only"
            BondsMOEXDataRetriever.dump_results_to_file(bonds_list, cache_filename, cached_status)
        else:
            logging.info("Found cached data. Less new requests will be required.")
            cached_object = BondsMOEXDataRetriever.load_results_from_file(cache_filename)
            cached_status = cached_object.get("status", "list_only")
            bonds_list = cached_object.get("data", [])

        if cached_status == "list_only":
            logging.info("There is no data about bonds description. This data will be retrieved.")
            bonds_list = BondsMOEXDataRetriever.enrich_bonds_description(bonds_list)
            cached_status = "with_descr"
            BondsMOEXDataRetriever.dump_results_to_file(bonds_list, cache_filename, cached_status)

        if cached_status == "with_descr":
            logging.info("There is no data about bonds payments. This data will be retrieved.")
            bonds_list = BondsMOEXDataRetriever.enrich_bonds_payments(bonds_list)
            cached_status = "with_paym"
            BondsMOEXDataRetriever.dump_results_to_file(bonds_list, cache_filename, cached_status)

        if cached_status == "with_paym":
            logging.info("There is no data about bonds sales history. This data will be retrieved.")
            bonds_list = BondsMOEXDataRetriever.enrich_bonds_sales_history(bonds_list)
            cached_status = "with_sales"
            BondsMOEXDataRetriever.dump_results_to_file(bonds_list, cache_filename, cached_status)

        logging.info(str(len(bonds_list)) + " bonds is loaded for analyzing.")
        return bonds_list

    @staticmethod
    def get_bonds_info(bounds_group_list):
        result = []
        for bonds_group in bounds_group_list:
            # additional info about coupon can be found in COUPONPERCENT, COUPONVALUE, NEXTCOUPON, COUPONPERIOD
            request_url = "https://iss.moex.com/iss/engines/stock/markets/bonds/boardgroups/" + str(bonds_group) + \
                          "/securities.json?iss.meta=off&iss.only=securities" \
                          "&securities.columns=SECID,ISIN,SHORTNAME,SECNAME,PREVPRICE,LOTSIZE,FACEVALUE," \
                          "MATDATE,OFFERDATE,FACEUNIT,ACCRUEDINT,SECTYPE"
            logging.debug("Request url for bonds group: " + request_url)
            content = urllib.request.urlopen(request_url, timeout=60).read()
            data = json.loads(content)
            converted_data = BondsMOEXDataRetriever._convert_data_to_dict(data, "securities")
            result = result + converted_data
        logging.info("Found " + str(len(result)) + " bonds")
        return result

    @staticmethod
    def get_bond_description(sec_id):
        request_url = "https://iss.moex.com/iss/securities/" + str(sec_id) + \
                      ".json?iss.meta=off&iss.only=description&description.columns=name,value"
        logging.debug("Request url for bond description: " + request_url)
        content = urllib.request.urlopen(request_url, timeout=60).read()
        data = json.loads(content)
        result = {}
        for line in data['description']['data']:
            key = line[0]
            if key in ("ISQUALIFIEDINVESTORS", "TYPE", "EMITTER_ID"):
                result[key] = line[1]
        return result

    @staticmethod
    def get_bond_payments(sec_id):
        request_url = "https://iss.moex.com/iss/statistics/engines/stock/markets/bonds/bondization/" + str(sec_id) + \
                      ".json?iss.meta=off&iss.only=amortizations,coupons,offers&limit=unlimited" \
                      "&amortizations.columns=amortdate,faceunit,value" \
                      "&coupons.columns=coupondate,faceunit,value" \
                      "&offers.columns=offerdate,offertype"
        logging.debug("Request url for bond payments: " + request_url)
        content = urllib.request.urlopen(request_url, timeout=60).read()
        data = json.loads(content)
        amortizations_data = BondsMOEXDataRetriever._convert_data_to_dict(data, "amortizations")
        coupons_data = BondsMOEXDataRetriever._convert_data_to_dict(data, "coupons")
        offers_data = BondsMOEXDataRetriever._convert_data_to_dict(data, "offers")
        return amortizations_data, coupons_data, offers_data

    @staticmethod
    def get_bonds_sales_history(sec_id, days_delta=15):
        today = datetime.today()
        date_from = today - timedelta(days=days_delta)
        request_url = "https://iss.moex.com/iss/history/engines/stock/markets/bonds/securities/" + str(sec_id) + \
                      ".json?iss.meta=off&iss.only=history&history.columns=TRADEDATE,VOLUME,NUMTRADES" \
                      "&limit=20&from=" + datetime.strftime(date_from, '%Y-%m-%d')
        logging.debug("Request url for bond sales history: " + request_url)
        content = urllib.request.urlopen(request_url, timeout=60).read()
        data = json.loads(content)
        return BondsMOEXDataRetriever._convert_data_to_dict(data, "history")

    @staticmethod
    def enrich_bonds_description(bonds_list):
        result = []
        for bond in bonds_list:
            if "SECID" not in bond:
                logging.error("While executing function 'enrich_bonds_description' can not find 'SECID' for bond " + str(bond))
                continue
            sec_id = bond["SECID"]
            bond_description = BondsMOEXDataRetriever.get_bond_description(sec_id)
            bond_enriched = dict(bond)
            bond_enriched.update(bond_description)
            result.append(bond_enriched)
        logging.info("Successfully enriched description for " + str(len(result)) + " bonds")
        return result

    @staticmethod
    def enrich_bonds_payments(bonds_list):
        result = []
        for bond in bonds_list:
            if "SECID" not in bond:
                logging.error("While executing function 'enrich_bonds_payments' can not find 'SECID' for bond " + str(bond))
                continue
            sec_id = bond["SECID"]
            (amortizations_data, coupons_data, offers_data) = BondsMOEXDataRetriever.get_bond_payments(sec_id)
            bond_enriched = dict(bond)
            bond_enriched["amortizations"] = amortizations_data
            bond_enriched["coupons"] = coupons_data
            bond_enriched["offers"] = offers_data
            result.append(bond_enriched)
        logging.info("Successfully enriched payments for " + str(len(result)) + " bonds")
        return result

    @staticmethod
    def enrich_bonds_sales_history(bonds_list):
        result = []
        for bond in bonds_list:
            if "SECID" not in bond:
                logging.error("While executing function 'enrich_bonds_sales_history' can not find 'SECID' for bond " + str(bond), exc_info=True)
                continue
            sec_id = bond["SECID"]
            bond_sales_history = BondsMOEXDataRetriever.get_bonds_sales_history(sec_id)
            bond_enriched = dict(bond)
            bond_enriched['sales_history'] = bond_sales_history
            result.append(bond_enriched)
        logging.info("Successfully enriched sales history for " + str(len(result)) + " bonds")
        return result

    @staticmethod
    def dump_results_to_file(bonds_list, filename, status):
        cached_object = {"data": bonds_list, "status": status}
        fh = open(filename, 'w+')
        fh.write(json.dumps(cached_object))
        fh.close()
        logging.info("Data was successfully saved into '" + filename + "' file.")

    @staticmethod
    def load_results_from_file(filename):
        fh = open(filename, 'r')
        bonds_list = json.loads(fh.read())
        fh.close()
        return bonds_list

    @staticmethod
    def _convert_data_to_dict(data, root_name):
        field_name_list = data[root_name]['columns']
        result = []
        for line in data[root_name]["data"]:
            current_dict = {}
            for i in range(len(field_name_list)):
                current_dict[field_name_list[i]] = line[i]
            result.append(current_dict)
        return result


class BondsMOEXFilter:
    @staticmethod
    def filter_bonds_advanced(bonds_list, filter_description_dict):
        # Get all filtering options from filter_description_dict
        max_bond_value = filter_description_dict.get('max_bond_value', None)
        min_bond_value = filter_description_dict.get('min_bond_value', None)
        max_expiration_date = filter_description_dict.get('max_expiration_date', None)
        min_expiration_date = filter_description_dict.get('min_expiration_date', None)
        is_offert_interesting = filter_description_dict.get('is_offert_interesting', True)
        is_amortization_interesting = filter_description_dict.get('is_amortization_interesting', True)
        is_qualified = filter_description_dict.get('is_qualified', False)
        is_noliquid_interesting = filter_description_dict.get('is_noliquid_interesting', False)
        is_infinity_interesting = filter_description_dict.get('is_infinity_interesting', False)
        max_offert_date = filter_description_dict.get('max_offert_date', max_expiration_date)
        min_offert_date = filter_description_dict.get('min_offert_date', min_expiration_date)
        sales_threshold_amount = filter_description_dict.get('sales_threshold_amount', 50)
        sales_threshold_deal = filter_description_dict.get('sales_threshold_deal', 10)

        result = []
        for line in bonds_list:
            try:
                # Getting all important values for bond
                prev_price = line.get('PREVPRICE', None)
                is_for_qualified = int(line["ISQUALIFIEDINVESTORS"])
                bond_value = int(line["FACEVALUE"])
                expiration_date = line["MATDATE"]
                offer_date = line["OFFERDATE"]
                amortizations = line["amortizations"]
                sales_history = line["sales_history"]
                total_sales_volume = 0
                total_sales_deals = 0
                for day in sales_history:
                    total_sales_volume += day['VOLUME']
                    total_sales_deals += day['NUMTRADES']

                # Filtering by unknown last price
                if prev_price is None:
                    continue
                # Filtering by qualification
                if (not is_qualified) and (is_for_qualified == 1):
                    continue
                # Filtering by sales amount recently
                if (not is_noliquid_interesting) and \
                        (total_sales_volume <= sales_threshold_amount or total_sales_deals <= sales_threshold_deal):
                    continue
                # Filtering by bond value
                if (max_bond_value is not None) and (bond_value > max_bond_value):
                    continue
                if (min_bond_value is not None) and (bond_value < min_bond_value):
                    continue
                # Filtering by amortization
                if (not is_amortization_interesting) and (len(amortizations) > 1):
                    continue
                if (offer_date is not None):
                    # Filtering by offer
                    if (not is_offert_interesting):
                        continue
                    else:
                        # Filter by offer date
                        offer_date = datetime.strptime(offer_date, '%Y-%m-%d')
                        if (max_offert_date is not None) and (offer_date > max_offert_date):
                            continue
                        if (min_offert_date is not None) and (offer_date < min_offert_date):
                            continue
                else:
                    # Filter by expiration date
                    if not is_infinity_interesting:
                        if expiration_date == "0000-00-00" or expiration_date is None:
                            continue
                        expiration_date = datetime.strptime(expiration_date, '%Y-%m-%d')
                        if (max_expiration_date is not None) and (expiration_date > max_expiration_date):
                            continue
                        if (min_expiration_date is not None) and (expiration_date < min_expiration_date):
                            continue

                result.append(line)
            except KeyError:
                logging.error("Can not find important key for bond " + str(line), exc_info=True)
            except ValueError:
                logging.error("Bad time format for bond's expiration or offer date. Bond is: " + str(line),
                              exc_info=True)
        logging.info("After advanced filtering based on configuration " + str(len(result)) + " bonds left")
        return result

    @staticmethod
    def filter_bonds_by_expiration_date(bonds_list, upper_bound, bottom_bound=None,
                                        filter_infinity=True, use_offer_date=False):
        result = []
        for line in bonds_list:
            try:
                if not use_offer_date:
                    if line["OFFERDATE"] is not None:
                        result.append(line)
                        continue
                    expiration_date = line["MATDATE"]
                else:
                    expiration_date = line["OFFERDATE"]
                if expiration_date == "0000-00-00" or expiration_date is None:
                    if not filter_infinity:
                        result.append(line)
                    continue
                expiration_date = datetime.strptime(expiration_date, '%Y-%m-%d')
                if expiration_date > upper_bound:
                    continue
                if (bottom_bound is not None) and (expiration_date < bottom_bound):
                    continue
                result.append(line)
            except KeyError:
                logging.error("Can not find expiration or offer date for bond " + str(line), exc_info=True)
            except ValueError:
                logging.error("Bad time format for bond's expiration or offer date. Bond is: " + str(line),
                              exc_info=True)
        if not use_offer_date:
            logging.info("After filtering by expiration date " + str(len(result)) + " bonds left")
        else:
            logging.info("After filtering by offer date " + str(len(result)) + " bonds left")
        return result

    @staticmethod
    def filter_bonds_by_value(bonds_list, upper_bound, bottom_bound=None):
        result = []
        for line in bonds_list:
            try:
                value = int(line["FACEVALUE"])
                if value > upper_bound:
                    continue
                if (bottom_bound is not None) and (value < bottom_bound):
                    continue
                result.append(line)
            except KeyError:
                logging.error("Can not find 'FACEVALUE' for bond " + str(line), exc_info=True)
        logging.info("After filtering by value " + str(len(result)) + " bonds left")
        return result

    @staticmethod
    def filter_bonds_by_qualification(bonds_list):
        result = []
        for line in bonds_list:
            try:
                value = int(line["ISQUALIFIEDINVESTORS"])
                if value == 1:
                    continue
                result.append(line)
            except KeyError:
                logging.error("Can not find 'ISQUALIFIEDINVESTORS' for bond " + str(line), exc_info=True)
        logging.info("After filtering by qualification " + str(len(result)) + " bonds left")
        return result

    @staticmethod
    def filter_bonds_by_amortization(bonds_list):
        result = []
        for line in bonds_list:
            try:
                amortizations = line["amortizations"]
                if len(amortizations) > 1:
                    continue
                result.append(line)
            except KeyError:
                logging.error("Can not find 'amortizations' for bond " + str(line), exc_info=True)
        logging.info("After filtering by amortization " + str(len(result)) + " bonds left")
        return result

    @staticmethod
    def filter_bonds_by_offer(bonds_list):
        result = []
        for line in bonds_list:
            try:
                offer_date = line["OFFERDATE"]
                if offer_date is not None:
                    continue
                result.append(line)
            except KeyError:
                logging.error("Can not find 'OFFERDATE' for bond " + str(line), exc_info=True)
        logging.info("After filtering by offer " + str(len(result)) + " bonds left")
        return result

    @staticmethod
    def filter_bonds_by_null_price(bonds_list):
        result = []
        for line in bonds_list:
            prev_price = line.get('PREVPRICE', None)
            if prev_price is not None:
                result.append(line)
        logging.info("After filtering by unknown last price " + str(len(result)) + " bonds left")
        return result

    @staticmethod
    def filter_bonds_without_sales(bonds_list, threshold_deal=10, threshold_amount=50):
        result = []
        for line in bonds_list:
            try:
                sales_history = line["sales_history"]
                total_volume = 0
                total_deals = 0
                for day in sales_history:
                    total_volume += day['VOLUME']
                    total_deals += day['NUMTRADES']
                if total_volume > threshold_amount and total_deals > threshold_deal:
                    result.append(line)
            except KeyError:
                logging.error("Can not find 'sales_history' for bond " + str(line), exc_info=True)
        logging.info("After filtering by no sales recently " + str(len(result)) + " bonds left")
        return result

    @staticmethod
    def filter_bonds_by_isin_blacklist(bonds_list, black_list):
        result = []
        for line in bonds_list:
            try:
                isin = line["ISIN"]
                if isin not in black_list:
                    result.append(line)
            except KeyError:
                logging.error("Can not find 'ISIN' for bond " + str(line), exc_info=True)
        logging.info("After filtering by isin black list " + str(len(result)) + " bonds left")
        return result

    @staticmethod
    def check_specific_bond_existence(bonds_list, isin):
        for line in bonds_list:
            current_isin = line.get('ISIN', "")
            if current_isin == isin:
                print(line)
                return True
        return False

    @staticmethod
    def get_specific_bond(bonds_list, isin):
        for line in bonds_list:
            current_isin = line.get('ISIN', "")
            if current_isin == isin:
                return line
        return None


class BondsCustomCalculationAndFilter:
    @staticmethod
    def calculate_bonds_profit(bonds_list, commission_ratio):
        for bond in bonds_list:
            (bond_profit, profit_type, coupon_type) = BondsCustomCalculationAndFilter.calculate_bond_profit(bond, commission_ratio)
            bond['year_profit_ratio'] = bond_profit
            bond['profit_type'] = profit_type
            bond['coupon_type'] = coupon_type
        return

    @staticmethod
    def calculate_bond_profit(bond, commission_ratio):
        logging.debug("Starting to calculate profit for bond" + str(bond))
        tax_ratio = 0.13
        today = datetime.today()
        profit_type = "simple"
        coupon_type = "predefined"
        try:
            buy_price = bond['PREVPRICE'] * bond['FACEVALUE'] / 100.0
            current_coupon = bond['ACCRUEDINT']
            full_price = buy_price * (1 + commission_ratio) + current_coupon
            coupons = bond['coupons']
            amortizations = bond['amortizations']
            close_price = bond['FACEVALUE']
            offer_date = bond['OFFERDATE']
            if offer_date is None:
                close_date = datetime.strptime(bond['MATDATE'], '%Y-%m-%d')
            else:
                close_date = datetime.strptime(offer_date, '%Y-%m-%d')
                profit_type = "offert"
            duration = close_date - today
        except KeyError:
            logging.error("While calculating bond profit can not find fields for bond " + str(bond), exc_info=True)
            return 0, 'error'
        except ValueError:
            logging.error("While calculating bond profit bad time format for bond 'MATDATE' or 'OFFERDATE'. Bond is: " +
                          str(bond), exc_info=True)
            return 0, 'error'
        coupons_sum = 0
        profit_ratio_list = []
        last_known_coupon_value = 0
        last_amortization_day_number = 0
        coupons_calendar = BondsCustomCalculationAndFilter._convert_list_to_calendar(coupons, 'coupondate', 'value')
        amortization_calendar = BondsCustomCalculationAndFilter._convert_list_to_calendar(amortizations, 'amortdate', 'value')
        if offer_date is not None:
            amortization_calendar[offer_date] = [close_price]
        for day_number in range(1, duration.days + 2):
            day = datetime.strftime(today + timedelta(days=day_number), "%Y-%m-%d")
            this_day_coupon = coupons_calendar.get(day, [])
            this_day_amortization = amortization_calendar.get(day, [])
            for coupon_value in this_day_coupon:
                if coupon_value is None:
                    coupon_type = "extrapolated"
                    coupons_sum += last_known_coupon_value
                else:
                    coupons_sum += coupon_value
                    last_known_coupon_value = coupon_value
            if this_day_amortization == 0:
                continue
            if len(this_day_amortization) > 0:
                current_duration = day_number - last_amortization_day_number
                clear_coupons_sum = coupons_sum * (1 - tax_ratio)
                clear_income = clear_coupons_sum + close_price
                clear_profit = clear_income - full_price
                profit_ratio = clear_profit / full_price
                profit_year_ratio = profit_ratio / current_duration * 365
                profit_ratio_list.append(profit_year_ratio)

                last_amortization_day_number = day_number
                close_price = close_price - this_day_amortization[0]
                full_price = close_price
                coupons_sum = 0
        if len(profit_ratio_list) > 1:
            profit_type = 'amortization'
        profit_year_ratio = sum(profit_ratio_list) / len(profit_ratio_list)
        return profit_year_ratio, profit_type, coupon_type

    @staticmethod
    def calculate_bond_profit_old(bond, commission_ratio):
        logging.debug("Starting to calculate profit for bond" + str(bond))
        tax_ratio = 0.13
        today = datetime.today()
        profit_type = "simple"
        try:
            buy_price = bond['PREVPRICE'] * bond['FACEVALUE'] / 100.0
            current_coupon = bond['ACCRUEDINT']
            full_price = buy_price * (1 + commission_ratio) + current_coupon
            coupons = bond['coupons']
            close_price = bond['FACEVALUE']
            offer_date = bond['OFFERDATE']
            if offer_date is None:
                close_date = datetime.strptime(bond['MATDATE'], '%Y-%m-%d')
            else:
                close_date = datetime.strptime(offer_date, '%Y-%m-%d')
                profit_type = "offert"
            duration = close_date - today
        except KeyError:
            logging.error("While calculating bond profit can not find fields for bond " + str(bond), exc_info=True)
            return 0, 'error'
        except ValueError:
            logging.error("While calculating bond profit bad time format for bond 'MATDATE' or 'OFFERDATE'. Bond is: " +
                          str(bond), exc_info=True)
            return 0, 'error'
        coupons_sum = 0
        last_known_coupon_value = 0
        for coupon in coupons:
            try:
                coupon_date = datetime.strptime(coupon['coupondate'], '%Y-%m-%d')
                if coupon_date < today:
                    continue
                if profit_type.startswith("offert") and coupon_date > close_date:
                    continue
                if coupon['value'] is None:
                    profit_type = "offert_extrapol" if profit_type.startswith("offert") else "extrapol"
                    logging.info("Coupons are not known yet for bond " + bond['ISIN'] +
                                 ". Last known coupon value will be used for profit calculation.")
                    coupons_sum += last_known_coupon_value
                else:
                    coupons_sum += coupon['value']
                    last_known_coupon_value = coupon['value']
            except KeyError:
                logging.error("While calculating bond profit can not find field 'coupondate' or 'value' or"
                              " 'ISIN' for bond " + str(bond), exc_info=True)
            except ValueError:
                logging.error("While calculating bond profit bad time format for coupon date. Bond is: " + str(bond),
                              exc_info=True)
        clear_coupons_sum = coupons_sum * (1 - tax_ratio)
        value_diff = close_price - buy_price - current_coupon
        price_tax = (value_diff * tax_ratio) if value_diff > 0 else 0
        clear_income = clear_coupons_sum + close_price - price_tax
        clear_profit = clear_income - full_price
        profit_ratio = clear_profit / full_price
        profit_year_ratio = profit_ratio / duration.days * 365
        return profit_year_ratio, profit_type

    @staticmethod
    def enrich_bonds_emitter_local(bonds_list, local_db_name='emitters.db'):
        if not os.path.isfile(local_db_name):
            logging.warning("Local database with name '" + local_db_name + "' is not found. Can not enrich emitters")
            return bonds_list
        connection = sqlite3.connect(local_db_name)
        cursor = connection.cursor()
        result = []
        for bond in bonds_list:
            try:
                emitter_id = bond["EMITTER_ID"]
                sql_query = "SELECT name, risk FROM emitters WHERE id='{0}'".format(emitter_id)
                logging.debug("SQL query for emitter enrichment is " + sql_query)
                for row in cursor.execute(sql_query):
                    bond["EMITTER_ID"] = row[0]
                    bond["emitter_risk"] = row[1]
                result.append(bond)
            except KeyError:
                logging.error("Can not find 'EMITTER_ID' for bond " + str(bond), exc_info=True)
        connection.close()
        logging.info("Successfully enriched emitter name for " + str(len(result)) + " bonds")
        return result

    @staticmethod
    def filter_bonds_by_profit_ratio(bonds_list, bottom_bound, upper_bound=None):
        result = []
        for line in bonds_list:
            try:
                profit_ratio = line["year_profit_ratio"]
                if profit_ratio < bottom_bound:
                    continue
                if (upper_bound is not None) and (profit_ratio < upper_bound):
                    continue
                result.append(line)
            except KeyError:
                logging.error("Can not find 'year_profit_ratio' for bond " + str(line), exc_info=True)
        logging.info("After filtering by profit ratio " + str(len(result)) + " bonds left")
        return result

    @staticmethod
    def filter_bonds_by_emitter(bonds_list):
        result = []
        for line in bonds_list:
            emitter_risk = line.get('emitter_risk', '')
            if emitter_risk == "exclude":
                continue
            result.append(line)
        logging.info("After filtering by emitter black list " + str(len(result)) + " bonds left")
        return result

    @staticmethod
    def force_moex_mistakes(bonds_list, mistakes_dict=None):
        if mistakes_dict is None:
            mistakes_dict = {'RU000A0JX0H6': {'coupon_type': 'extrapolated'}}
        for line in bonds_list:
            try:
                isin = line["ISIN"]
                if isin in mistakes_dict.keys():
                    for force_key in mistakes_dict[isin].keys():
                        line[force_key] = mistakes_dict[isin][force_key]
            except KeyError:
                logging.error("Can not find 'ISIN' for bond " + str(line), exc_info=True)
        logging.info("Fix MOEX mistakes is ended for " + str(len(mistakes_dict.keys())) + " bonds.")

    @staticmethod
    def _convert_list_to_calendar(input_list, date_field_name, value_field_name):
        calendar = {}
        for entry in input_list:
            current_date = entry.get(date_field_name)
            if current_date not in calendar:
                calendar[current_date] = []
            calendar[current_date].append(entry.get(value_field_name, 0))
        return calendar


class BondsCSVWriter:
    @staticmethod
    def output_csv(bonds_list, remove_offer_date=False, filename='result.csv'):
        cleared_result = []
        field_names = ['ISIN', 'SHORTNAME', 'SECNAME', 'FACEVALUE', 'PREVPRICE', 'MATDATE',
                       'TYPE', 'EMITTER_ID', 'emitter_risk', 'year_profit_ratio', 'profit_type', 'coupon_type']
        if not remove_offer_date:
            field_names = field_names[:5] + ['OFFERDATE'] + field_names[5:]
        for line in bonds_list:
            current_bond = {}
            for key in line:
                if key in field_names:
                    current_bond[key] = line[key]
            cleared_result.append(current_bond)

        with open(filename, 'w+', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=field_names, delimiter=";")
            writer.writeheader()
            for line in cleared_result:
                writer.writerow(BondsCSVWriter._localize_floats(line))

    @staticmethod
    def _localize_floats(input_dict):
        output_dict = {}
        for key in input_dict:
            if isinstance(input_dict[key], float):
                output_dict[key] = str(input_dict[key]).replace('.', ',')
            else:
                output_dict[key] = input_dict[key]
        return output_dict
