import boto3
import os
import logging
from datetime import datetime
from MOEXBondScrinner import BondsMOEXDataRetriever


def retrieve_and_save_to_s3(aws_access_key_id, aws_secret_access_key, s3_bucket_name):
    # init s3 object for yandex.cloud
    session = boto3.session.Session(aws_access_key_id, aws_secret_access_key)
    s3 = session.client(service_name='s3', endpoint_url='https://storage.yandexcloud.net')

    logging.info("Start retrieving current data.")
    bonds_list = BondsMOEXDataRetriever.get_bonds_info((7, 58))
    logging.info("There is no data about bonds description. This data will be retrieved.")
    bonds_list = BondsMOEXDataRetriever.enrich_bonds_description(bonds_list)
    logging.info("There is no data about bonds payments. This data will be retrieved.")
    bonds_list = BondsMOEXDataRetriever.enrich_bonds_payments(bonds_list)
    logging.info("There is no data about bonds sales history. This data will be retrieved.")
    bonds_list = BondsMOEXDataRetriever.enrich_bonds_sales_history(bonds_list)
    logging.info(f"{str(len(bonds_list))} bonds were loaded for analyzing.")

    cache_filename = "dump_" + datetime.strftime(datetime.today(), "%Y-%m-%d") + ".json"
    cached_status = "with_sales"
    BondsMOEXDataRetriever.dump_results_to_file(bonds_list, cache_filename, cached_status)
    s3.upload_file(cache_filename, s3_bucket_name, cache_filename)
    logging.info(f"File {cache_filename} has successfully uploaded to S3.")


if __name__ == '__main__':
    # Set logging variables
    logging_level = logging.DEBUG if 'DEBUG' in os.environ else logging.INFO
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging_level)
    logging.root.name = "BondsSearch"

    # Get S3 variables
    aws_access_key_id = os.environ.get("s3_key_id")
    aws_secret_access_key = os.environ.get("s3_key_secret")
    s3_bucket_name = os.environ.get("s3_bucket_name")

    retrieve_and_save_to_s3(aws_access_key_id, aws_secret_access_key, s3_bucket_name)
