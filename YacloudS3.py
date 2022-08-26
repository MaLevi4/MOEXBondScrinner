import boto3
import os
import logging
import json
from datetime import datetime
from MOEXBondScrinner import BondsMOEXDataRetriever


class YacloudS3:
    def __init__(self, aws_access_key_id, aws_secret_access_key, s3_bucket_name):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.s3_bucket_name = s3_bucket_name

        # init s3 object for yandex.cloud
        self.session = boto3.session.Session(aws_access_key_id, aws_secret_access_key)
        self.s3 = self.session.client(service_name='s3', endpoint_url='https://storage.yandexcloud.net')

    def upload_file(self, file_name):
        self.s3.upload_file(file_name, s3_bucket_name, file_name)

    def get_json_from_s3(self, file_name):
        logging.info(f"Start trying to get file {file_name} from s3.")
        try:
            json_content = json.loads(self.s3.get_object(Bucket=self.s3_bucket_name, Key=file_name)['Body'].read())
            logging.info(f"JSON content of file {file_name} was successfully retrieved from s3.")
            return json_content
        except:
            logging.error(f"Smth went wrong. JSON Content of file {file_name} can not be retrieved from s3")
            return None

    def load_bonds_from_s3(self):
        logging.info("Start trying to get current data from s3.")
        cache_filename = "dump_" + datetime.strftime(datetime.today(), "%Y-%m-%d") + ".json"
        cached_content = self.get_json_from_s3(cache_filename)

        if cached_content is None:
            logging.error("Smth went wrong. Content of cached file can not be retrieved from s3")
            raise

        cached_status = cached_content.get("status", "list_only")
        bonds_list = cached_content.get("data", [])

        if cached_status != "with_sales":
            logging.error("Smth went wrong. Cached file don't have proper cached_status")
            raise
        else:
            logging.info(f"{str(len(bonds_list))} bonds were loaded for analyzing.")
            return bonds_list

    def retrieve_and_save_to_s3(self):
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
        self.upload_file(cache_filename)
        logging.info(f"File {cache_filename} has successfully uploaded to S3.")


if __name__ == '__main__':
    # Set logging variables
    logging_level = logging.DEBUG if 'DEBUG' in os.environ else logging.INFO
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging_level)
    logging.root.name = "BondsSearch"

    # Get S3 variables
    aws_access_key_id = os.environ["s3_key_id"]
    aws_secret_access_key = os.environ["s3_key_secret"]
    s3_bucket_name = os.environ["s3_bucket_name"]

    yacloud_s3 = YacloudS3(aws_access_key_id, aws_secret_access_key, s3_bucket_name)
    yacloud_s3.retrieve_and_save_to_s3()
