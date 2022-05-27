from google.cloud import bigquery
from datetime import date, timedelta
import logging
import os
import re
import ftplib
import pandas as pd
import base64

# Create a custom logger
logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

bigquery_client = bigquery.Client()

# The directory for storing temporary files
gc_write_dir = '/tmp'


def get_file_ftp(host, path_to_file, ftp_configuration):
    """Copy an existing file from FTP via ftp://*host*/*path_to_file* link to home directory.

    Args:
        host (str): FTP server host.
        path_to_file (str): The path to the file in FTP server.
        ftp_configuration: FTP configuration.

    Returns:
        Full path to the file that has been downloaded.
    """
    # Construct FTP object and get the file from a server
    with ftplib.FTP(host, user=ftp_configuration['user'], passwd=ftp_configuration['password']) as ftp_conn:
        filename = re.findall('[^/]*$', path_to_file)[0]
        with open(filename, 'wb') as wf:
            ftp_conn.retrbinary(f'RETR {filename}', wf.write)

    file_location = f'{gc_write_dir}/{filename}'
    logger.info(f'File {path_to_file} has got successfully.')

    return file_location


def transform_to_dataframe(file_location):
    """Convert data from CSV file to dataframe.

    Args:
        file_location (str): The location of the file for converting.

    Returns:
        Dataframe.
    """

    col_names = ["createdAt", "updatedAt", "shippingDate", "orderId", "productName", "productCategory", "metalType",
                 "size", "weight", "quantity", "price", "sale", "shippingCost", "orderSum", "orderSource", "city",
                 "status", "paymentMethod", "clientId", "userId", "phone", "email", "authPhone"]

    df = pd.read_csv(file_location, delimiter=";", encoding="cp1251", header=None, names=col_names, skiprows=1,
                     dtype={"quantity": "str", "clientId": "str", "userId": "str", "phone": "str", "authPhone": "str"})

    return df


def load_data_bq(client, table_id, dataset_id, project_id, data):
    """Load data to Google BigQuery table.

    Args:
        client: BigQuery Client.
        table_id (str): The table ID of BigQuery.
        dataset_id (str): The dataset ID of BigQuery.
        project_id (str): The project name of Google Cloud account.
        data (dataframe): Data to be loaded.
    """
    table_ref = f'{project_id}.{dataset_id}.{table_id}'

    job_config = bigquery.LoadJobConfig(
        time_partitioning=bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY),
        clustering_fields=['status'])

    # Make an API request.
    load_job = client.load_table_from_dataframe(data, table_ref, location='US', job_config=job_config)

    # Waits for the job to complete.
    load_job.result()

    table = client.get_table(table_ref)

    logger.info(f'Successfully loaded data to table {table.table_id}. Loaded {table.num_rows} rows.')


def main(event, context):
    """Get the file from the ftp server and load data to the BigQuery table.

    Args:
       event: The special object with Pub/Sub message attributes.
       context: Pub/Sub event metadata.

    Returns:
       str
    """

    pubsub_message = base64.b64decode(event['data']).decode('utf-8')

    if pubsub_message == 'get_ftp_data':

        table_id = event['attributes']['table_id']
        dataset_id = event['attributes']['dataset_id']
        project_id = event['attributes']['project_id']

        ftp_host_name = event['attributes']['hostname']
        yesterday = date.today() - timedelta(1)
        str_yesterday = yesterday.strftime('%d-%m-%Y')

        ftp_configuration = {
            'user': event['attributes']['user'],
            'password': event['attributes']['password'],
            'path_to_file': f'ftp://{ftp_host_name}/{str_yesterday}.csv'
        }

        host = re.sub('ftp://', '', re.findall('ftp://[^/]*', ftp_configuration['path_to_file'])[0])
        path = re.sub('/$', '', re.sub(f'ftp://{host}/', '', ftp_configuration['path_to_file']))

        # Go to directory for temporary files
        os.chdir(gc_write_dir)

        # Get the file from FTP
        try:
            source = get_file_ftp(host, path, ftp_configuration)
        except Exception as e:
            logger.exception('Exception occurred while getting the file from FTP')
            return 'failed'

        data = transform_to_dataframe(source)
        load_data_bq(bigquery_client, table_id, dataset_id, project_id, data)

        os.remove(source)

    return 'ok'
