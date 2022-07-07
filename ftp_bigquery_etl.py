from google.cloud import bigquery
from datetime import date, timedelta
import logging
import os
import re
import ftplib
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
    logger.info(f'File {path_to_file} has been successfully received.')

    return file_location


def load_file_bq(file_path, config):
    """Load data to Google BigQuery table.

    Args:
        file_path: Path to the file that has been downloaded.
        config: Dict with configuration settings for BigQuery load job.
    """

    project_id = config['project_id']
    dataset_id = config['dataset_id']
    table_id = config['table_id']

    table_ref = f'{project_id}.{dataset_id}.{table_id}'

    job_config = bigquery.LoadJobConfig()
    job_config.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY)
    job_config.source_format = config['source_format']
    job_config.write_disposition = config['write_disposition']
    job_config.autodetect = True

    if config['source_format'] == 'CSV':
        job_config.field_delimiter = config['delimiter']
        job_config.skip_leading_rows = 1

    # Load the file into BigQuery table.
    with open(file_path, 'rb') as f:
        load_job = bigquery_client.load_table_from_file(f, table_ref, location=config['location'],
                                                        job_config=job_config)

    # Waits for the job to complete.
    load_job.result()

    table = bigquery_client.get_table(table_ref)

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

        yesterday = date.today() - timedelta(1)
        str_yesterday = yesterday.strftime('%d-%m-%Y')

        bq_configuration = {
            'project_id': event['attributes']['project_id'],
            'dataset_id': event['attributes']['dataset_id'],
            'table_id': event['attributes']['table_id'],
            'delimiter': event['attributes']['delimiter'],
            'source_format': event['attributes']['source_format'].upper(),
            'location': event['attributes']['location'],
            'write_disposition': event['attributes']['write_disposition'].upper()
        }

        ftp_configuration = {
            'user': event['attributes']['user'],
            'password': event['attributes']['password'],
            'path_to_file': f"ftp://{event['attributes']['hostname']}/{str_yesterday}.csv"
        }

        host = re.sub('ftp://', '', re.findall('ftp://[^/]*', ftp_configuration['path_to_file'])[0])
        path = re.sub('/$', '', re.sub(f'ftp://{host}/', '', ftp_configuration['path_to_file']))

        # Go to directory for temporary files
        os.chdir(gc_write_dir)

        # Get the file from FTP
        try:
            ftp_file = get_file_ftp(host, path, ftp_configuration)
        except Exception:
            logger.exception('Exception occurred while getting the file from FTP')
            return 'failed'

        load_file_bq(ftp_file, bq_configuration)

        os.remove(ftp_file)

    return 'ok'
