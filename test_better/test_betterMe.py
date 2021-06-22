from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import csv
import glob


project = 'clever-axe-251109'
dataset_id = 'Test_4'
table_id = 'Test_Better'
credentials = service_account.Credentials.from_service_account_file('../db_reporting.json')
client_bq = bigquery.Client(project, credentials=credentials)

def get_data():
    txt_list = glob.glob('*.txt')
    for idx, file in enumerate(txt_list):
        with open(file, 'r') as csvfile:
            dialect = csv.Sniffer().sniff(csvfile.readline())
            print(dialect.delimiter)
        txtdata = pd.read_csv(file, sep=dialect.delimiter)
        txtdata.columns = txtdata.columns.str.replace(' ', '_')
        txtdata.columns = txtdata.columns.str.replace('-', '_')
        if idx == 0:
            load_data(txtdata, "WRITE_TRUNCATE")
        else:
            load_data(txtdata, "WRITE_APPEND")



def load_data(df, write_disposition):
    dataset_ref = client_bq.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = write_disposition
    job_config.autodetepythonct = True

    job = client_bq.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Waits for table load to complete.
    print("Load {}, {} rows.".format(job.output_rows, table_id))


def main():
    get_data()


if __name__ == '__main__':
    main()
