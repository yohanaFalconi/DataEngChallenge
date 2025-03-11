from google.cloud import bigquery
import pandas as pd
import fastavro
from src.utils.clean_utils import drop_duplicates_ignore_columns

# Lee un archivo AVRO y devuelve un df
def read_avro_file_df(avro_path):
    try:
        with open(avro_path, 'rb') as f:
            reader = fastavro.reader(f)
            records = list(reader)
            df = pd.DataFrame(records)
            return drop_duplicates_ignore_columns(df, ignore_columns="uuid")

    except Exception as e:
        print(f"Error al leer archivo AVRO : {e}")
        return pd.DataFrame()


#Genera BigQuery LoadJobConfig para restaurar la tabla
def generate_restore_job_config_from_backup(table_name, backup_path):
    try:
        df = read_avro_file_df(backup_path)
        if df.empty:
            print("El archivo AVRO está vacío o falló al leer.")
            return

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Borra la tabla
            source_format=bigquery.SourceFormat.AVRO 
        )
        return job_config
    except Exception as e:
        print(f"Error al restaurar la tabla {table_name}: {e}")
