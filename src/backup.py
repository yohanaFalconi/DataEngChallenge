from google.cloud import bigquery
import db_dtypes
import pandas as pd
import fastavro
import os

# Generar el esquema AVRO dinámicamente
def infer_avro_schema(df: pd.DataFrame, name: str):
    fields = []
    for column in df.columns:
        dtype = df[column].dtype
        avro_type = "string" 

        if pd.api.types.is_integer_dtype(dtype):
            avro_type = "int"
        elif pd.api.types.is_float_dtype(dtype):
            avro_type = "float"
        elif pd.api.types.is_bool_dtype(dtype):
            avro_type = "boolean"

        fields.append({"name": column, "type": ["null", avro_type]}) 

    return {
        "doc": f"{name} backup schema",
        "name": name,
        "namespace": "backup.schema",
        "type": "record",
        "fields": fields
    }

def backup_table_to_avro(table_name: str, project_id: str, dataset_id: str, output_dir: str = "backups"):
    try:
        client = bigquery.Client(project=project_id)
        table_ref = f"{project_id}.{dataset_id}.{table_name}"
        
        query = f"SELECT * FROM `{table_ref}`"
        df = client.query(query).to_dataframe()
        
        if df.empty:
            print(f"No data found in table: {table_ref}")
            return

        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, f"{table_name}.avro")

        # 3. Convertir a formato AVRO
        records = df.to_dict(orient="records")
        schema = infer_avro_schema(df, table_name)

        with open(output_file, "wb") as out:
            fastavro.writer(out, schema=schema, records=records)

        print(f"Backup for table '{table_name}' saved to {output_file}")

    except Exception as e:
        print(f"Error backing up table {table_name}: {e}")
