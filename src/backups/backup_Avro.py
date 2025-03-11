import pandas as pd
import os
from google.cloud import bigquery
from fastavro import writer, reader, parse_schema
from src.utils.clean_utils import drop_duplicates_ignore_columns

# Generar el esquema .AVRO dinámicamente
def infer_avro_schema(df, name):
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

#  Leer backup .AVRO existente
def read_existing_avro(file_path):
    if not os.path.exists(file_path):
        return pd.DataFrame()
    
    with open(file_path, "rb") as f:
        existing_records = list(reader(f))
    return pd.DataFrame(existing_records)

# Devolver la diferencia de datos (nuevos registros .AVRO)
def get_new_rows_to_append(df_new, backup_path):
    try:
        df_existing = read_existing_avro(backup_path)
        if df_existing.empty:
            return df_new.drop_duplicates()

        compare_cols = [col for col in df_new.columns if col != "uuid"]
        merged = df_new.merge(df_existing[compare_cols], on=compare_cols, how='left', indicator=True)
        new_rows = merged[merged['_merge'] == 'left_only']
        new_rows = new_rows.drop(columns=["_merge"]).drop_duplicates()
        return new_rows
    except FileNotFoundError:
        return df_new.drop_duplicates()

    except Exception as e:
        print(f"Error comparing backups: {e}")
        return pd.DataFrame()
    
def backup_table_to_avro(table_name: str, project_id: str, dataset_id: str, output_dir: str = "backups"):
    try:
        client = bigquery.Client(project=project_id)
        table_ref = f"{project_id}.{dataset_id}.{table_name}"
        
        query = f"SELECT * FROM `{table_ref}`"
        df_new = client.query(query).to_dataframe()
        df_new = df_new.drop_duplicates()

        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, f"{table_name}.avro")

        df_new_unique = get_new_rows_to_append(df_new, output_file)
        if df_new_unique.empty:
            print(f"No hay nuevos registros para agregar en {table_name}.")
            return
        
        records = df_new_unique.to_dict(orient="records")
        schema = infer_avro_schema(df_new_unique, table_name)
        parsed_schema = parse_schema(schema)

        with open(output_file, "wb") as out: 
            writer(out, parsed_schema, records)
        print(f"Backup actualizado y guardado en {output_file} sin duplicados.")

    except Exception as e:
        print(f"Error backing up tabla {table_name}: {e}")
