from google.cloud import bigquery
from google.api_core.exceptions import NotFound

import pandas as pd
import os 
from src.get_data import get_validated_data
from src.utils.clean_utils import prepare_values_for_insert
from src.utils.bd_utils import get_connection
from src.config import settings


# Inicialización
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r".\src\bigquery-credencial_2.json"

config = settings['bd']
project_id = config.project_id
dataset_id = config.dataset_id
client = bigquery.Client(project=project_id)

get_connection(project_id)

def upload_dataframe_to_bq(df, table_name, project_id, dataset_id):
    try:
        client = bigquery.Client(project=project_id)
        table_ref = f"{project_id}.{dataset_id}.{table_name}"

        if not isinstance(df, pd.DataFrame):
            df = pd.DataFrame(df)

        try:
            client.get_table(table_ref)
            table_exists = True
        except NotFound:
            table_exists = False

        if not table_exists:
            schema = []
            for column in df.columns:
                column_name = column if isinstance(column, str) else column[0]

                if pd.api.types.is_integer_dtype(df[column]):
                    schema.append(f"{column_name} INT64")
                elif pd.api.types.is_float_dtype(df[column]):
                    schema.append(f"{column_name} FLOAT64")
                elif pd.api.types.is_bool_dtype(df[column]):
                    schema.append(f"{column_name} BOOL")
                else:
                    schema.append(f"{column_name} STRING")

            create_table_sql = f"""
            CREATE TABLE `{table_ref}` (
                {', '.join(schema)}
            )
            """
            client.query(create_table_sql).result()
            print(f"Tabla {table_ref} creada exitosamente.")
        else:
            print(f"La tabla {table_ref} ya existe. Se agregarán los datos.")

        # Preparar los valores para la inserción
        values = prepare_values_for_insert(df)
        values_str = ', '.join([str(value) for value in values])
        columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]
        columns_str = ', '.join(columns)

        insert_query = f"""INSERT INTO `{table_ref}` ({columns_str}) VALUES {values_str}"""
        client.query(insert_query).result()
        
        print(f"Subida exitosa a la tabla: {table_ref}")

    except Exception as e:
        print(f"Error al subir {table_name}: {e}")



def main():
    try:
        df_departments, df_jobs, df_hired_employees = get_validated_data()
        # print('paso', df_departments)
    except Exception as e:
        print(f"Error get_data: {e}")
        return

    upload_dataframe_to_bq(df_departments, "departments", project_id, dataset_id)
    upload_dataframe_to_bq(df_jobs, "jobs", project_id, dataset_id)
    upload_dataframe_to_bq(df_hired_employees, "hired_employees", project_id, dataset_id)

if __name__ == "__main__":
    main()
