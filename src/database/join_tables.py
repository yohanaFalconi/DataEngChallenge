from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import pandas as pd
import os 
from src.database.get_data import get_validated_data
from src.utils.clean_utils import prepare_values_for_insert
from src.utils.bd_utils import (get_connection, add_uuid_column)
from src.config import settings
from src.database.get_data import (
   load_bd_table,
   joinned_validation
)

# Inicialización
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r".\src\bigquery-credencial_2.json"

config = settings['bd']
project_id = config.project_id
dataset_id = config.dataset_id

get_connection(project_id)

def create_joinned_table(table_name, project_id, dataset_id):
    try:
        client = bigquery.Client(project=project_id)
        table_ref = f"{project_id}.{dataset_id}.{table_name}"

        try:
            client.get_table(table_ref)
            table_exists = True
        except NotFound:
            table_exists = False

        sql = f"""
            CREATE TABLE `{table_ref}` AS
            SELECT 
                hired.id,
                hired.name,
                hired.datetime,
                hired.department_id,
                dep.department,
                hired.job_id,
                job.job
            FROM `{project_id}.{dataset_id}.hired_employees` AS hired
            JOIN `{project_id}.{dataset_id}.departments` AS dep 
                ON hired.department_id = dep.id
            JOIN `{project_id}.{dataset_id}.jobs` AS job 
                ON hired.job_id = job.id
        """
        if not table_exists:
            client.query(sql).result()
            print(f"'{table_ref}'Table creaded correctly")

    except Exception as e:
        print(f"Error: Failed to create table '{table_name}': {e}")
        return pd.DataFrame()  

# create_joinned_table("joinned_table", project_id, dataset_id)


# def main():
#     try:
#         df =  load_bd_table('joinned_table')
#         print('1111',df)

#         df = joinned_validation(df)
#         print('2222',df)
#         # rows = [dict(row) for row in df]
#         # df_joinned_table = pd.DataFrame(rows)

#         return df
#     except Exception as e:
#         print(f"Error get_data: {e}")
#         return

# if __name__ == "__main__":
#     main()

# FROM `plucky-shell-453303-t9.jobs_database.hired_employees` hired
# JOIN `plucky-shell-453303-t9.jobs_database.departments` dep 
#   ON hired.department_id = dep.id
# JOIN `plucky-shell-453303-t9.jobs_database.jobs` job 
#   ON hired.job_id = job.id


