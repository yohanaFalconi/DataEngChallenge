import pandas as pd


def load_bq_table(table_name,client,project_id,dataset_id):
    try:
        sql = f"SELECT * FROM `{project_id}.{dataset_id}.{table_name}`"
        query_job = client.query(sql)
        results = query_job.result() 
        return pd.DataFrame([dict(row) for row in results])
    except Exception as e:
        print(f"Error: Failed to load table '{table_name}': {e}")
        return pd.DataFrame()  