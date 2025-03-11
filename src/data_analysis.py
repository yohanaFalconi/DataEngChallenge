import pandas as pd
import os
from src.utils.config import settings
from google.cloud import bigquery
from src.database.get_data import ( load_bd_table, joinned_validation)

def get_hires_by_quarter(df):
   try:
      df['datetime'] = pd.to_datetime(df['datetime'])
      df_2021 = df[df['datetime'].dt.year == 2021].copy()
      df_2021['quarter'] = 'Q' + df_2021['datetime'].dt.quarter.astype(str)

      hires_by_quarter = (
         df_2021.groupby(['department', 'job', 'quarter'])
         .size()
         .unstack(fill_value=0)  # convierte 'quarter' en columnas
         .reset_index()
         .rename_axis(None, axis=1)  # elimina el nombre del índice
      )

      for q in ['Q1', 'Q2', 'Q3', 'Q4']:
         if q not in hires_by_quarter.columns:
            hires_by_quarter[q] = 0

      hires_by_quarter = hires_by_quarter[['department', 'job', 'Q1', 'Q2', 'Q3', 'Q4']]
      hires_by_quarter = hires_by_quarter.sort_values(by=['department', 'job'])
      print(hires_by_quarter)

      return hires_by_quarter

   except Exception as e:
      print(f"Error: {e}")
      return pd.DataFrame()
   

def get_departments_above_avg_hires(df):
   try:
      df['datetime'] = pd.to_datetime(df['datetime'])
      df_2021 = df[df['datetime'].dt.year == 2021].copy()

      hires_by_department = (
         df_2021.groupby(['department_id', 'department'])
         .size()
         .reset_index(name='hired')
      )
      mean_hires = hires_by_department['hired'].mean()
      above_avg = hires_by_department[hires_by_department['hired'] >= mean_hires]
      result = above_avg.sort_values(by='hired', ascending=False).reset_index(drop=True)
      print(result)
      return result

   except Exception as e:
      print(f"Error: {e}")
      return pd.DataFrame()


# Inicialización
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r".\src\bigquery-credencial_2.json"

config = settings['bd']
project_id = config.project_id

dataset_id = config.dataset_id
client = bigquery.Client(project=project_id)

df =  load_bd_table('joinned_table', dataset_id)
df = joinned_validation(df)
get_hires_by_quarter(df)
get_departments_above_avg_hires(df)