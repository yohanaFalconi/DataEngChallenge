import pandas as pd
from src.database.get_data import (
   load_bd_table,
   joinned_validation
)

def get_hires_by_quarter(df):
   try:
      df['datetime'] = pd.to_datetime(df['datetime'])

      df_2021 = df[df['datetime'].dt.year == 2021].copy()

      # Creamos la columna de trimestre (Q1, Q2, etc.)
      df_2021['quarter'] = 'Q' + df_2021['datetime'].dt.quarter.astype(str)

      # Agrupamos por departamento, job y trimestre
      hires_by_quarter = (
         df_2021.groupby(['department', 'job', 'quarter'])
         .size()
         .unstack(fill_value=0)  # convierte 'quarter' en columnas
         .reset_index()
         .rename_axis(None, axis=1)  # elimina el nombre del índice
      )

      # Aseguramos que siempre existan las columnas Q1–Q4
      for q in ['Q1', 'Q2', 'Q3', 'Q4']:
         if q not in hires_by_quarter.columns:
               hires_by_quarter[q] = 0

      # Ordenamos las columnas
      hires_by_quarter = hires_by_quarter[['department', 'job', 'Q1', 'Q2', 'Q3', 'Q4']]

      # Ordenamos alfabéticamente por departamento y job
      hires_by_quarter = hires_by_quarter.sort_values(by=['department', 'job'])
      print(hires_by_quarter)

      return hires_by_quarter

   except Exception as e:
      print(f"Error: {e}")
      return pd.DataFrame()
   







df =  load_bd_table('joinned_table')
df = joinned_validation(df)
print('2222',df)

get_hires_by_quarter(df)
