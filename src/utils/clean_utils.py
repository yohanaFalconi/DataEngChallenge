import pandas as pd
import re

#Elimina espacios al inicio y final de todas las columnas tipo string
def trim_strings(df):
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].str.strip()
    return df

#Elimina caracteres especiales
def remove_special_chars(df, exclude_cols=[]):
    for col in df.select_dtypes(include='object').columns:
        if col not in exclude_cols:
            df[col] = df[col].apply(lambda x: re.sub(r'[^\w\s]', '', x) if isinstance(x, str) else x)
    return df

#Elimina nulos
def drop_na(df, threshold=0.5):
    return df.dropna(axis=1, thresh=int((1 - threshold) * len(df)))


#Convierte cabeceras a tipo snake_case, elimina caracteres especiales y espacios
def normalize_columns_names(df):
    df.columns = (
        df.columns.str.strip() 
                  .str.lower()
                  .str.replace(r'[^\w\s]', '', regex=True)
                  .str.replace(r'\s+', '_', regex=True) 
    )
    return df