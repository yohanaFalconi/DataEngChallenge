import pandas as pd
import re

# Escribe cada cabecera y valida que la columna exista
def validate_and_complete_headers(df, expected_columns, log_path):
    df.columns = [expected_columns]
    with open(log_path, "a") as log:
        for col in expected_columns:
            if col not in df.columns:
                log.write(f"ERROR: Missing column: {col}\n")
    return df

# Cambia el tipo de date a ISO 8601 como string 
def format_datetime(df, datetime_col, log_path):
    with open(log_path, "a") as log:
        try:
            df[datetime_col] = pd.to_datetime(df[datetime_col], errors='coerce')
            df[datetime_col] = df[datetime_col].dt.strftime('%Y-%m-%dT%H:%M:%S')
        except Exception as e:
            log.write(f"DATETIME ERROR: Failed to convert '{datetime_col}': {e}\n")
    return df

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



''''''''''''''''''''
# Reemplazar comillas
def escape_quotes(value):
    if isinstance(value, str):
        # Usar repr para obtener una representación válida de la cadena
        return repr(value)  # Esto escapa las comillas automáticamente
    return str(value)

# Convierte un None a su representación en SQL
def convert_to_sql_value(value):
    if value is None:
        return "NULL"
    return escape_quotes(value)

# def prepare_values_for_insert(df):
#     values = []
#     for index, row in df.iterrows():
#         row_values = [f"'{str(value)}'" if isinstance(value, str) else str(value) for value in row]
#         values.append(f"({', '.join(row_values)})")
#     return values


def prepare_values_for_insert(df):
    values = []
    for index, row in df.iterrows():
        row_values = [f"'{str(value)}'" if isinstance(value, str) else str(value) for value in row]
        values.append(f"({', '.join(row_values)})")
    return values