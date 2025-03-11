import pandas as pd
import re
from datetime import datetime
import os
# Escribe cada cabecera y valida que la columna exista
# def validate_and_complete_headers(df, expected_columns, log_path):
#     df.columns = [expected_columns]
#     with open(log_path, "a") as log:
#         for col in expected_columns:
#             if col not in df.columns:
#                 log.write(f"ERROR: Missing column: {col}\n")
#     return df
def validate_and_complete_headers(df, expected_columns, log_path=None):
    try:
        original_columns = list(df.columns)

        # Si la cantidad de columnas no coincide, forzar los nombres esperados
        if len(df.columns) != len(expected_columns):
            with open(log_path, "a") as log:
                log.write("\n--- Header validation ---\n")
                log.write(f"WARNING: Column count mismatch. Expected {len(expected_columns)}, got {len(df.columns)}.\n")
                log.write(f"Original columns: {original_columns}\n")
                log.write(f"Columns set to: {expected_columns}\n")
            df.columns = expected_columns
        else:
            # Verificar si los nombres coinciden
            mismatched = [i for i, col in enumerate(df.columns) if col != expected_columns[i]]
            if mismatched and log_path:
                with open(log_path, "a") as log:
                    log.write("\n--- Header validation ---\n")
                    log.write("WARNING: Column names mismatch.\n")
                    for i in mismatched:
                        log.write(f"Column {i}: '{df.columns[i]}' replaced with '{expected_columns[i]}'\n")
                df.columns = expected_columns

        return df

    except Exception as e:
        print(f"Error while validating headers: {e}")
        return df


# Cambia el tipo de date a ISO 8601 como string 
def format_datetime(df, datetime_col, log_path=None):
    try:
        original_values = df[datetime_col].copy()
        df[datetime_col] = pd.to_datetime(df[datetime_col], errors='coerce')
        failed_conversion = df[df[datetime_col].isna() & original_values.notna()]
        df[datetime_col] = df[datetime_col].dt.strftime('%Y-%m-%dT%H:%M:%S')

        if log_path and not failed_conversion.empty:
            os.makedirs(os.path.dirname(log_path), exist_ok=True)
            with open(log_path, "a", encoding="utf-8") as log:
                log.write("\n--- DATETIME FORMAT WARNINGS ---\n")
                for idx, val in failed_conversion[datetime_col].items():
                    log.write(f"Row {idx}: Could not parse datetime value '{original_values[idx]}'\n")
        return df
    except Exception as e:
        print(f"Error while formatting datetime column '{datetime_col}': {e}")
        return df

#Elimina espacios al inicio y final de todas las columnas tipo string
# def trim_strings(df):
#     for col in df.select_dtypes(include='object').columns:
#         df[col] = df[col].str.strip()
#     return df

def trim_strings(df, log_path=None):
    try:
        modified_entries = []
        for col in df.select_dtypes(include='object').columns:
            for idx, val in df[col].items():
                if isinstance(val, str):
                    trimmed_val = val.strip()
                    if val != trimmed_val:
                        modified_entries.append((idx, col, val, trimmed_val))
                        df.at[idx, col] = trimmed_val

        if modified_entries and log_path:
            with open(log_path, "a") as log:
                log.write("\n--- Trimmed string values ---\n")
                for idx, col, original, trimmed in modified_entries:
                    log.write(f"Row {idx}, Column '{col}': '{original}' -> '{trimmed}'\n")
        return df
    except Exception as e:
        print(f"Error while trimming string values: {e}")
        return df


#Elimina nulos
def drop_rows_with_mostly_nans(df, min_valid=3, log_path=None):
    try:
        invalid_rows = df[df.notna().sum(axis=1) < min_valid]
        if not invalid_rows.empty:
            with open(log_path, "a") as log:
                log.write("\n--- Rows dropped due to excessive NaNs ---\n")
                for idx, row in invalid_rows.iterrows():
                    log.write(f"Index {idx} removed: {row.to_dict()}\n")

        return df[df.notna().sum(axis=1) >= min_valid]

    except Exception as e:
        print(f"Error while dropping rows with too many NaNs: {e}")
        return df

def drop_duplicate_rows(df, log_path=None):
    try:
        duplicated_rows = df[df.duplicated(keep=False)]  # Muestra todas las filas duplicadas, no solo una
        df_cleaned = df.drop_duplicates()

        if not duplicated_rows.empty and log_path:
            os.makedirs(os.path.dirname(log_path), exist_ok=True)
            with open(log_path, "a", encoding="utf-8") as log:
                log.write("\n--- Rows dropped due to duplication ---\n")
                for idx, row in duplicated_rows.iterrows():
                    log.write(f"Index {idx} duplicated: {row.to_dict()}\n")

        return df_cleaned

    except Exception as e:
        print(f"Error while dropping duplicate rows: {e}")
        return df

# Convierte datetime a ISO 8601 como string 
def normalize_datetime_fields(data):
    for row in data:
        for key, value in row.items():
            if isinstance(value, datetime):
                row[key] = value.isoformat()
            elif isinstance(value, str) and len(value) == 15 and 'T' in value:
                try:
                    parsed = datetime.strptime(value, "%Y%m%dT%H%M%S")
                    row[key] = parsed.isoformat()
                except ValueError:
                    pass  
    return data

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

# Prepara los valores de la query
# def prepare_values_for_insert(df):
#     values = []
#     for index, row in df.iterrows():
#         row_values = [f"'{str(value)}'" if isinstance(value, str) else str(value) for value in row]
#         values.append(f"({', '.join(row_values)})")
#     return values

def prepare_values_for_insert(df):
    values = []
    for index, row in df.iterrows():
        row_values = []
        for value in row:
            if pd.isna(value):
                row_values.append("NULL")
            elif isinstance(value, str):
                safe_str = value.replace("'", "\\'")
                row_values.append(f"'{safe_str}'")
            else:
                row_values.append(str(value))
        values.append(f"({', '.join(row_values)})")
    return values

#  Elimina duplicados excepto las especificadas en ignore_columns
def drop_duplicates_ignore_columns(df, ignore_columns=None):
    try:
        if ignore_columns is None:
            ignore_columns = []
        
        cols_to_check = [col for col in df.columns if col not in ignore_columns]
        return df.drop_duplicates(subset=cols_to_check)
    except Exception as e:
        print(f"Error while dropping duplicate rows: {e}")
        return df

# Elmina si hay duplicados en el DataFrame basados en id
# def check_duplicates_by_ids(df, id_columns):
#     duplicates = df[df.duplicated(subset=id_columns, keep=False)]
#     has_duplicates = not duplicates.empty

#     if has_duplicates:
#         print(duplicates)
#     else:
#         print(f"No hay duplicados en las columnas {id_columns}.")

#     return has_duplicates, duplicates