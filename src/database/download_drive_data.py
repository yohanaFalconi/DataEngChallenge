import pandas as pd
import gdown
import os

file_id = "11xzthHdCJrrb-JfgtsgzntW_T5m4m3cv"
output_file = "data.csv"

url = f"https://drive.google.com/uc?id={file_id}"

gdown.download(url, output_file, quiet=False)

if os.path.exists(output_file):
    print("Descarga correcta")

    df = pd.read_csv(output_file)
    print(df.head())
else:
    print("Error")
