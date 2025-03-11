from fastapi.responses import HTMLResponse
from src.data_analysis.data_analysis import (get_hires_by_quarter, get_departments_above_avg_hires) 


def hires_by_quarter_html(df):
    try:
        df_result = get_hires_by_quarter(df)
        html_table = df_result.to_html(index=False, border=1, justify="center", classes="dataframe")
        html_content = f"""
        <html>
            <head>
                <title>Hires by Quarter</title>
                <style>
                    body {{
                        font-family: Arial, sans-serif;
                        padding: 40px;
                        background-color: #f9f9f9;
                    }}
                    table.dataframe {{
                        width: 80%;
                        margin: auto;
                        border-collapse: collapse;
                        border: 1px solid #ccc;
                    }}
                    table.dataframe th, table.dataframe td {{
                        border: 1px solid #ccc;
                        padding: 8px 12px;
                        text-align: center;
                    }}
                    table.dataframe th {{
                        background-color: #eee;
                    }}
                </style>
            </head>
            <body>
                <h2 style="text-align:center;">Hires by Quarter</h2>
                {html_table}
            </body>
        </html>
        """
        return HTMLResponse(content=html_content)
    except Exception as e:
        return HTMLResponse(content=f"<h3>Error: {e}</h3>", status_code=500)
    

def departments_above_average_html(df):
    try:
        df_result = get_departments_above_avg_hires(df)
        html_table = df_result.to_html(index=False, border=1, justify="center", classes="dataframe")

        html_content = f"""
        <html>
            <head>
                <title>Departments Above Average Hires</title>
                <style>
                    body {{
                        font-family: Arial, sans-serif;
                        padding: 40px;
                        background-color: #f9f9f9;
                    }}
                    table.dataframe {{
                        width: 60%;
                        margin: auto;
                        border-collapse: collapse;
                        border: 1px solid #ccc;
                    }}
                    table.dataframe th, table.dataframe td {{
                        border: 1px solid #ccc;
                        padding: 8px 12px;
                        text-align: center;
                    }}
                    table.dataframe th {{
                        background-color: #eee;
                    }}
                </style>
            </head>
            <body>
                <h2 style="text-align:center;">Departments Above Average Hires</h2>
                {html_table}
            </body>
        </html>
        """
        return HTMLResponse(content=html_content)
    except Exception as e:
        return HTMLResponse(content=f"<h3>Error: {e}</h3>", status_code=500)