from flask import Flask, render_template
import requests as rq
import pandas as pd
from io import StringIO
import os
import time
import sys
sys.stdout.reconfigure(encoding='utf-8')


app = Flask(__name__)

marker_folder = os.path.join(os.getcwd(), 'marker')  
app.config['marker_folder'] = marker_folder

path = "250308"
api_key = "gwvhtbcs7dujznyxg7bsmu3ypx4gv73dcxsjfsdg26rzy808zty78cbtuu1c9wer"
url_base = "https://selfserve.decipherinc.com/api/v1/surveys/selfserve/563/"
quota_url = f"{url_base}{path}/quota"

marker_file_path = os.path.join(marker_folder, "marker.xlsx")

last_modified_time = 0
df_marker = None  

def load_marker_file():
    """Load marker.xlsx only if it is updated"""
    global last_modified_time, df_marker
    if os.path.exists(marker_file_path):
        modified_time = os.path.getmtime(marker_file_path) 
        if modified_time > last_modified_time:  
            last_modified_time = modified_time
            df_marker = pd.read_excel(marker_file_path)  
            print("🔄 Marker file updated, reloading data...")
    else:
        print("Error: marker.xlsx file not found!")
        df_marker = None

@app.route('/')
def index():
    global df_marker

    quota_req = rq.get(quota_url, headers={"x-apikey": api_key})
    if quota_req.status_code != 200:
        return f"Failed to fetch quota data. Status Code: {quota_req.status_code}, Response: {quota_req.text}"
    
    response = quota_req.json()

    response_data = rq.get(f"{url_base}{path}/data", headers={"x-apikey": api_key})
    response_data.raise_for_status()
    
    content = response_data.text
    data = pd.read_csv(StringIO(content), sep="\t")

    quota_data = []
    for quota in response.get('sheets', []):
        for sheet in response['sheets'][quota]:
            for cell in sheet.get('cells', []):
                marker = cell.get('marker')
                quota_values = response.get('markers', {}).get(marker, {})

                limit = quota_values.get('limit', 0)
                complete = quota_values.get('complete', 0)
                needs = limit - complete if limit is not None and complete is not None else None

                quota_data.append({
                    'marker': marker,
                    'limit': limit,
                    'complete': complete,
                    'needs': needs
                })

    df_quota = pd.DataFrame(quota_data)

    load_marker_file()
    
    if df_marker is None:
        return "Error: marker.xlsx file not found or not loaded!"
    # df_marker = df_marker.dropna(axis=0)

    df_final = df_marker.merge(df_quota, on="marker", how="left")
    # print(df_marker)
    # df_final.insert(0, "Quotasheet", "")
    df_final.insert(1, "QuotaBuckets", df_final["Quota"])

    # df_final.loc[df_final.index[0], "Quotasheet"] = "Quota"

    df_final.drop(columns=["Quota", "marker"], inplace=True)

    table_html = df_final.to_html(classes='table table-striped table-bordered', index=False)

    return render_template("index.html", table=table_html)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))  # Use Render-assigned port
    app.run(host='0.0.0.0', port=port, debug=False)
