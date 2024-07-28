import sqlite3
import pandas as pd
from datetime import datetime

conn = sqlite3.connect('/usr/share/hassio/homeassistant/home-assistant_v2.db')
cursor = conn.cursor()

metadata_ids = [15, 16, 12, 13, 17, 18, 19, 42, 43, 44, 45, 46, 47]

current_time = datetime.now()
current_timestamp = current_time.replace(minute=(current_time.minute // 30) * 30, second=0, microsecond=0)

csv_file = './test.xlsx'

df_existing = pd.read_excel(csv_file, skiprows=2)

new_data = []

for metadata_id in metadata_ids:
    query = f"SELECT * FROM statistics_short_term WHERE metadata_id = {metadata_id}"
    cursor.execute(query)
    rows = cursor.fetchall()

    if rows:
        for row in rows:
            data = {
                "日時": current_timestamp,
                "value": row[7]
            }
            new_data.append(data)

df_new = pd.DataFrame(new_data)

df_combined = pd.concat([df_existing, df_new], ignore_index=True)

with pd.ExcelWriter(csv_file, engine='xlsxwriter') as writer:
    df_combined.to_excel(writer, index=False, startrow=3)

    workbook  = writer.book
    worksheet = writer.sheets['Sheet1']
    
    worksheet.write(0, 0, '稼働日/休日')
    worksheet.write(1, 0, '合計 / ■30分値')
    worksheet.write(2, 0, '■日時')
    headers = ["バンドソーHBA520AU（WH01-1）", "バンドソーHBA420AU（WH01-2）",
               "バンドソーHFA300（WH02）", "コンプレッサー（WH04）", "冷却器（WH10）",
               "切削機(WH11)", "コンプレッサー（WH03）", "ローリングミル",
               "油圧ポンプNo1&No2", "油圧ポンプNo3&No4", "油圧ポンプNo5&No6",
               "ローリングミル（大）", "受電パルス"]
    for col_num, header in enumerate(headers, 1):
        worksheet.write(2, col_num, header)

conn.close()
