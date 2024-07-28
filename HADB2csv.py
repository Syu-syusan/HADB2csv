import sqlite3
import pandas as pd
from datetime import datetime
import openpyxl

conn = sqlite3.connect('/usr/share/hassio/homeassistant/home-assistant_v2.db')
cursor = conn.cursor()

metadata_ids = [15, 16, 12, 13, 17, 18, 19, 42, 43, 44, 45, 46, 47]

current_time = datetime.now()
current_timestamp = current_time.replace(minute=(current_time.minute // 30) * 30, second=0, microsecond=0)

csv_file = './test.xlsx'

workbook = openpyxl.load_workbook(csv_file)
sheet = workbook.active

last_row = sheet.max_row

new_data = []

for metadata_id in metadata_ids:
    query = f"SELECT * FROM statistics_short_term WHERE metadata_id = {metadata_id} limit 1"
    cursor.execute(query)
    rows = cursor.fetchall()

    if rows:
        for row in rows:
            data = {
                "日時": current_timestamp,
                "電力積算値": row[7]
            }
            new_data.append(data)

conn.close()

print(new_data)
sheet.cell(row=5, column=1, value=new_data[0]['日時'])
for index, data in enumerate(new_data):
    row_num = 1 + index
    sheet.cell(row=5, column=row_num, value=data['電力積算値'])

workbook.save(csv_file)
