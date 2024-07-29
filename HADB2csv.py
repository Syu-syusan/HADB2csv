import sqlite3
from datetime import datetime
import openpyxl

conn = sqlite3.connect('/usr/share/hassio/homeassistant/home-assistant_v2.db')
cursor = conn.cursor()

metadata_ids = [15, 16, 12, 13, 17, 18, 19, 42, 43, 44, 45, 46, 54]

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

sheet.cell(row=5, column=1, value=new_data[1]['日時'])

row_num = 1

sheet.cell(row=5, column=row_num + 1, value=new_data[0]['電力積算値'])
sheet.cell(row=5, column=row_num + 2, value=new_data[1]['電力積算値'])
sheet.cell(row=5, column=row_num + 3, value=new_data[2]['電力積算値'])
sheet.cell(row=5, column=row_num + 4, value=new_data[3]['電力積算値'])
sheet.cell(row=5, column=row_num + 5, value=new_data[4]['電力積算値'])
sheet.cell(row=5, column=row_num + 6, value=new_data[5]['電力積算値'])
sheet.cell(row=5, column=row_num + 7, value=new_data[6]['電力積算値'])
sheet.cell(row=5, column=row_num + 8, value=new_data[7]['電力積算値'])
sheet.cell(row=5, column=row_num + 9, value=new_data[8]['電力積算値'])
sheet.cell(row=5, column=row_num + 10, value=new_data[9]['電力積算値'])
sheet.cell(row=5, column=row_num + 11, value=new_data[10]['電力積算値'])
sheet.cell(row=5, column=row_num + 12, value=new_data[11]['電力積算値'])
sheet.cell(row=5, column=row_num + 13, value=new_data[12]['電力積算値'])

workbook.save(csv_file)
