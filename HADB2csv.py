import sqlite3
from datetime import datetime, timedelta
import openpyxl
import time

conn = sqlite3.connect('/usr/share/hassio/homeassistant/home-assistant_v2.db')
cursor = conn.cursor()

metadata_ids = [15, 16, 12, 13, 17, 18, 19, 42, 43, 44, 45, 46, 54]

csv_file = './test.xlsx'

workbook = openpyxl.load_workbook(csv_file)
sheet = workbook.active

last_row = sheet.max_row

new_data = []

row_num = 5

def main():
    global row_num
    new_data.clear()
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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

    if len(new_data) == len(metadata_ids):
        sheet.cell(row=row_num+1, column=1, value=new_data[1]['日時'])
        for i in range(len(new_data)):
            sheet.cell(row=5, column=row_num + 1 + i, value=new_data[i]['電力積算値'])

        workbook.save(csv_file)
        row_num += 1

if __name__ == '__main__':
    try:
        while True:
            now = datetime.now()
            if now.minute == 0 or now.minute == 30:
                main()
            next_minute = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
            sleep_time = (next_minute - now).total_seconds()
            time.sleep(sleep_time)
    except KeyboardInterrupt:
        conn.close()
        workbook.save(csv_file)
