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

# Excelファイルを読み込む
workbook = openpyxl.load_workbook(csv_file)
sheet = workbook.active

# 既存のExcelファイルの最後の行を取得
last_row = sheet.max_row

# 新規データのリスト
new_data = []

# 各metadata_idごとにデータを取得し、リストに追加
for metadata_id in metadata_ids:
    query = f"SELECT * FROM statistics_short_term WHERE metadata_id = {metadata_id} limit 1"
    cursor.execute(query)
    rows = cursor.fetchall()

    # データが存在する場合のみ処理
    if rows:
        for row in rows:
            # 8個目のカラムのデータを辞書形式に変換
            data = {
                "日時": current_timestamp,
                "電力積算値": row[7]
            }
            new_data.append(data)

# SQLite接続を閉じる
conn.close()

# 既存のデータフレームに新規データを追加
for index, data in enumerate(new_data):
    row_num = last_row + 1 + index
    sheet.cell(row=row_num, column=2, value=data['日時'])  # B列に日時
    sheet.cell(row=row_num, column=3, value=data['電力積算値'])  # C列に電力積算値
    # 必要に応じて他の列にもデータを追加

# 新しいデータを含むExcelファイルを書き込む
workbook.save(csv_file)
