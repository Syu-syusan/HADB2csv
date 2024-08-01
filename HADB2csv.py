import paho.mqtt.client as mqtt
import sqlite3
import csv
from datetime import datetime, timezone, timedelta
import time
import json

# MQTT設定
MQTT_BROKER = "192.168.11.20"
MQTT_PORT = 1883
MQTT_TOPIC = "haudi/hass/h7Me1xrJrxNS-DXDsKmOFg/HADB2csv"

# SQLite3設定
DB_PATH = "./home-assistant_v2.db"

# Meta IDs
META_IDS = [15, 16, 12, 13, 17, 18, 19, 42, 43, 44, 45, 46, 54]
NAME = ["バンドソーHBA520AU（WH01-1）","バンドソーHBA420AU（WH01-2）","バンドソーHFA300（WH02）","コンプレッサー（WH04）","冷却器（WH10）","切削機(WH11)","コンプレッサー（WH03）","ローリングミル","油圧ポンプNo1&No2","油圧ポンプNo3&No4","油圧ポンプNo5&No6","ローリングミル（大）","受電パルス"]

# CSVファイルの設定
CSV_FILE_PATH = "output.csv"

# データベース接続の確立
def connect_database():
    return sqlite3.connect(DB_PATH)

# データベースからデータを取得
def fetch_data(start_ts, end_ts):
    connection = connect_database()
    cursor = connection.cursor()
    query = """
        SELECT created_ts, metadata_id, state 
        FROM statistics 
        WHERE metadata_id IN ({seq}) 
        AND created_ts BETWEEN ? AND ?
    """.format(seq=','.join(['?']*len(META_IDS)))
    
    params = META_IDS + [start_ts, end_ts]
    cursor.execute(query, params)
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data

# UNIXタイムスタンプをJST日時文字列に変換し、秒を丸める
def unix_to_rounded_jst_datetime(ts):
    dt = datetime.fromtimestamp(ts, timezone.utc) + timedelta(hours=9)
    dt = dt.replace(second=0, microsecond=0)  # 秒を丸める
    return dt.strftime('%Y-%m-%d %H:%M:%S')

# CSVファイルにデータを書き込み
def write_to_csv(data):
    # データを整理する
    sorted_data = {}
    for row in data:
        timestamp = unix_to_rounded_jst_datetime(row[0])
        meta_id = row[1]
        state = row[2]
        if timestamp not in sorted_data:
            sorted_data[timestamp] = {}
        sorted_data[timestamp][meta_id] = state

    # CSVに上書きする
    with open(CSV_FILE_PATH, mode='w', newline='') as file:
        writer = csv.writer(file)
        
        # 4行の空行を追加
        for _ in range(3):
            writer.writerow([])

        # ヘッダーを書き込む
        header = ['日時'] + NAME
        writer.writerow(header)
        
        # 各行を書き込む
        for timestamp, values in sorted(sorted_data.items()):
            row = [timestamp] + [values.get(meta_id, 0) for meta_id in META_IDS]
            writer.writerow(row)

# MQTTメッセージのコールバック
def on_message(client, userdata, msg):
    try:
        # 受信したメッセージを表示
        print(f"Received message: {msg.payload.decode()}")

        # 受信したメッセージを修正
        corrected_message = msg.payload.decode().replace('”', '"').replace("’", "'")
        
        # メッセージをJSONに変換
        message = json.loads(corrected_message)
        start_str = message['start']
        end_str = message['end']

        # UTCとして時刻を変換
        start_ts = int(datetime.strptime(start_str, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc).timestamp())
        end_ts = int(datetime.strptime(end_str, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc).timestamp())

        data = fetch_data(start_ts, end_ts)
        write_to_csv(data)
    except json.JSONDecodeError as e:
        print(f"JSON decode error: {e}")
    except Exception as e:
        print(f"Error processing message: {e}")

# MQTTクライアントの設定
client = mqtt.Client()
client.on_message = on_message

client.connect(MQTT_BROKER, MQTT_PORT, 60)
client.subscribe(MQTT_TOPIC, qos=0)

client.loop_forever()