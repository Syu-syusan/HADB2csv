import paho.mqtt.client as mqtt
import sqlite3
import csv
from datetime import datetime, timezone, timedelta
import time
import json

MQTT_BROKER = "192.168.11.20"
MQTT_PORT = 1883
MQTT_TOPIC = "haudi/hass/h7Me1xrJrxNS-DXDsKmOFg/HADB2csv"

DB_PATH = "/usr/share/hassio/homeassistant/home-assistant_v2.db"

METADATA_IDS = [15, 16, 12, 13, 17, 18, 19, 42, 43, 44, 45, 46, 54]
NAME = ["バンドソーHBA520AU（WH01-1）","バンドソーHBA420AU（WH01-2）","バンドソーHFA300（WH02）","コンプレッサー（WH04）","冷却器（WH10）","切削機(WH11)","コンプレッサー（WH03）","ローリングミル","油圧ポンプNo1&No2","油圧ポンプNo3&No4","油圧ポンプNo5&No6","ローリングミル（大）","受電パルス"]

def connect_database():
    return sqlite3.connect(DB_PATH)

def fetch_data(start_ts, end_ts):
    connection = connect_database()
    cursor = connection.cursor()
    query = """
        SELECT created_ts, metadata_id, state 
        FROM statistics 
        WHERE metadata_id IN ({seq}) 
        AND created_ts BETWEEN ? AND ?
    """.format(seq=','.join(['?']*len(METADATA_IDS)))
    params = METADATA_IDS + [start_ts, end_ts]
    cursor.execute(query, params)
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data

def unix_to_rounded_jst_datetime(ts):
    # データベースのタイムスタンプがUTCであることを前提とし、JSTに変換
    dt = datetime.fromtimestamp(ts, timezone.utc) + timedelta(hours=9)
    dt = dt.replace(second=0, microsecond=0)  # 秒を丸める
    return dt.strftime('%Y-%m-%d %H:%M:%S')

def write_to_csv(data, file_path):
    sorted_data = {}
    for row in data:
        timestamp = unix_to_rounded_jst_datetime(row[0])
        meta_id = row[1]
        state = row[2]
        if timestamp not in sorted_data:
            sorted_data[timestamp] = {}
        sorted_data[timestamp][meta_id] = state

    with open(file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        for _ in range(3):
            writer.writerow([])
        header = ['日時'] + NAME
        writer.writerow(header)
        for timestamp, values in sorted(sorted_data.items()):
            row = [timestamp] + [values.get(meta_id, 0) for meta_id in METADATA_IDS]
            writer.writerow(row)

def on_message(client, userdata, msg):
    try:
        print(f"Received message: {msg.payload.decode()}")
        corrected_message = msg.payload.decode().replace('”', '"').replace("’", "'")
        message = json.loads(corrected_message)
        start_str = message['start']
        end_str = message['end']
        start_ts = int(datetime.strptime(start_str, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc).timestamp())
        end_ts = int(datetime.strptime(end_str, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc).timestamp())
        start_ts -= 9 * 3600
        end_ts -= 9 * 3600
        data = fetch_data(start_ts, end_ts)
        
        # ファイル名をMQTTメッセージのタイムスタンプに基づいて作成
        start_date = datetime.strptime(start_str, '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y%m%d')
        end_date = datetime.strptime(end_str, '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%m%d')
        csv_filename = f"output_{start_date}-{end_date}.csv"
        csv_filepath = f"/usr/share/hassio/homeassistant/www/{csv_filename}"
        write_to_csv(data, csv_filepath)
        
        # ファイル名をログに出力
        print(f"CSV file created: {csv_filepath}")
        
        # MQTTメッセージを送信してファイル名を更新
        client.publish("haudi/hass/h7Me1xrJrxNS-DXDsKmOFg/latest_csv_filename", "/local"+csv_filename)
        
    except json.JSONDecodeError as e:
        print(f"JSON decode error: {e}")
    except Exception as e:
        print(f"Error processing message: {e}")

client = mqtt.Client()
client.on_message = on_message

client.connect(MQTT_BROKER, MQTT_PORT, 60)
client.subscribe(MQTT_TOPIC, qos=0)

client.loop_forever()
