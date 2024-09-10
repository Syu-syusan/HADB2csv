import paho.mqtt.client as mqtt
import sqlite3
import csv
from datetime import datetime, timezone, timedelta
import json
import requests

MQTT_BROKER = "192.168.11.20"
MQTT_PORT = 1883
MQTT_TOPIC = "haudi/hass/h7Me1xrJrxNS-DXDsKmOFg/HADB2csv_v2"

DB_PATH = "./home-assistant_v2_copy.db"
#"/usr/share/hassio/homeassistant/home-assistant_v2.db"

METADATA_IDS = [15, 16, 12, 13, 17, 18, 19, 42, 43, 44, 45, 46, 54]
NAME = ["バンドソーHBA520AU（WH01-1）","バンドソーHBA420AU（WH01-2）","バンドソーHFA300（WH02）","コンプレッサー（WH04）","冷却器（WH10）","切削機(WH11)","コンプレッサー（WH03）","ローリングミル","油圧ポンプNo1&No2","油圧ポンプNo3&No4","油圧ポンプNo5&No6","ローリングミル（大）","受電パルス"]

def connect_database():
    return sqlite3.connect(DB_PATH)

def fetch_data(start_ts, end_ts):
    connection = connect_database()
    cursor = connection.cursor()
    query = """
        SELECT created_ts, metadata_id, state, sum 
        FROM statistics 
        WHERE metadata_id IN ({seq}) 
        AND created_ts BETWEEN ? AND ?
        ORDER BY created_ts ASC
    """.format(seq=','.join(['?']*len(METADATA_IDS)))
    params = METADATA_IDS + [start_ts, end_ts]
    cursor.execute(query, params)
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data

def unix_to_rounded_jst_datetime(ts):
    dt = datetime.fromtimestamp(ts, timezone.utc) + timedelta(hours=9)
    dt = dt.replace(second=0, microsecond=0)
    return dt.strftime('%Y-%m-%d %H:%M:%S')

def calculate_difference(current_value, previous_value):
    return current_value - previous_value

def write_to_csv(data, file_path):
    sorted_data = {}
    previous_values = {}
    total_sums = {meta_id: 0 for meta_id in METADATA_IDS}  # 各列の総計を保持する辞書

    row_index = 0  # 行インデックスを追跡

    for row in data:
        # 出力するタイムスタンプを1時間前にシフト
        timestamp = unix_to_rounded_jst_datetime(row[0] + 5)  # 1時間(3600秒)前にシフトしている部分に65分を追加
        meta_id = row[1]
        current_value = row[3]  # sumの値

        # 1つ前のデータが存在する場合に差分を計算
        previous_value = previous_values.get(meta_id, current_value)
        state = calculate_difference(current_value, previous_value)

        # 5行目以外を総計に加算
        if row_index != 4:  # インデックスは0から始まるので、5行目はインデックス4
            total_sums[meta_id] += state

        # 現在の値を保存して次の差分計算に使用
        previous_values[meta_id] = current_value

        if timestamp not in sorted_data:
            sorted_data[timestamp] = {}
        sorted_data[timestamp][meta_id] = state

        row_index += 1  # 行インデックスをインクリメント

    # CSVファイルに書き込み
    with open(file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['稼働日/休日', '-'])  # リストの結合を修正
        writer.writerow([])
        writer.writerow(['合計 / ■30分値', 'ポイント名'])  # リストの結合を修正
        header = ['■日時'] + NAME
        writer.writerow(header)
        for timestamp, values in sorted(sorted_data.items()):
            row = [timestamp] + [values.get(meta_id, 0) for meta_id in METADATA_IDS]
            writer.writerow(row)
        
        # 総計行を追加
        total_row = ['総計'] + [total_sums[meta_id] for meta_id in METADATA_IDS]
        writer.writerow(total_row)

def shorten_url(url):
    api_url = "https://is.gd/create.php"
    params = {
        "format": "simple",
        "url": url
    }
    response = requests.get(api_url, params=params)
        
    if response.status_code == 200:
        return response.text
    else:
        return None

def remove_5th_row_and_shift(csv_filepath):
    # 一時的にファイルを読み込み、すべての行をリストに格納
    with open(csv_filepath, mode='r', newline='') as file:
        reader = csv.reader(file)
        rows = list(reader)
    
    # 4行目を削除
    if len(rows) >= 5:
        del rows[4]  # 4行目はインデックス3に対応
    
    # 更新された内容を再び同じファイルに書き込む
    with open(csv_filepath, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(rows)

def on_message(client, userdata, msg):
    try:
        print(f"Received message: {msg.payload.decode()}")
        corrected_message = msg.payload.decode().replace('”', '"').replace("’", "'")
        message = json.loads(corrected_message)
        start_str = message['start']
        end_str = message['end']
        start_ts = int(datetime.strptime(start_str, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc).timestamp()) - 1800
        end_ts = int(datetime.strptime(end_str, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc).timestamp()) + (3600 * 25)
        start_ts -= 9 * 3600
        end_ts -= 9 * 3600
        data = fetch_data(start_ts, end_ts)
        
        # ファイル名をMQTTメッセージのタイムスタンプに基づいて作成
        start_date = datetime.strptime(start_str, '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y%m%d')
        end_date = datetime.strptime(end_str, '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%m%d')
        csv_filename = f"output_{start_date}-{end_date}.csv"
        csv_filepath = f"/usr/share/hassio/homeassistant/www/{csv_filename}"
        write_to_csv(data, csv_filepath)

        csv_filepath = f"/usr/share/hassio/homeassistant/www/{csv_filename}"
        remove_5th_row_and_shift(csv_filepath)
        
        # ファイル名をログに出力
        print(f"CSV file created: {csv_filepath}")
        
        long_url = "https://familia-a126c8.haudi.app/local/" + csv_filename
        short_url = shorten_url(long_url)

        # MQTTメッセージを送信してファイル名を更新
        client.publish("haudi/hass/h7Me1xrJrxNS-DXDsKmOFg/latest_csv_filename", short_url)
    except json.JSONDecodeError as e:
        print(f"JSON decode error: {e}")
    except Exception as e:
        print(f"Error processing message: {e}")

client = mqtt.Client()
client.on_message = on_message

client.connect(MQTT_BROKER, MQTT_PORT, 60)
client.subscribe(MQTT_TOPIC, qos=0)

client.loop_forever()
