import sqlite3

# ソースデータベースのパスとターゲットデータベースのパス
source_db_path = '/usr/share/hassio/homeassistant/home-assistant_v2.db'
target_db_path = '/home/fc10081001/Documents/HADB2csv/home-assistant_v2_copy.db'

# ソースから最新のデータを取得するSQLクエリ
source_query = "SELECT * FROM statistics_short_term WHERE metadata_id = 18 ORDER BY id DESC limit 1"

# ターゲットから最大IDを取得するSQLクエリ
target_query_max_id = "SELECT MAX(id) FROM statistics"

# ターゲットにデータを挿入するためのSQLクエリ（13カラム）
insert_query = """
INSERT INTO statistics (id, created, start, mean, min, max, last_reset, state, sum, metadata_id, created_ts, start_ts, last_reset_ts)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

try:
    # ソースデータベースに接続してデータ取得
    source_conn = sqlite3.connect(source_db_path)
    source_cursor = source_conn.cursor()
    source_cursor.execute(source_query)
    source_data = source_cursor.fetchone()  # 最新のデータを取得

    if not source_data:
        raise Exception("No data found for metadata_id = 18 in statistics_short_term.")

    # ターゲットデータベースに接続して最大IDを取得
    target_conn = sqlite3.connect(target_db_path)
    target_cursor = target_conn.cursor()
    target_cursor.execute(target_query_max_id)
    max_id = target_cursor.fetchone()[0] or 0  # 最大IDがなければ0を使用

    # 新しいIDを生成
    new_id = max_id + 1

    # source_dataからすべての必要なフィールドを抽出（idを除く）
    insert_data = (
        new_id,  # id
        source_data[1],  # created
        source_data[2],  # start
        source_data[3],  # mean
        source_data[4],  # min
        source_data[5],  # max
        source_data[6],  # last_reset
        source_data[7],  # state
        source_data[8],  # sum
        source_data[9],  # metadata_id
        source_data[10],  # created_ts
        source_data[11],  # start_ts
        source_data[12]  # last_reset_ts
    )

    # データをターゲットデータベースに挿入
    target_cursor.execute(insert_query, insert_data)
    target_conn.commit()

    print(f"Inserted data with new ID {new_id}")

except sqlite3.Error as e:
    print(f"SQLite error: {e}")
except Exception as e:
    print(f"Error: {e}")
finally:
    # すべての接続を閉じる
    if source_cursor:
        source_cursor.close()
    if source_conn:
        source_conn.close()
    if target_cursor:
        target_cursor.close()
    if target_conn:
        target_conn.close()

