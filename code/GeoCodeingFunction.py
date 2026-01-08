import boto3
import json
import os
from datetime import datetime, timezone
from decimal import Decimal
from botocore.exceptions import ClientError
import random

# --- 設定情報 (環境変数から読み込む) ---
TARGET_DYNAMODB_TABLE_NAME = os.environ['TARGET_DYNAMODB_TABLE_NAME'] 
PLACE_INDEX_NAME = os.environ['PLACE_INDEX_NAME'] 
# ターゲットDBが存在するリージョンを環境変数から取得
LOCATION_REGION = os.environ.get("LOCATION_REGION", os.environ.get("AWS_REGION", "ap-northeast-1")) 

# --- クライアントの初期化 ---
try:
    # DynamoDBクライアントは、TARGET_DB_REGIONで初期化
    DDB_TARGET_TABLE = boto3.resource('dynamodb', region_name=LOCATION_REGION).Table(TARGET_DYNAMODB_TABLE_NAME)
    # Location Clientも同じリージョンで初期化
    LOCATION_CLIENT = boto3.client('location', region_name=LOCATION_REGION)
    print(f"クライアント初期化完了。ターゲットDB: {TARGET_DYNAMODB_TABLE_NAME} ({LOCATION_REGION})")
except Exception as e:
    print(f"初期化エラーが発生しました: {e}")
    DDB_TARGET_TABLE = None
    LOCATION_CLIENT = None

# 1. DynamoDB Streams形式のデータからPython辞書に変換するヘルパー関数
def unmarshal_dynamodb_json(dynamodb_json):
    """DynamoDB Streams形式のJSONを通常のPython辞書形式に変換する"""
    data = {}
    for key, value in dynamodb_json.items():
        if 'S' in value:
            data[key] = value['S']
        elif 'N' in value:
            # 数値型は整数または浮動小数点数に変換
            data[key] = int(value['N']) if '.' not in value['N'] else float(value['N'])
        elif 'M' in value and value['M']:
            data[key] = unmarshal_dynamodb_json(value['M'])
    return data

# 2. Amazon Location Service 呼び出し関数 (ジオコーディング)
def geocode_address(address):
    """住所文字列から緯度経度を取得する"""
    if not LOCATION_CLIENT or not address or address in ['N/A', 'null'] or not PLACE_INDEX_NAME:
        return None
    
    try:
        response = LOCATION_CLIENT.search_place_index_for_text(
            IndexName=PLACE_INDEX_NAME, 
            Text=address,
            FilterCountries=['JPN'], 
            MaxResults=1
        )
        
        if response and response['Results']:
            point = response['Results'][0]['Place']['Geometry']['Point']
            # Location Serviceは通常 [経度 (Lng), 緯度 (Lat)] の順
            return {
                "lng": point[0], 
                "lat": point[1]
            }
        else:
            return None

    except ClientError as e:
        print(f"ジオコーディングエラー (アクセス拒否/無効なインデックス): {e}")
        return None
    except Exception as e:
        print(f"その他のジオコーディングエラー ({address}): {e}")
        return None

# 3. ターゲットDBへの保存関数
def save_to_final_db(data, coords):
    """
    データと緯度経度情報を統合し、新しいターゲットDBに格納する
    """
    if not DDB_TARGET_TABLE:
        return False
        
    try:
        # FinalDB形式にマッピング
        item = {
            # SemifinalDBから流れてきたデータをそのまま使用
            'postId': data['postId'], 
            'platform': data['platform'],
            'url': data['url'],
            'title': data['title'], 
            'fetchedAt': data['fetchedAt'],
            'placeName': data.get('placeName', 'N/A'),
            'address': data.get('address', 'N/A'),
            
            # ジオコーディング結果 (Decimal型に変換)
            'lat': Decimal(str(coords.get('lat', 0.0))),
            'lng': Decimal(str(coords.get('lng', 0.0))),
            
            # バズり度
            'buzz': data.get('buzz', 0)
        }
        
        DDB_TARGET_TABLE.put_item(Item=item)
        return True
    except ClientError as e:
        print(f"最終DB PutItemエラー: {e}")
        return False
    except Exception as e:
        # Floatエラー回避のため Decimal(str(...)) を使用しましたが、ここで発生する例外をキャッチ
        print(f"その他のDB書き込みエラー: {e}")
        return False

# 4. メインハンドラー関数 (DynamoDB Streamsトリガー用)
def lambda_handler(event, context):
    """DynamoDB Streamsイベントを処理するエントリーポイント"""
    if not DDB_TARGET_TABLE or not LOCATION_CLIENT:
        return {'statusCode': 500, 'body': 'AWS clients failed to initialize.'}
    success_count = 0
    for record in event['Records']:
        if record['eventName'] == 'INSERT' or record['eventName'] == 'MODIFY':
            
            new_image = record['dynamodb']['NewImage']
            original_data = unmarshal_dynamodb_json(new_image)
            
            # postId と address を取得 (GeoCodingFunctionはSemifinalDBのデータ形式に依存)
            record_id = original_data.get('postId') 
            address_to_geocode = original_data.get('address') 

            if record_id and address_to_geocode and address_to_geocode not in ['N/A', 'null', None]:
                
                # ジオコーディングの実行
                coordinates = geocode_address(address_to_geocode)
                
                if coordinates:
                    # 最終DBへ保存
                    if save_to_final_db(original_data, coordinates):
                        success_count += 1
                        print(f"ID: {record_id} - ジオコーディング成功し、FinalDBに保存。")
                else:
                    print(f"ID: {record_id} - ジオコーディング失敗/結果なし。スキップします。")
            else:
                print(f"ID: {record_id} - 住所情報がないためスキップします。")
            
    return {'statusCode': 200, 'body': f'Geocoding and saving complete. Success count: {success_count}'}
