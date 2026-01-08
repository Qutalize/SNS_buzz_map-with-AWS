import json
import os
import time
import random
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError
from google import genai
from google.genai import types

# --- 設定情報 (環境変数から読み込む) ---
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY") 
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
TARGET_DYNAMODB_TABLE_NAME = os.environ.get("TARGET_DYNAMODB_TABLE_NAME", "SemiFinalDB") 
TARGET_DB_REGION = os.environ.get("TARGET_DB_REGION", os.environ.get("AWS_REGION", "us-east-1"))

# --- クライアントの初期化 ---
GEMINI_CLIENT = None
DDB_TABLE = None

try:
    if GEMINI_API_KEY:
        GEMINI_CLIENT = genai.Client(api_key=GEMINI_API_KEY)
    
    # ターゲットDB (SemiFinalDB) への書き込みクライアントを初期化
    DDB_TABLE = boto3.resource('dynamodb', region_name=TARGET_DB_REGION).Table(TARGET_DYNAMODB_TABLE_NAME)

    print(f"クライアント初期化完了。ターゲットDB: {TARGET_DYNAMODB_TABLE_NAME} ({TARGET_DB_REGION})")
except Exception as e:
    print(f"初期化エラー: {e}")
  
# === 1. ヘルパー関数 ===
def unmarshal_dynamodb_json(dynamodb_json):
    """DynamoDB Streams形式のJSONを通常のPython辞書形式に変換する"""
    data = {}
    for key, value in dynamodb_json.items():
        if 'S' in value:
            data[key] = value['S']
        elif 'N' in value:
            data[key] = int(value['N']) if '.' not in value['N'] else float(value['N'])
        elif 'M' in value and value['M']:
            data[key] = unmarshal_dynamodb_json(value['M'])
    return data

def call_gemini_for_extraction(text_to_analyze):
    """Gemini APIを呼び出し、JSON形式で店舗情報を抽出する (バックオフ処理付き)"""
    if not GEMINI_CLIENT:
        return None
    
    system_instruction = (
        "あなたは、YouTubeのグルメ動画のタイトルと説明文から店舗情報をJSON形式で抽出する専門家です。"
        "抽出する項目は、placeName (店名) と address (住所) の2つです。"
        "情報がない場合は、値をnullとしてください。回答はJSONオブジェクトのみとし、前後の説明は不要です。"
        "JSONスキーマ: {\"placeName\": \"...\", \"address\": \"...\"}"
    )
    
    MAX_RETRIES = 5
    INITIAL_BACKOFF_TIME = 10

    for attempt in range(MAX_RETRIES):
        try:
            response = GEMINI_CLIENT.models.generate_content(
                model=GEMINI_MODEL,
                contents=f"TEXT: {text_to_analyze}",
                config=types.GenerateContentConfig(
                    system_instruction=system_instruction,
                    response_mime_type="application/json",
                    temperature=0.1
                )
            )
            llm_output_text = response.text.strip()
            return json.loads(llm_output_text)
            
        except Exception as e:
            if "TooManyRequests" in str(e) or (hasattr(e, 'response') and getattr(e.response, 'status_code', 0) == 429):
                wait_time = INITIAL_BACKOFF_TIME * (2 ** attempt) + random.uniform(0, 1)
                print(f"⚠️ Gemini Rate Limit。{wait_time:.2f}秒待機して再試行します...")
                time.sleep(wait_time)
            else:
                print(f"❌ Gemini API呼び出し/パースエラー: {e}")
                return None
    
    return None

# 2. SemiFinalDB 格納ロジック
def save_to_semifinal_db(original_data, extracted_data):
    """抽出結果をSemiFinalDBの形式にマッピングして保存する"""
    if not DDB_TABLE: return False
    
    try:
        views = original_data.get("views", 0)
        subscribers = original_data.get("subscriber_count", 1)

        # --- 経過時間の計算 ---
        published_at_str = original_data.get('published_at')
        crawled_at_str = original_data.get('crawled_at')
        if published_at_str and crawled_at_str and subscribers > 0:
            # ISO文字列をUTCのdatetimeオブジェクトに変換
            dt_crawled = datetime.fromisoformat(crawled_at_str.replace('Z', '+00:00'))
            dt_published = datetime.fromisoformat(published_at_str.replace('Z', '+00:00'))
            
            time_diff = dt_crawled - dt_published
            elapsed_hours = time_diff.total_seconds() / 3600.0
            
            # 公開直後 (1時間未満) の場合、ゼロ除算を防ぐため最小値を設定
            if elapsed_hours < 1.0:
                 elapsed_hours = 1.0 
            
            # --- buzz の計算 ---
            # buzz = 100 * views / {subscribers * elapsed_hours}
            buzz_score = (100.0 * views) / (subscribers * elapsed_hours)
            buzz_score = max(1, min(5, buzz_score))
            
        else:
            buzz_score = 1
        
        # SemiFinalDB形式にマッピング
        item = {
            'postId': original_data['videoId'], 
            'platform': 'Youtube',
            'url': original_data['url'],
            'title': original_data['title'], 
            'placeName': extracted_data.get('placeName', 'N/A'),
            'address': extracted_data.get('address', 'N/A'),
            'buzz': int(buzz_score), # viewsとlikesの合計をbuzzとする
            'fetchedAt': original_data['crawled_at'] # クロールされた日時を使用
        }
        
        # DynamoDBに書き込み (SemiFinalDBに書き込まれると、後続のGeoCodingFunctionがトリガーされる)
        DDB_TABLE.put_item(Item=item)
        return True
    except ClientError as e:
        print(f"DynamoDB書き込みエラー (ID: {original_data['videoId']}): {e}")
    except Exception as e:
        print(f"その他のDBエラー: {e}")
        return False
      
# === 3. AWS Lambda ハンドラ (Streams Trigger) ===
def lambda_handler(event, context):
    """
    YouTubeDB Streamsからのイベントを処理し、Gemini抽出とSemiFinalDBへの格納を実行する
    """
    if not DDB_TABLE or not GEMINI_CLIENT:
        return {'statusCode': 500, 'body': 'Clients failed to initialize.'}

    saved_count = 0
    for record in event['Records']:
        if record['eventName'] == 'INSERT' or record['eventName'] == 'MODIFY':
            new_image = record['dynamodb']['NewImage']
            original_data = unmarshal_dynamodb_json(new_image)
            
            # 抽出に使用するテキスト (タイトルと説明を結合)
            extraction_text = f"Title: {original_data.get('title', '')}\nDescription: {original_data.get('description', '')}"

            # Gemini 抽出
            extracted_data = call_gemini_for_extraction(extraction_text)
            
            if extracted_data:
                if save_to_semifinal_db(original_data, extracted_data):
                    saved_count += 1
            
    return {
        'statusCode': 200,
        'body': json.dumps(f"YouTube extraction successful. Saved {saved_count} items to SemiFinalDB.")
    }
