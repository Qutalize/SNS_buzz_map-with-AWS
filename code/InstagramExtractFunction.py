import boto3
import json
import os
from datetime import datetime, timezone
from google import genai
from google.genai import types
import random
import time
from botocore.exceptions import ClientError # Boto3のエラー処理用

# --- 設定情報 (環境変数から読み込む) ---
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY") 
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
TARGET_DYNAMODB_TABLE_NAME = os.environ.get("TARGET_DYNAMODB_TABLE_NAME") # SemifinalDB (FinalDBの前のDB)
TARGET_DB_REGION = os.environ.get("TARGET_DB_REGION", os.environ.get("AWS_REGION")) # ターゲットDBが存在するリージョン

# --- クライアントの初期化 ---
gemini_client = None
ddb_table = None

if GEMINI_API_KEY and TARGET_DYNAMODB_TABLE_NAME and TARGET_DB_REGION:
    try:
        gemini_client = genai.Client(api_key=GEMINI_API_KEY)
        
        ddb_resource = boto3.resource('dynamodb', region_name=TARGET_DB_REGION)
        ddb_table = ddb_resource.Table(TARGET_DYNAMODB_TABLE_NAME)
        
        print(f"クライアント初期化完了。ターゲットDB: {TARGET_DYNAMODB_TABLE_NAME} ({TARGET_DB_REGION})")
    except Exception as e:
        print(f"初期化エラー: {e}")
        ddb_table = None
else:
    missing_vars = []
    if not GEMINI_API_KEY: missing_vars.append("GEMINI_API_KEY")
    if not TARGET_DYNAMODB_TABLE_NAME: missing_vars.append("TARGET_DYNAMODB_TABLE_NAME")
    if not TARGET_DB_REGION: missing_vars.append("TARGET_DB_REGION")
    print(f"環境変数が不足しています: {', '.join(missing_vars)}。初期化スキップ。")


# 1. Gemini API呼び出し関数 (バックオフ処理付き)
MAX_RETRIES = 5
INITIAL_BACKOFF_TIME = 10

def call_gemini_for_extraction(caption_text):
    """Gemini APIを呼び出し、JSON形式で店舗情報を抽出する (バックオフ処理付き)"""
    if not gemini_client:
        return None
    
    system_instruction = (
        "あなたは、Instagramのグルメキャプションから店舗情報をJSON形式で抽出する専門家です。"
        "抽出する項目は、placeName (店名) と address (住所) の2つです。"
        "情報がない場合は、値をnullとしてください。回答はJSONオブジェクトのみとし、前後の説明は不要です。"
        "JSONスキーマ: {\"placeName\": \"...\", \"address\": \"...\"}"
    )
    
    for attempt in range(MAX_RETRIES):
        try:
            response = gemini_client.models.generate_content(
                model=GEMINI_MODEL,
                contents=f"TEXT: {caption_text}",
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
                print(f"Rate Limit。{wait_time:.2f}秒待機して再試行します...")
                time.sleep(wait_time)
            else:
                print(f"Gemini API呼び出し/パースエラー: {e}")
                return None
    
    return None

# 2. DynamoDB Streams形式のデータからPython辞書に変換するヘルパー関数
def unmarshal_dynamodb_json(dynamodb_json):
    """
    DynamoDB Streams形式のJSON (例: {"S": "value", "N": "123"}) を
    通常のPython辞書形式に変換する
    """
    data = {}
    for key, value in dynamodb_json.items():
        if 'S' in value:
            data[key] = value['S']
        elif 'N' in value:
            data[key] = int(value['N']) # 数値型に変換 (like_count)
        elif 'M' in value and value['M']:
            data[key] = unmarshal_dynamodb_json(value['M'])
    return data

# 3. ターゲットDBへの保存関数
def save_to_target_db(original_data, extracted_data):
    """
    抽出結果とオリジナルデータを統合し、SemiFinalDBに格納する
    """
    if not ddb_table:
        return False
        
    try:
        likes = original_data.get("like_count", 0)
        # --- 経過日数の計算 ---
        timestamp_str = original_data.get('timestamp') # 投稿日時
        crawled_at_str = original_data.get('crawled_at') # クロール日時

        if timestamp_str and crawled_at_str:
            # ISO文字列をUTCのdatetimeオブジェクトに変換
            # 'T'と'+'以降の部分を含むISO形式を処理
            dt_crawled = datetime.fromisoformat(crawled_at_str.replace('Z', '+00:00'))
            dt_posted = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            
            time_diff = dt_crawled - dt_posted
            elapsed_days = time_diff.total_seconds() / (24 * 3600.0)
            
            # 経過日数がゼロまたは負にならないよう処理 (ゼロ除算防止)
            if elapsed_days < 0.01:
                 elapsed_days = 0.01 
            
            # --- buzz の計算 ---
            # buzz = likes / elapsed_days / 100 
            buzz_score = likes / elapsed_days / 100.0
            buzz_score = max(1, min(5, buzz_score))
            
        else:
            buzz_score = 1

        item = {
            'postId': original_data['media_id'], 
            'platform': 'Instagram', # 確定
            'url': original_data['permalink'],
            'title': original_data['caption'], 
            
            # Gemini抽出結果
            'placeName': extracted_data.get('placeName', 'N/A'),
            'address': extracted_data.get('address', 'N/A'),
            
            # バズり度 (いいね数 like_count をそのまま利用)
            'buzz': int(buzz_score),
            
            # 取得日時 (LLM処理を終えた日時)
            'fetchedAt': datetime.now(timezone.utc).isoformat()
        }
        
        # DynamoDBに書き込み (PutItem)
        ddb_table.put_item(Item=item)
        return True
    except ClientError as e:
        print(f"ターゲットDBへの書き込みエラー (ID: {original_data['media_id']}): {e}")
        return False
    except Exception as e:
        print(f"その他の書き込みエラー: {e}")
        return False

# 4. メインハンドラー関数 (DynamoDB Streamsトリガー用)
def lambda_handler(event, context):
    """DynamoDB Streamsイベントを処理するエントリーポイント"""
    if not gemini_client or not ddb_table:
        return {'statusCode': 500, 'body': 'Client or DB initialization failed.'}

    for record in event['Records']:
        # INSERTまたはMODIFYイベントのみを対象とする
        if record['eventName'] == 'INSERT' or record['eventName'] == 'MODIFY':
            new_image = record['dynamodb']['NewImage']
            # DynamoDBの形式からPython辞書に変換
            original_data = unmarshal_dynamodb_json(new_image)
            
            media_id = original_data.get('media_id')
            caption_text = original_data.get('caption')

            if media_id and caption_text:
                
                # 抽出処理
                extracted_data = call_gemini_for_extraction(caption_text)
                
                if extracted_data:
                    # ターゲットDB (SemifinalDB) への保存
                    if save_to_target_db(original_data, extracted_data):
                        print(f"SemifinalDBに抽出結果を保存完了: {media_id}")
                else:
                    print(f"抽出失敗またはGeminiから結果なし: {media_id}")
            
    return {'statusCode': 200, 'body': 'Extraction pipeline finished successfully.'}
