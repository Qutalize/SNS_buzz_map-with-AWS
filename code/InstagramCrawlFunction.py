import requests
from datetime import datetime, timedelta, timezone
import os
import time
import boto3

# --- 設定情報 (環境変数から読み込む) ---
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN", "YOUR_ACCESS_TOKEN")  
INSTAGRAM_BUSINESS_ACCOUNT_ID = os.environ.get("INSTAGRAM_BUSINESS_ACCOUNT_ID", "YOUR_INSTAGRAM_BUSINESS_ACCOUNT_ID") 
HASHTAG = os.environ.get("HASHTAG", "グルメ") 
MAX_COUNT = 80 # 取得したい最大件数
MAX_DAYS = 30 # 遡る最大期間 (日)

# DynamoDBの設定
DYNAMODB_TABLE_NAME = os.environ.get("DYNAMODB_TABLE_NAME", "InstagramGourmetData")
API_BASE_URL = "https://graph.facebook.com/v19.0/"

# DynamoDBリソースの初期化
try:
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)
    print(f"DynamoDBテーブル '{DYNAMODB_TABLE_NAME}' を初期化しました。")
except Exception as e:
    print(f"DynamoDB初期化エラーが発生しました。IAM権限またはテーブル名を確認してください: {e}")

# --- get_hashtag_id 関数 ---
def get_hashtag_id(hashtag_name):
    """ハッシュタグ名からハッシュタグIDを取得する"""
    if not ACCESS_TOKEN or ACCESS_TOKEN == "YOUR_ACCESS_TOKEN":
        print("エラー: アクセストークンが環境変数に設定されていません。")
        return None
    if not INSTAGRAM_BUSINESS_ACCOUNT_ID or INSTAGRAM_BUSINESS_ACCOUNT_ID == "YOUR_INSTAGRAM_BUSINESS_ACCOUNT_ID":
        print("エラー: InstagramビジネスアカウントIDが環境変数に設定されていません。")
        return None
        
    endpoint = f"{API_BASE_URL}ig_hashtag_search"
    params = {
        "user_id": INSTAGRAM_BUSINESS_ACCOUNT_ID,
        "q": hashtag_name,
        "access_token": ACCESS_TOKEN
    }

    try:
        response = requests.get(endpoint, params=params)
        response.raise_for_status()
        data = response.json()

        if data and "data" in data and data["data"]:
            hashtag_id = data["data"][0]["id"]
            return hashtag_id
        else:
            return None

    except requests.exceptions.RequestException as e:
        try:
            error_data = response.json()
            print(f"リクエストエラーが発生しました (get_hashtag_id): {e}")
            print(f"APIエラー詳細: {error_data.get('error', {})}")
        except:
            print(f"リクエストエラーが発生しました (get_hashtag_id): {e}")
        return None

# --- get_top_hashtag_media 関数 ---
def get_top_hashtag_media(hashtag_id, max_count, max_days):
    """
    ハッシュタグIDに基づき、人気投稿（top_media）をページネーションで取得し、
    指定期間と件数でフィルタリングする
    """
    if not hashtag_id:
        return []

    date_limit = datetime.now(timezone.utc) - timedelta(days=max_days)
    all_media = []
    
    endpoint = f"{API_BASE_URL}{hashtag_id}/top_media" 
    fields = "id,caption,timestamp,permalink,like_count,comments_count,media_type" 
    params = {
        "user_id": INSTAGRAM_BUSINESS_ACCOUNT_ID,
        "fields": fields,
        "limit": 20, # APIの最大制限
        "access_token": ACCESS_TOKEN
    }

    while len(all_media) < max_count:
        print(f"\n- APIリクエスト中... (現在 {len(all_media)} 件)")
        
        try:
            response = requests.get(endpoint, params=params)
            response.raise_for_status()
            data = response.json()
            
            new_media = data.get("data", [])

            for media in new_media:
                media_timestamp = datetime.strptime(media.get('timestamp'), '%Y-%m-%dT%H:%M:%S%z')

                if media_timestamp < date_limit:
                    print(f"期間外の投稿に到達しました。取得を終了します。")
                    return all_media

                if len(all_media) >= max_count:
                    print(f"目標件数 {max_count} 件に達しました。取得を終了します。")
                    return all_media

                all_media.append(media)

            if "paging" in data and "next" in data["paging"]:
                endpoint = data["paging"]["next"]
                params = None
                time.sleep(1) 
            else:
                print("すべてのページを取得しました。")
                break

        except requests.exceptions.RequestException as e:
            try:
                error_data = response.json()
                print(f"リクエストエラーが発生しました (get_top_hashtag_media): {e}")
                print(f"   APIエラー詳細: {error_data.get('error', {})}")
            except:
                print(f"リクエストエラーが発生しました (get_top_hashtag_media): {e}")
            break
            
    return all_media

# --- save_to_dynamodb 関数 ---
def save_to_dynamodb(media_item):
    """取得したメディア情報をDynamoDBに保存する"""
    if 'table' not in globals() or not table:
        print("DynamoDBテーブルが初期化されていないため、書き込みをスキップしました。")
        return False
        
    try:
        # DynamoDBは空の文字列を許容しないため、キャプションが空の場合は ' ' を格納
        item = {
            'media_id': media_item.get('id'), 
            'permalink': media_item.get('permalink'),
            'caption': media_item.get('caption', ' ').replace('\n', ' '),
            'timestamp': media_item.get('timestamp'),
            'like_count': int(media_item.get('like_count', 0)), 
            'comments_count': int(media_item.get('comments_count', 0)), 
            'media_type': media_item.get('media_type'),
            'crawled_at': datetime.now(timezone.utc).isoformat()
        }
        
        table.put_item(Item=item)
        return True

    except Exception as e:
        print(f"DynamoDBへの書き込みエラーが発生しました (ID: {media_item.get('id')}): {e}")
        return False

# --- メインハンドラー関数 ---
def lambda_handler(event, context):
    """Lambda関数のエントリーポイント"""
    # 1. ハッシュタグIDの取得
    print(f"🔎 ハッシュタグ '{HASHTAG}' の情報を取得します...")
    hashtag_id = get_hashtag_id(HASHTAG)
    
    # 必須設定のエラーチェック
    if not hashtag_id and (ACCESS_TOKEN == "YOUR_ACCESS_TOKEN" or INSTAGRAM_BUSINESS_ACCOUNT_ID == "YOUR_INSTAGRAM_BUSINESS_ACCOUNT_ID"):
        return {'statusCode': 400, 'body': 'Configuration error: ACCESS_TOKEN or ID not set.'}
    if hashtag_id:
        # 2. 人気投稿メディアをページネーションで取得・フィルタリング
        media_list = get_top_hashtag_media(hashtag_id, MAX_COUNT, MAX_DAYS)
        
        if media_list:
            print(f"\n--- 最終的に取得したメディア件数 (全体): {len(media_list)} 件 ---")

            # 3. いいね数に基づくソート
            try:
                # リール絞り込みを削除。media_list全体をソート
                media_list_sorted = sorted(
                    media_list, 
                    key=lambda x: float(x.get('like_count', 0)), 
                    reverse=True
                )
            except Exception as e:
                print(f"ソート中にエラーが発生しました: {e}。ソートせずに処理を続行します。")
                media_list_sorted = media_list
            
            # 4. DynamoDBに保存
            dynamodb_saved_count = 0
            for media in media_list_sorted:
                if save_to_dynamodb(media):
                    dynamodb_saved_count += 1
                
            print(f"DynamoDBに {dynamodb_saved_count} 件のデータを書き込みました。")
            
            return {
                'statusCode': 200,
                'body': f'Successfully crawled and saved {dynamodb_saved_count} items to DynamoDB.'
            }
        else:
            return {'statusCode': 200, 'body': 'No media found matching criteria.'}
    
    return {'statusCode': 500, 'body': 'Failed to get hashtag ID.'}
