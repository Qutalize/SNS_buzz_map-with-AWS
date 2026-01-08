import json
import os
import datetime
import isodate
import time
from datetime import timezone
import boto3
from botocore.exceptions import ClientError
from googleapiclient.discovery import build

# --- 設定情報 ---
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY") 
TARGET_DB_REGION = os.environ.get("TARGET_DB_REGION", os.environ.get("AWS_REGION", "us-east-1"))
YOUTUBE_DB_NAME = os.environ.get("TARGET_DB_NAME") 

# --- クライアントの初期化 ---
YOUTUBE_CLIENT = None
DDB_TABLE_YOUTUBE = None

try:
    if YOUTUBE_API_KEY:
        YOUTUBE_CLIENT = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
    
    # YouTubeDBへの書き込みクライアントを初期化
    DDB_TABLE_YOUTUBE = boto3.resource('dynamodb', region_name=TARGET_DB_REGION).Table(YOUTUBE_DB_NAME)
    print(f"クライアント初期化完了。YouTubeDB: {YOUTUBE_DB_NAME} ({TARGET_DB_REGION})")
except Exception as e:
    print(f"初期化エラー: {e}")

# --- YouTube API クロールロジック ---
def get_youtube_videos():
    """
    YouTube Data API v3 を呼び出し、DB登録用のデータリストを生成する
    """
    if not YOUTUBE_CLIENT:
        print("YouTube APIキーが未設定です。")
        return []
    
    print("Fetching data from YouTube API...")
    
    try:
        # --- 検索設定 ---
        published_after = (datetime.datetime.utcnow() - datetime.timedelta(days=30)).isoformat("T") + "Z"
        query = "グルメ OR ラーメン OR 寿司 OR カフェ"
        MAX_VIDEOS_TO_CHECK = 80
        
        # --- 1. 検索実行 (Quota: 100) ---
        search_response = YOUTUBE_CLIENT.search().list(
            q=query,
            part="id,snippet",
            type="video",
            order="viewCount",
            publishedAfter=published_after,
            maxResults=MAX_VIDEOS_TO_CHECK
        ).execute()

        video_ids_from_search = [item["id"]["videoId"] for item in search_response.get("items", [])]
        if not video_ids_from_search:
            print("No new videos found.")
            return []

        # --- 2. 詳細情報取得 (Quota: 1) ---
        # statistics (views/likes) と contentDetails (duration) を取得
        videos_response = YOUTUBE_CLIENT.videos().list(
            part="snippet,contentDetails,statistics",
            id=",".join(video_ids_from_search)
        ).execute()

        videos_to_process = []
        channel_ids = set() # チャンネルIDを重複なく収集
        
        # 2分以下の動画のみをフィルタリング
        for video in videos_response.get("items", []):
            try:
                duration = isodate.parse_duration(video["contentDetails"]["duration"]).total_seconds()
                if duration <= 120:
                    videos_to_process.append(video)
                    channel_ids.add(video["snippet"]["channelId"]) # ★ チャンネルIDを収集
            except:
                # durationがパースできない場合はスキップ
                continue

        # --- チャンネル情報（登録者数）取得  ---
        channel_stats = {}
        if channel_ids:
            print(f"Fetching subscriber counts for {len(channel_ids)} channels...")
            
            channel_response = YOUTUBE_CLIENT.channels().list(
                part="statistics",
                id=",".join(list(channel_ids))
            ).execute()
            
            for item in channel_response.get("items", []):
                channel_stats[item["id"]] = item["statistics"].get("subscriberCount", 0)

        # --- 最終データリストの作成 ---
        final_data_list = []
        for video in videos_to_process:
            video_id = video["id"]
            channel_id = video["snippet"]["channelId"]

            # 登録者数を動画データに追加
            video['subscriber_count'] = int(channel_stats.get(channel_id, 0))
            final_data_list.append(video)

        print(f"Fetched {len(final_data_list)} items with subscriber data.")
        return final_data_list

    except Exception as e:
        print(f"Error fetching from YouTube: {e}")
        return []


# --- DynamoDB 格納ロジック ---
def save_to_youtube_db(video):
    """
    YouTube動画の詳細をYouTubeDBに格納する
    """
    if not DDB_TABLE_YOUTUBE:
        return False
        
    try:
        video_id = video["id"]
        snippet = video["snippet"]
        stats = video["statistics"]
        
        item = {
            'videoId': video_id, # 主キー
            'title': snippet.get("title", "No Title"),
            'description': snippet.get("description", "No Description"),
            'url': f"https://www.youtube.com/watch?v={video_id}",
            'views': int(stats.get("viewCount", 0)),
            'likes': int(stats.get("likeCount", 0)),
            'published_at': snippet.get("publishedAt"),
            'crawled_at': datetime.datetime.now(timezone.utc).isoformat(),
            'subscriber_count': video.get('subscriber_count', 0)
        }
        
        DDB_TABLE_YOUTUBE.put_item(Item=item)
        return True
    except ClientError as e:
        print(f"DynamoDB書き込みエラー (ID: {video_id}): {e}")
    except Exception as e:
        print(f"その他のエラー (ID: {video_id}): {e}")
        return False

# --- Lambda ハンドラ ---
def lambda_handler(event, context):
    videos_to_process = get_youtube_videos()
    if not videos_to_process:
        return {'statusCode': 200, 'body': 'No videos to process.'}

    saved_count = 0
    for video in videos_to_process:
        if save_to_youtube_db(video):
            saved_count += 1
            
    print(f"YouTubeクロール完了。{len(videos_to_process)}動画中 {saved_count} 件をYouTubeDBに格納しました。")

    return {
        'statusCode': 200,
        'body': json.dumps(f"YouTube crawl successful. Saved {saved_count} items.")
    }
