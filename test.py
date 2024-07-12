import streamlit as st
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
import isodate
from datetime import datetime
from sqlalchemy import text

# Function to convert ISO 8601 to MySQL datetime format
def convert_to_mysql_datetime(iso_datetime):
    if iso_datetime:
        return datetime.strptime(iso_datetime, '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
    return None


# Function to get YouTube data
def get_youtube_data(api_key, channel_id):
    try:
        youtube = build('youtube', 'v3', developerKey=api_key)
        
        # Get channel details
        channel_response = youtube.channels().list(
            part='snippet,statistics,contentDetails,status',
            id=channel_id
        ).execute()
        
        channel_data = channel_response['items'][0]

        # Convert statistics to integers
        video_count = int(channel_data['statistics'].get('videoCount', 0))
        subscription_count = int(channel_data['statistics'].get('subscriberCount', 0))
        channel_views = int(channel_data['statistics'].get('viewCount', 0))

        # Determine channel status
        is_active = video_count > 0
        is_verified = channel_data['status'].get('longUploadsStatus', 'not_verified') == 'eligible'
        channel_status = 'active' if is_active else 'inactive'
        channel_verified_status = 'verified' if is_verified else 'not verified'
        
        channel_info = {
            "channel_name": channel_data['snippet']['title'],
            "channel_id": channel_id,
            "subscription_count": subscription_count,
            "channel_views": channel_views,
            "channel_description": channel_data['snippet']['description'],
            "playlist_id": channel_data['contentDetails']['relatedPlaylists']['uploads'],
            "channel_status": channel_status,
            "channel_verified_status": channel_verified_status
        }
        
        # Get videos from playlist
        playlist_id = channel_info['playlist_id']
        videos = []
        next_page_token = None
        
        while True:
            playlist_response = youtube.playlistItems().list(
                part='snippet',
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            ).execute()
            
            videos.extend(playlist_response['items'])
            next_page_token = playlist_response.get('nextPageToken')
            if not next_page_token:
                break
        
        video_ids = [video['snippet']['resourceId']['videoId'] for video in videos]
        
        # Ensure video_ids is not empty
        if not video_ids:
            return {"error": "No videos found for this channel."}
        
        # Fetch video details in chunks of 50 (YouTube API limit)
        videos_data = []
        for i in range(0, len(video_ids), 50):
            chunk_ids = video_ids[i:i + 50]
            video_ids_str = ','.join(chunk_ids)
            
            try:
                video_response = youtube.videos().list(
                    part='snippet,statistics,contentDetails',
                    id=video_ids_str
                ).execute()
                
                videos_data.extend(video_response['items'])
            except HttpError as e:
                return {"error": f"An error occurred: {e}"}
        
        # Prepare video data
        video_list = []
        comments_list = []
        for video in videos_data:
            video_id = video['id']
            video_info = {
                "video_id": video_id,
                "channel_id": channel_id,
                "video_name": video['snippet']['title'],
                "video_description": video['snippet']['description'],
                "tags": ','.join(video['snippet'].get('tags', [])),
                "published_at": video['snippet']['publishedAt'],
                "view_count": int(video['statistics'].get('viewCount', 0)),
                "like_count": int(video['statistics'].get('likeCount', 0)),
                "dislike_count": int(video['statistics'].get('dislikeCount', 0)),
                "favorite_count": int(video['statistics'].get('favoriteCount', 0)),
                "comment_count": int(video['statistics'].get('commentCount', 0)),
                "duration": isodate.parse_duration(video['contentDetails']['duration']).total_seconds(),
                "thumbnail": video['snippet']['thumbnails']['default']['url'],
                "caption_status": video['contentDetails']['caption']
            }
            video_list.append(video_info)
            
            # Get comments for the video
            try:
                comments_response = youtube.commentThreads().list(
                    part='snippet',
                    videoId=video_id,
                    maxResults=100,
                    textFormat='plainText'
                ).execute()
                
                for comment in comments_response['items']:
                    comment_data = comment['snippet']['topLevelComment']['snippet']
                    comments_list.append({
                        "comment_id": comment['id'],
                        "video_id": video_id,
                        "comment_text": comment_data['textDisplay'],
                        "comment_author": comment_data['authorDisplayName'],
                        "comment_published_at": comment_data['publishedAt']
                    })
            except HttpError:
                continue
        
        data = {
            "channel_info": channel_info,
            "videos": video_list,
            "comments": comments_list,
            "playlists": [{
                "playlist_id": playlist_id,
                "channel_id": channel_id,
                "playlist_name": "Uploads"
            }]
        }
        
        return data

    except HttpError as e:
        return {"error": f"An error occurred: {e}"}
    except Exception as e:
        return {"error": f"An unexpected error occurred: {e}"}

# Database connection
DATABASE_URL = "mysql+pymysql://root:Admin@localhost/sys"
engine = create_engine(DATABASE_URL)

def run_query(query):
    with engine.connect() as connection:
        result = connection.execute(text(query))  # Wrap the query string with the text() function
        return pd.DataFrame(result.fetchall(), columns=result.keys())
    

# List of queries
queries = {
    "Names of all the videos and their corresponding channels": """
        SELECT video.video_name, channel.channel_name
        FROM video
        JOIN channel ON video.channel_id = channel.channel_id;
    """,
    "Channels with the most number of videos and their count": """
        SELECT channel.channel_name, COUNT(video.video_id) AS video_count
        FROM video
        JOIN channel ON video.channel_id = channel.channel_id
        GROUP BY channel.channel_id
        ORDER BY video_count DESC;
    """,
    "Top 10 most viewed videos and their respective channels": """
        SELECT video.video_name, channel.channel_name, video.view_count
        FROM video
        JOIN channel ON video.channel_id = channel.channel_id
        ORDER BY video.view_count DESC
        LIMIT 10;
    """,
    "Number of comments on each video and their corresponding video names": """
        SELECT video.video_name, COUNT(comment.comment_id) AS comment_count
        FROM comment
        JOIN video ON comment.video_id = video.video_id
        GROUP BY video.video_id;
    """,
    "Videos with the highest number of likes and their corresponding channel names": """
        SELECT video.video_name, channel.channel_name, video.like_count
        FROM video
        JOIN channel ON video.channel_id = channel.channel_id
        ORDER BY video.like_count DESC
        LIMIT 10;
    """,
    "Total number of likes and dislikes for each video and their corresponding video names": """
        SELECT video.video_name, video.like_count, video.dislike_count
        FROM video;
    """,
    "Total number of views for each channel and their corresponding channel names": """
        SELECT channel.channel_name, channel.channel_views
        FROM channel;
    """,
    "Names of all channels that have published videos in the year 2022": """
        SELECT DISTINCT channel.channel_name
        FROM video
        JOIN channel ON video.channel_id = channel.channel_id
        WHERE YEAR(video.published_date) = 2022
    """,
    "Average duration of all videos in each channel and their corresponding channel names": """
        SELECT channel.channel_name, AVG(video.duration) AS avg_duration
        FROM video
        JOIN channel ON video.channel_id = channel.channel_id
        GROUP BY channel.channel_id;
    """,
    "Videos with the highest number of comments and their corresponding channel names": """
        SELECT video.video_name, channel.channel_name, COUNT(comment.comment_id) AS comment_count
        FROM comment
        JOIN video ON comment.video_id = video.video_id
        JOIN channel ON video.channel_id = channel.channel_id
        GROUP BY video.video_id
        ORDER BY comment_count DESC
        LIMIT 10;
    """
}            

# Function to delete existing channel and its related data

def delete_channel_data(engine, channel_id):
    with engine.connect() as connection:
        # First, find all playlist_ids for the given channel_id
        result = connection.execute(text("SELECT playlist_id FROM playlist WHERE channel_id = :channel_id"), {"channel_id": channel_id})
        playlist_ids = [row[0] for row in result]  # Access the first element of each row
        
        # Find all video_ids for the playlists
        for playlist_id in playlist_ids:
            video_result = connection.execute(text("SELECT video_id FROM video WHERE playlist_id = :playlist_id"), {"playlist_id": playlist_id})
            video_ids = [row[0] for row in video_result]
            
            # Delete comments for each video_id
            for video_id in video_ids:
                connection.execute(text("DELETE FROM comment WHERE video_id = :video_id"), {"video_id": video_id})
            
            # Delete videos for each playlist_id
            connection.execute(text("DELETE FROM video WHERE playlist_id = :playlist_id"), {"playlist_id": playlist_id})
        
        # Delete playlists for the given channel_id
        connection.execute(text("DELETE FROM playlist WHERE channel_id = :channel_id"), {"channel_id": channel_id})



# Function to store data in the database
def store_data(engine, data):
    channel_info = data['channel_info']
    playlists = data['playlists']
    videos = data['videos']
    comments = data['comments'] 
    
    # Delete existing data
    delete_channel_data(engine, channel_info['channel_id'])

    # Load existing video_id values
    existing_videos = pd.read_sql('SELECT video_id FROM video', engine)
    

    # Insert new data
    channel_df = pd.DataFrame([{
        'channel_id': channel_info['channel_id'],
        'channel_name': channel_info['channel_name'],
        'channel_views': channel_info['channel_views'],
        'channel_description': channel_info['channel_description'],
        'channel_status': channel_info['channel_status']
    }])

    try:
        channel_df.to_sql('channel', engine, if_exists='append', index=False)
    except IntegrityError:
        st.warning('Channel data already exists in the database.')

    # Insert new data for the playlist table
    playlist_df = pd.DataFrame([{
        'playlist_id': playlist.get('playlist_id', None),
        'channel_id': playlist.get('channel_id', None),
        'playlist_name': playlist.get('playlist_name', None),
    } for playlist in playlists])
    try:
        playlist_df.to_sql('playlist', engine, if_exists='append', index=False)
    except IntegrityError:
        st.warning('Channel Playlist already exist.')
 # Insert new data for the videos
    video_df = pd.DataFrame([{
        'video_id': video.get('video_id', None),
        'playlist_id': channel_info.get('playlist_id', None),
        'video_name': video.get('video_name', None),
        'video_description': video.get('video_description', None),
        'published_date': convert_to_mysql_datetime(video.get('published_at', None)),
        'view_count': video.get('view_count', None),
        'like_count': video.get('like_count', None),
        'dislike_count': video.get('dislike_count', None),
        'favorite_count': video.get('favorite_count', None),
        'comment_count': video.get('comment_count', None),
        'duration': video.get('duration', None),
        'thumbnail': video.get('thumbnail', None),
        'caption_status': video.get('caption_status', None),
        'channel_id': channel_info['channel_id']
    } for video in videos])

    try:
        video_df.to_sql('video', engine, if_exists='append', index=False)
    except IntegrityError:
        st.warning('Video data already exists.')
    # Check if video_ids in comments exist in video table
    video_ids = set(video_df['video_id'].tolist())
    comment_video_ids = set(comment['video_id'] for comment in comments)

    missing_video_ids = comment_video_ids - video_ids
    if missing_video_ids:
        st.warning(f"Some comments are associated with missing video IDs: {missing_video_ids}")
        comments = [comment for comment in comments if comment['video_id'] not in missing_video_ids]

    # Insert new data for the comments
    comment_df = pd.DataFrame([{
        'comment_id': comment.get('comment_id', None),
        'video_id': comment.get('video_id', None),
        'comment_text': comment.get('comment_text', None),
        'comment_author': comment.get('comment_author', None),
        'comment_published_date': convert_to_mysql_datetime(comment.get('comment_published_at', None))
    } for comment in comments])

    try:
        comment_df.to_sql('comment', engine, if_exists='append', index=False)
    except IntegrityError:
        st.warning('Comment data already exists.')


# Streamlit App Interface
st.title('YouTube Data Harvesting and Warehousing')

# Sidebar inputs
st.sidebar.header('Input YouTube Channel ID')
channel_id = st.sidebar.text_input('Channel ID')

# Initialize session state
if 'data' not in st.session_state:
    st.session_state['data'] = None

# Retrieve data button
if st.sidebar.button('Retrieve Data'):
    api_key = 'AIzaSyDd86JEIEGKRH39RxaI0Uc-MaPLA-as5F8'  
    if channel_id:
        data = get_youtube_data(api_key, channel_id)
        if "error" in data:
            st.error(data["error"])
        else:
            st.session_state['data'] = data  # Store in session state
            st.subheader('Channel Information')
            st.write(data['channel_info'])
            
            st.subheader('Videos')
            for video in data['videos']:
                st.write(video)
                
            st.subheader('Comments')
            for comment in data['comments']:
                st.write(comment)
    else:
        st.error('Please enter a valid Channel ID.')

# Button to store data
if st.sidebar.button('Store Data'):
    if st.session_state['data']:
        store_data(engine, st.session_state['data'])
        st.write("Data stored successfully")
        st.session_state['data'] = None  # Clear data after storing
    else:
        st.error('No data to store. Please retrieve data first.')

# Button to clear session state
if st.sidebar.button('Clear Data'):
    st.session_state['data'] = None
    st.write("Session data cleared")

# Query selection
query_selection = st.selectbox("Select a query to run", list(queries.keys()))

if st.button("Run Query"):
    query = queries[query_selection]
    result_df = run_query(query)
    st.write(result_df)





