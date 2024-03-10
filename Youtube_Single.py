import googleapiclient.discovery 
from pprint import pprint
import pymongo
import pymysql
import pandas as pd
from datetime import datetime
import streamlit as st
from PIL import Image

# Accessing youtube API
def api_connect():
    api_service_name = "youtube"
    api_version = "v3"
    api_key = "AIzaSyCepq862nPZTRZbwF0pp8gY_EIx3-GtTmc"
    
    youtube = googleapiclient.discovery.build(api_service_name,api_version,developerKey=api_key)
    
    return youtube

youtube = api_connect()

#Accessing Channel info:
def channel_info(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id)
    response = request.execute()
    
    for i in response['items']:
        channel_data = {
            'channel_name' : i['snippet']['title'],
            'channel_id' : i['id'],
            'subscriber' : i['statistics']['subscriberCount'],
            'views' : i['statistics']['viewCount'],
            'Videos_count' : i['statistics']['videoCount'],
            'Channel_description' : i['snippet']['description'],
            'playlist_id' : i['contentDetails']['relatedPlaylists']['uploads']        
        }
    return channel_data

# Accessing Video_ids 
def video_ids(channel_id):
    video_ids = []
    video_id_request = youtube.channels().list(
                        id=channel_id,
                        part = 'contentDetails').execute()
    playlist_id = video_id_request['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
        video = youtube.playlistItems().list(
                    part='snippet',
                    playlistId=playlist_id,
                    maxResults = 50,
                    pageToken = next_page_token).execute()
        for i in range(len(video['items'])):
            video_ids.append(video['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = video.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids

# Accessing Video Information
def video_information(Video_Ids):
    video_info = []
    for video_id in Video_Ids:
        request = youtube.videos().list(
                            part = 'snippet,ContentDetails,statistics',
                            id = video_id)
        response = request.execute()

        for item in response['items']:
            data = {
                    'channel_name' : item['snippet']['channelTitle'],
                    'channel_id' : item['snippet']['channelId'],
                    'video_id' : item['id'],
                    'title' : item['snippet']['title'],
                    'tags' : item['snippet'].get('tags'),
                    'thumbnails' : item ['snippet']['thumbnails']['default']['url'],
                    'description' : item['snippet'].get('description'),
                    'published_date' : item['snippet']['publishedAt'],
                    'duration' : item['contentDetails']['duration'],
                    'views' : item['statistics'].get('viewCount'),
                    'likes' : item['statistics'].get('likeCount'),
                    'comments' : item['statistics'].get('commentCount'),
                    'favorite_count' : item['statistics']['favoriteCount'],
                    'definition' : item['contentDetails']['definition'],
                    'caption_status' : item['contentDetails']['caption']
                    }
            video_info.append(data)
    return video_info

# Accessing comment info
def comment_info(Video_Ids):
    comment_data = []
    try:
        for video_id in Video_Ids:
            request = youtube.commentThreads().list(
                        part = 'snippet',
                        videoId = video_id,
                        maxResults = 50)
            response = request.execute()

            for item in response['items']:
                data = {
                        'comment_id' : item['id'],
                        'video_Id' : item['snippet']['topLevelComment']['snippet']['videoId'],
                        'comment' : item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        'comment_author' : item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        'comment_published' : item['snippet']['topLevelComment']['snippet']['publishedAt']
                            }
                comment_data.append(data)
    except:
        pass
    return comment_data

# Accessing playlist info:
def playlist_information(channel_id):
    playlist_info = []
    next_page_token = None

    while True:
        request = youtube.playlists().list(
                    part = 'snippet,contentDetails',
                    channelId = channel_id,
                    maxResults = 50,
                    pageToken = next_page_token)
        response = request.execute()

        for item in response['items']:
            data = {
                    'playlist_id' : item['id'],
                    'title' : item['snippet']['title'],
                    'channel_id' : item['snippet']['channelId'],
                    'channel_name' : item['snippet']['channelTitle'],
                    'publishedAt' : item['snippet']['publishedAt'],
                    'video_count' : item['contentDetails']['itemCount']
                    }
            playlist_info.append(data)

        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break
    return playlist_info

# inserting the youtube data collection into mongodb
client = pymongo.MongoClient('mongodb://127.0.0.1:27017/')
mydb = client['youtube_db']

def channel_details(channel_id):
    ch_details = channel_info(channel_id)
    pl_details = playlist_information(channel_id)
    vid_ids = video_ids(channel_id)
    vid_details = video_information(vid_ids)
    com_details = comment_info(vid_ids)
    
    collection = mydb['channel_details']
    collection.insert_one({'channel_information' : ch_details,
                            'playlist_information' : pl_details,
                            'video_information' : vid_details,
                            'comment_information' : com_details})
    
    return 'upload completed successfully'

# creation of tables for youtube projects in mysql
myconnection = pymysql.connect(host = '127.0.0.1',user='root',passwd='Deva93_guvi@SQL')
cur = myconnection.cursor()

cur.execute("create database if not exists Youtube_db")

# creating table for channel 
def channels_table(selected_channel_name):
    myconnection = pymysql.connect(host = '127.0.0.1',user='root',passwd='Deva93_guvi@SQL',database = 'youtube_db')
    cur = myconnection.cursor()

    

    
    create = cur.execute('''create table if not exists channels(
                        Channel_Name varchar(100),
                        Channel_Id varchar(100) primary key,
                        Subscribers bigint,
                        Views bigint,
                        Total_Videos int,
                        Channel_Description text,
                        Playlist_Id varchar(100) )''')
    myconnection.commit()

    

    selected_channel=[]
    mydb = client['youtube_db']
    collection = mydb['channel_details']
    for ch_data in collection.find({'channel_information.channel_name':selected_channel_name},{'_id':0}):
        selected_channel.append(ch_data['channel_information'])
    channel_df=pd.DataFrame(selected_channel)

    for index,row in channel_df.iterrows():
        insert = ''' insert into channels(Channel_Name,
                                          Channel_Id,
                                          Subscribers,
                                          Views,
                                          Total_Videos,
                                          Channel_Description,
                                          Playlist_Id)

                                          values(%s,%s,%s,%s,%s,%s,%s)'''
        values = (row['channel_name'],
                  row['channel_id'],
                  row['subscriber'],
                  row['views'],
                  row['Videos_count'],
                  row['Channel_description'],
                  row['playlist_id'])

        try:
            cur.execute(insert,values)
            myconnection.commit()
        except:
            print('Channel values are already inserted')

 # creating table for video
def video_table(selected_channel_name):
    
    myconnection=pymysql.connect(host="127.0.0.1",user="root",passwd='Deva93_guvi@SQL',database='youtube_db')
    cur=myconnection.cursor()
   

    create=cur.execute("""CREATE TABLE IF NOT EXISTS videos(
                channel_name varchar(100),
                channel_id varchar(100),
                video_id varchar(100) PRIMARY KEY,
                title VARCHAR(500) ,\
                description TEXT,\
                published_date DATETIME,\
                views INT,\
                likes INT,\
                favorite_count INT,\
                comments INT,\
                duration TIME,\
                thumbnails TEXT,\
                caption_status varchar(50))""")
    myconnection.commit()

    selected_channel_video = []
    mydb = client['youtube_db']
    collection = mydb['channel_details']
    for video_data in collection.find({'channel_information.channel_name':selected_channel_name},{'_id':0}):
            selected_channel_video.append(video_data['video_information'])
    video_df=pd.DataFrame(selected_channel_video[0])
    video_df['published_date']=pd.to_datetime(video_df['published_date'])
    video_df['duration'] = pd.to_timedelta(video_df['duration']).dt.total_seconds()
    video_df = video_df.where(pd.notnull(video_df), None)
    
    video_df['duration'] = pd.to_datetime(video_df['duration'],unit='s').dt.strftime('%H:%M:%S')

    for index,row in video_df.iterrows():
            insert=("""insert into videos(
            channel_name,
            channel_id,
            video_id,
            title,
            description,
            published_date,
            views,
            likes,
            favorite_count,
            comments,
            duration,
            thumbnails,
            caption_status)
            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""")

            values=(row['channel_name'],
                    row['channel_id'],
                    row['video_id'],
                    row['title'],
                    row['description'],
                    row['published_date'],
                    row['views'],
                    row['likes'],
                    row['favorite_count'],
                    row['comments'],
                    row['duration'],
                    row['thumbnails'],
                    row['caption_status'])


            try:
                cur.execute(insert,values)
                myconnection.commit()

            except:
                print("videos values already inserted in the table")

# creating table for comment
def comment_table(selected_channel_name):

    myconnection=pymysql.connect(host="127.0.0.1",user="root",passwd='Deva93_guvi@SQL',database='youtube_db')
    cur=myconnection.cursor()
    
    create=cur.execute("""CREATE TABLE IF NOT EXISTS comments(comment_id varchar(100) PRIMARY KEY,
                video_Id varchar(50),
                comment TEXT,\
                comment_author TEXT,\
                comment_published DATETIME)""")
    myconnection.commit()

    selected_channel_comment = []
    mydb = client['youtube_db']
    collection = mydb['channel_details']
    for comment_data in collection.find({'channel_information.channel_name':selected_channel_name},{'_id':0}):
            selected_channel_comment.append(comment_data['comment_information'])
    comment_df=pd.DataFrame(selected_channel_comment[0])
    comment_df['comment_published']=pd.to_datetime(comment_df['comment_published'])

    for index,row in comment_df.iterrows():
            insert=("""insert into comments(comment_id,
            video_Id,
            comment,
            comment_author,
            comment_published)
            values(%s,%s,%s,%s,%s)""")

            values=(row['comment_id'],
                    row['video_Id'],
                    row['comment'],
                    row['comment_author'],
                    row['comment_published'])

            try:
                cur.execute(insert,values)
                myconnection.commit()
                
            except:
                print("This comments are already exist in comments table")

def table(selected_channel):
    channels_table(selected_channel)
    video_table(selected_channel)
    comment_table(selected_channel)
    
    return "Tables are created successfully"

st.title(':red[Youtube Data Harvesting and Warehousing]')
tab1, tab2, tab3 = st.tabs(['Collection','Migrate to MySQL','Analysis'])

def show_channel_table():
    ch_list=[]
    mydb = client['youtube_db']
    collection = mydb['channel_details']
    for ch_data in collection.find({},{'_id':0,'channel_information':1}):
        ch_list.append(ch_data['channel_information'])
    channel_table=st.dataframe(ch_list)
    return channel_table
#show_channel_table()

def show_videos_table():
    video_list = []
    mydb = client['youtube_db']
    collection = mydb['channel_details']
    for video_data in collection.find({},{"_id": 0,"video_information":1}):
        for i in range(len(video_data['video_information'])):
            video_list.append(video_data['video_information'][i])
    videos_table=st.dataframe(video_list)
    return videos_table
#show_videos_table()

def show_comments_table():
    comment_list = []
    mydb = client['youtube_db']
    collection = mydb['channel_details']
    for comment_data in collection.find({},{"_id": 0,"comment_information":1}):
        for i in range(len(comment_data['comment_information'])):
            comment_list.append(comment_data['comment_information'][i])
    comments_table=st.dataframe(comment_list)
    return comments_table
#show_comments_table()


with st.sidebar:
    image = Image.open('Youtube.png')
    rotated_image = image.rotate(90, expand=True)
    left_side_css = """
        <style>
            .left-side {
                display: flex;
                flex-direction: row;
            }
        </style>
    """


    st.markdown(left_side_css, unsafe_allow_html=True)
    st.image(rotated_image, use_column_width=False)
    
with tab1:
    st.header('Collection')    
    st.subheader ('Enter the Channel ID')
    channel_id = st.text_input('Enter a 24-character ID:')
    if st.button("Collect and Store data"):
        #for channel in channels:
            ch_ids = []
            mydb = client['youtube_db']
            collection = mydb['channel_details']
            for ch_data in collection.find({},{"_id":0,"channel_information":1}):
                    ch_ids.append(ch_data["channel_information"]["channel_id"])
            if channel_id in ch_ids:
                    st.success("Channel details of the given channel id: " + channel_id + " already exists")
            else:
                    output = channel_details(channel_id)
                    st.success(output)
   
            


    show_table = st.radio("SELECT THE TABLE FOR VIEW",("channels","videos","comments"))

    if show_table == "channels":
        show_channel_table()
    elif show_table =="videos":
        show_videos_table()
    elif show_table == "comments":
        show_comments_table()

with tab2:
    st.header('Data in MySQL')

    channel_name=[]
    mydb = client['youtube_db']
    collection = mydb['channel_details']
    for ch_data in collection.find({},{'_id':0,'channel_information':1}):
        channel_name.append(ch_data['channel_information']['channel_name'])
  
    select_channel = st.selectbox('Select the channel name: ',channel_name) 

    if st.button("Migrate to SQL"):
        display = table(select_channel)
        st.success(display)

with tab3:
    st.header('Analysis')
    st.subheader('Select the query')


    myconnection=pymysql.connect(host="127.0.0.1",user="root",password='Deva93_guvi@SQL',database='youtube_db')
    cur=myconnection.cursor()

    question = st.selectbox(
        'Please Select Your Question',
        ('1. Names of all the videos and their corresponding channel name',
        '2. Channels with most number of videos',
        '3. Top 10 most viewed videos and their respective channel name',
        '4. Comments on each video and their corrsponding video name',
        '5. Videos have the highest number of likes and their corresponding channel name',
        '6. Total number of likes for each video and their corrsponding video name',
        '7. Total number of views for each channel and their corresponding channel name',
        '8. Names of all the channels that have published videos in the year 2022',
        '9. Average duration of all videos in each channel and their corresponding channel name',
        '10. Videos having the highest number of comments and their corresponding channel name'))

    if question == '1. Names of all the videos and their corresponding channel name':
        query1 = "select title as videos, channel_name as ChannelName from videos;"
        cur.execute(query1)
        myconnection.commit()
        t1=cur.fetchall()
        st.write(pd.DataFrame(t1, columns=["Video Title","Channel Name"]))

    elif question == '2. Channels with most number of videos':
        query2 = "select Channel_Name as ChannelName,Total_Videos as NO_Videos from channels order by Total_Videos desc;"
        cur.execute(query2)
        myconnection.commit()
        t2=cur.fetchall()
        st.write(pd.DataFrame(t2, columns=["Channel Name","No Of Videos"]))

    elif question == '3. Top 10 most viewed videos and their respective channel name':
        query3 = '''select views as views , channel_name as ChannelName,title as VideoTitle from videos 
                            where views is not null order by views desc limit 10;'''
        cur.execute(query3)
        myconnection.commit()
        t3 = cur.fetchall()
        st.write(pd.DataFrame(t3, columns = ["Views","channel Name","video Title"]))

    elif question == '4. Comments on each video and their corrsponding video name':
        query4 = "select comments as No_comments ,title as VideoTitle from videos where comments is not null;"
        cur.execute(query4)
        myconnection.commit()
        t4=cur.fetchall()
        st.write(pd.DataFrame(t4, columns=["No Of Comments", "Video Title"]))

    elif question == '5. Videos have the highest number of likes and their corresponding channel name':
        query5 = '''select title as VideoTitle, channel_name as ChannelName, likes as LikesCount from videos 
                        where likes is not null order by likes desc;'''
        cur.execute(query5)
        myconnection.commit()
        t5 = cur.fetchall()
        st.write(pd.DataFrame(t5, columns=["video Title","Channel Name","Like Count"]))

    elif question == '6. Total number of likes for each video and their corrsponding video name':
        query6 = '''select likes as likeCount,title as VideoTitle from videos
                            where likes is not null order by likes desc;'''
        cur.execute(query6)
        myconnection.commit()
        t6 = cur.fetchall()
        st.write(pd.DataFrame(t6, columns=["Like Count","Video Title"]))

    elif question == '7. Total number of views for each channel and their corresponding channel name':
        query7 = "select Channel_Name as ChannelName, Views as Channelviews from channels order by Views desc;"
        cur.execute(query7)
        myconnection.commit()
        t7=cur.fetchall()
        st.write(pd.DataFrame(t7, columns=["Channel Name","Total Views"]))

    elif question == '8. Names of all the channels that have published videos in the year 2022':
        query8 = '''select title as Video_Title, published_date as VideoRelease, channel_name as ChannelName from videos 
                    where extract(year from published_date) = 2022;'''
        cur.execute(query8)
        myconnection.commit()
        t8=cur.fetchall()
        st.write(pd.DataFrame(t8,columns=["Name", "Video Publised On", "ChannelName"]))

    elif question == '9. Average duration of all videos in each channel and their corresponding channel name':
        query9 =  "SELECT channel_name as ChannelName, AVG(duration) AS average_duration FROM videos GROUP BY channel_name;"
        cur.execute(query9)
        myconnection.commit()
        t9=cur.fetchall()
        t9 = pd.DataFrame(t9, columns=['ChannelTitle', 'Average Duration'])
        T9=[]
        for index, row in t9.iterrows():
            channel_title = row['ChannelTitle']
            average_duration = row['Average Duration']
            average_duration_str = str(average_duration)
            T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
        st.write(pd.DataFrame(T9))

    elif question == '10. Videos having the highest number of comments and their corresponding channel name':
        query10 = '''select title as VideoTitle, channel_name as ChannelName, comments as Comments from videos 
                        where comments is not null order by comments desc;'''
        cur.execute(query10)
        myconnection.commit()
        t10=cur.fetchall()
        st.write(pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'NO Of Comments']))


