from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

#API KEY connection
def Api_connect():
    Api_key="AIzaSyAp_Jw26mnxayAtzLnrFc0l8OZUtCrvUBk"
    api_service_name="youtube"
    api_version="v3"
    

    youtube=build(api_service_name,api_version,developerKey=Api_key)

    return youtube


youtube=Api_connect()



#CHANNEL INFORMATION
def get_channel_info(channel_id):
          request=youtube.channels().list(
                         part='snippet,statistics,contentDetails',
                         id=channel_id,
          )
          output=request.execute()

          for i in output['items']:
               channel_information={
                    'channel_Id': output['items'][0]['id'],
                    'channel_Name': output['items'][0]['snippet']['title'],
                    'subscription_count': output['items'][0]['statistics']['subscriberCount'],
                    'channel_Views': output['items'][0]['statistics']['viewCount'],
                    'Total_videos' : output['items'][0]['statistics']['videoCount'],
                    'channel_Description': output['items'][0]['snippet']['description'],
                    'Playlist_Id': output['items'][0]['contentDetails']['relatedPlaylists']['uploads']
          }
          return channel_information

  
#video id
def get_video_ids(channel_id):
    Video_Ids=[]
    output1=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=output1['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token=None

    while True:
        output2=youtube.playlistItems().list(part='snippet',
                                                playlistId=Playlist_Id,
                                                maxResults=50,
                                                pageToken=next_page_token).execute()
        for i in range(len(output2['items'])):
            Video_Ids.append(output2['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=output2.get('nextPageToken') 

        if next_page_token is None:
            break
    return Video_Ids


def get_video_info(video_ids):
    video_output = []
    for video_id in video_ids:
        request = youtube.videos().list(
                            part ="snippet,contentDetails,statistics",
                            id = video_id
        )
        response = request.execute()

        for item in response["items"]:
            video_information = dict(Channel_Name = item["snippet"]["channelTitle"],
                        Channel_Id = item["snippet"]["channelId"],
                        Video_Id = item["id"],
                        Title = item["snippet"]["title"],
                        Tags = item["snippet"].get("tags"),
                        Thumbnail=item['snippet']['thumbnails']['default']['url'],
                        Description = item["snippet"].get("description"),
                        PublishedDate = item["snippet"]["publishedAt"],
                        Duration = item["contentDetails"]["duration"],
                        Views = item["statistics"].get("viewCount"),
                        Likes = item["statistics"].get('likeCount'),
                        Comments = item["statistics"].get('commentCount'),
                        Favorite_Count = item["statistics"]["favoriteCount"],
                        Definition = item["contentDetails"]["definition"],
                        Caption_Status = item["contentDetails"]["caption"]
                        )

            video_output.append(video_information)
    return video_output


#comment information
def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
                request=youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=50
                )
                response=request.execute()

                for item in response['items']:
                    data={
                            'Comment_Id': response['items'][0]['snippet']['topLevelComment']['id'],
                            'Comment_Text': response['items'][0]['snippet']['topLevelComment']['snippet']['textDisplay'],
                            'Comment_Author': response['items'][0]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            'Comment_PublishedAt': response['items'][0]['snippet']['topLevelComment']['snippet']['publishedAt']}
                    Comment_data.append(data)

    except:
        pass
    return Comment_data               
                        

#playlist data
def get_playlist_info(channel_id):
    next_page_token=None
    playlist=[]
    while True:
        request=youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        
        )
        response=request.execute()

        for item in response['items']:
            playlist_data={
                'playlist_Id': response['items'][0]['id'],
                'Channel_Id': response['items'][0]['snippet']['channelId'],
                'playlist_name': response['items'][0]['snippet']['channelTitle']}
            
            playlist.append(playlist_data)
        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
             break
    return playlist    


#connecting to mongodb
client=pymongo.MongoClient("mongodb+srv://aswininehru:aswini1207@cluster0.32co2zi.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db=client["youtube_dataharvesting"]


def channel_details(channel_id):
    ch_detail=get_channel_info(channel_id)
    pl_detail=get_playlist_info(channel_id)
    vi_ids=get_video_ids(channel_id)
    vi_detail=get_video_info(vi_ids)
    com_detail=get_comment_info(vi_ids)

    coll=db["channel_details"]
    coll.insert_one({"channel_information":ch_detail,"playlist_information":pl_detail,
                     "video_information":vi_detail,"comment_information":com_detail})
    return "upload completed sucessfully"



#channel table creation
def channel_table():
    try:
        mydb = psycopg2.connect(host="localhost",
                                user="postgres",
                                password="aswini",
                                database="youtube_data",
                                port="5432")
        cursor = mydb.cursor()

        drop_query = '''DROP TABLE IF EXISTS channels'''
        cursor.execute(drop_query)
        mydb.commit()

        create_query = '''CREATE TABLE IF NOT EXISTS channels (
                            Channel_Id VARCHAR(80) PRIMARY KEY,
                            Channel_Name VARCHAR(100),
                            Subscribers BIGINT,
                            Views BIGINT,
                            Total_videos INT,
                            Channel_Description TEXT,
                            Playlist_Id VARCHAR(80)
                        )'''
        cursor.execute(create_query)
        mydb.commit()
    except psycopg2.Error as e:
        print("Error creating or connecting to the database:", e)
        return

    db = client["youtube_dataharvesting"]
    coll = db["channel_details"]
    ch_list = []

    for channel_info in coll.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(channel_info["channel_information"])

    df = pd.DataFrame(ch_list)

    for index, row in df.iterrows():
        insert_query = '''INSERT INTO channels (Channel_Id, Channel_Name, Subscribers, Views, Total_videos, Channel_Description, Playlist_Id)
                          VALUES (%s, %s, %s, %s, %s, %s, %s)'''
        values = (row['channel_Id'],
                  row['channel_Name'],
                  row['subscription_count'],
                  row['channel_Views'],
                  row['Total_videos'],
                  row['channel_Description'],
                  row['Playlist_Id'])
        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except psycopg2.Error as e:
            print("Error inserting channel values:", e)

    cursor.close()
    mydb.close()

channel_table()


def video_table():
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="aswini",
        database="youtube_data",
        port="5432"
    )
    cursor = mydb.cursor()
    drop_query = '''DROP TABLE IF EXISTS videos'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query = '''CREATE TABLE IF NOT EXISTS videos(Channel_Name VARCHAR(100),
                                        Channel_Id VARCHAR(100),
                                        Video_Id VARCHAR(100) PRIMARY KEY,
                                        Description text,
                                        Title varchar(150),
                                        Tags text,
                                        Thumbnail VARCHAR(100),
                                        PublishedDate timestamp,
                                        Views bigint,
                                        Likes bigint,
                                        Comments int,
                                        Favorite_count int,
                                        Duration interval,
                                        Caption_Status VARCHAR(100)
                                        )'''

    cursor.execute(create_query)
    mydb.commit()

    vi_list = []
    db = client["youtube_dataharvesting"]
    coll = db["channel_details"]
    for vi_info in coll.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_info["video_information"])):
            vi_list.append(vi_info["video_information"][i])
    df2 = pd.DataFrame(vi_list)



    for index,row in df2.iterrows():
        insert_query='''insert into videos(Channel_Name,
                                        Channel_Id,
                                        Video_Id,
                                        Description,
                                        Title,
                                        Tags,                                   
                                        Thumbnail,
                                        PublishedDate,
                                        Views,
                                        Likes,
                                        Comments,
                                        Favorite_Count,
                                        Duration,
                                        Caption_Status
                                        )
                                                        
                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''


        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Video_Id'],
                row['Description'],
                row['Title'],              
                row['Tags'],
                row['Thumbnail'],
                row['PublishedDate'],
                row['Views'],
                row['Likes'],
                row['Comments'],
                row['Favorite_Count'],
                row['Duration'],
                row['Caption_Status']
                )       
        cursor.execute(insert_query,values)
        mydb.commit()

video_table()


def playlist_table():
   
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="aswini",
        database="youtube_data",
        port="5432")
    cursor = mydb.cursor()

    drop_query = '''DROP TABLE IF EXISTS playlists'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query = '''CREATE TABLE IF NOT EXISTS playlists(
                                                    playlist_Id VARCHAR(80),
                                                    Channel_Id VARCHAR(100),
                                                    playlist_name VARCHAR(255)
                                                    
                                                    )'''
    cursor.execute(create_query)
    mydb.commit()

    pl_list=[]
    db=client["youtube_dataharvesting"]
    coll = db["channel_details"]
    for pl_data in coll.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range (len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1 = pd.DataFrame(pl_list)

    for index, row in df1.iterrows():
        insert_query = '''INSERT INTO playlists(
                                            playlist_Id,
                                            Channel_Id,
                                            playlist_name
                                            )
                                            VALUES(%s, %s, %s)'''
                                        
        values = (
            row['playlist_Id'],
            row['Channel_Id'],
            row['playlist_name'])

        cursor.execute(insert_query, values)
        mydb.commit()




def comment_table():

    mydb=psycopg2.connect(host="localhost",
                            user="postgres",
                            password="aswini",
                            database="youtube_data",
                            port="5432")
    cursor=mydb.cursor()

    drop_query = '''DROP TABLE IF EXISTS comments'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query='''create table if not exists comments(Comment_Id varchar(100),
                                                    Comment_Text text,
                                                    Comment_Author varchar(150),
                                                    Comment_PublishedAt timestamp
                                                    )'''

    cursor.execute(create_query)
    mydb.commit()


    comment_list = []
    db = client["youtube_dataharvesting"]
    coll = db["channel_details"]
    for comm_info in coll.find({},{"_id":0,"comment_information":1}):
            for i in range(len(comm_info["comment_information"])):
                    comment_list.append(comm_info["comment_information"][i])
    df3 = pd.DataFrame(comment_list)


    for index,row in df3.iterrows():
            insert_query='''insert into comments(Comment_Id,
                                                    Comment_Text,
                                                    Comment_Author,
                                                    Comment_PublishedAt
                                                    )
                                                    
                                                    values(%s,%s,%s,%s)'''
            
            
            values=(row['Comment_Id'],
                    row['Comment_Text'],
                    row['Comment_Author'],
                    row['Comment_PublishedAt']
                    )

            cursor.execute(insert_query,values)
            mydb.commit()

   
def tables():
    channel_table()
    playlist_table()
    video_table()
    comment_table()

    return "tables created successfully"


def show_channel_table():
    db = client["youtube_dataharvesting"]
    coll = db["channel_details"]
    ch_list = []

    for channel_info in coll.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(channel_info["channel_information"])

    df = st.dataframe(ch_list)

    return df


def show_video_table():
    vi_list = []
    db = client["youtube_dataharvesting"]
    coll = db["channel_details"]
    for vi_info in coll.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_info["video_information"])):
            vi_list.append(vi_info["video_information"][i])
    df2 = st.dataframe(vi_list)

    return df2

def show_playlist_table():
    pl_list=[]
    db=client["youtube_dataharvesting"]
    coll = db["channel_details"]
    for pl_data in coll.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range (len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1 = st.dataframe(pl_list)

    return df1


def show_comment_table():
        comment_list = []
        db = client["youtube_dataharvesting"]
        coll = db["channel_details"]
        for comm_info in coll.find({},{"_id":0,"comment_information":1}):
                for i in range(len(comm_info["comment_information"])):
                        comment_list.append(comm_info["comment_information"][i])
        df3 = st.dataframe(comment_list)

        return df3


#streamlit
with st.sidebar:
    st.title(":blue[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Project Take away")
    st.caption("Python coding")
    st.caption("Data collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and SQL")
channel_id=st.text_input("Enter the channel ID")

if st.button("collect and store the data in MongoDB"):
    ch_ids=[]
    db=client["youtube_dataharvesting"]
    coll=db["channel_details"]
    for channel_info in coll.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(channel_info["channel_information"]["channel_Id"])

    if channel_id in ch_ids:
        st.success("channel detals of the given channel id already exists")
    else:
        insert=channel_details(channel_id)
        st.success(insert)

if st.button("Migrate to sql"):
    Table=tables()
    st.success(Table)

show_table=st.radio("select the table to view",("channels","playlists","videos","comments"))

if show_table=="channels":
    show_channel_table()

elif show_table=="playlists":
    show_playlist_table()

elif show_table=="videos":
    show_video_table()

elif show_table=="comments":
    show_comment_table()


#SQL CONNECTION
mydb = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="aswini",
    database="youtube_data",
    port="5432"
)
cursor = mydb.cursor()


question=st.selectbox("select your question",("1.All the videos and channel name",
                                              "2.channels with most number of videos",
                                              "3.10 most viewed videos",
                                              "4.comments in each videos",
                                              "5.videos with highest likes",
                                              "6.likes of all videos",
                                              "7.views of each channel",
                                              "8.videos published in the year 2022",
                                              "9.average duration of all videos in each channel",
                                              "10.videos with highest number of comments"))

if question=="1.All the videos and channel name":
    query1='''select Title as videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["videos","channel name"])
    st.write(df)


elif question=="2.channels with most number of videos":
    query2='''select channel_name as channelname,total_videos as no_videos from channels 
                order by total_videos desc'''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name","No of videos"])
    st.write(df2)

elif question=="3.10 most viewed videos":
    query3='''select views as views,channel_name as channelname,title as videotitle from videos 
                where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views","channel name","videotitle"])
    st.write(df3)


elif question=="4.comments in each videos":
    query4='''select comments as no_comments,title as videotitle from videos where comments is not null'''
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["no of comments","videotitle"])
    st.write(df4)

elif question=="5.videos with highest likes": 
    query5='''select title as videotitle,channel_name as channelname,likes as likecount
                from videos where likes is not null order by likes desc'''
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
    st.write(df5)

elif question=="6.likes of all videos":
    query6='''select likes as likecount,title as videotitle from videos'''
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["likecount","videotitle"])
    st.write(df6)

elif question=="7.views of each channel":
    query7='''select channel_name as channelname ,views as totalviews from channels'''
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channel name","totalviews"])
    st.write(df7)

elif question=="8.videos published in the year 2022":
    query8='''select title as video_title,publishedDate as videorelease,channel_name as channelname from videos
                where extract(year from publishedDate)=2022'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["videotitle","publishedDate","channelname"])
    st.write(df8)
  

elif question=="9.average duration of all videos in each channel":
    query9='''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name'''
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channelname","averageduration"])

    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
    df1=pd.DataFrame(T9)
    st.write(df1)

elif question=="10.videos with highest number of comments":
    query10='''select title as videotitle, channel_name as channelname,comments as comments from videos where comments is
                not null order by comments desc'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["video title","channel name","comments"])
    st.write(df10)