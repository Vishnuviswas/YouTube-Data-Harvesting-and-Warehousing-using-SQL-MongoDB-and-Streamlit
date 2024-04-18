
from googleapiclient.discovery import build
import pymongo
import pandas as pd
import streamlit as st
def Channels(Channel_id):

    api_key='AIzaSyCWayCvrAx78Dr-hsRE3Huuy8EMNVhHfU0'
    youtube = build('youtube','v3',developerKey=api_key)
    
    request = youtube.channels().list(part='snippet,contentDetails,statistics',id=Channel_id)
    response=request.execute()
    channel_data= {'channel_name':response['items'][0]['snippet']['title'],
    'channel_id':response['items'][0]['id'],
    'channel_sc': response['items'][0]['statistics']['subscriberCount'],
    'channel_vc':response['items'][0]['statistics']['viewCount'],
    'channel_vic':response['items'][0]['statistics']['videoCount'],
    'channel_descn': response['items'][0]['snippet']['description'],
    'playlist_id': response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
     }

    return channel_data


def video_id(Channel_id):
    api_key='AIzaSyCWayCvrAx78Dr-hsRE3Huuy8EMNVhHfU0'
    youtube = build('youtube','v3',developerKey=api_key)
    channel_id=Channel_id
    video_ids=[]
    request = youtube.channels().list(part='snippet,contentDetails,statistics',id=Channel_id)
    response=request.execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page = None
    while True:
        request = youtube.playlistItems().list(part = 'snippet,contentDetails',playlistId=Playlist_Id,
                                                       maxResults=50,pageToken=next_page)
        response = request.execute()
        for item in response['items']:
            video_ids.append(item['contentDetails']['videoId'])
        next_page = response.get('nextPageToken')
        if next_page is None:
            break
    
    
     
    return video_ids


def convert_dur(s):
  l = []
  f = ''
  for i in s:
    if i.isnumeric():
      f = f+i
    else:
      if f:
        l.append(f)
        f = ''
  if 'H' not in s:
    l.insert(0,'00')
  if 'M' not in s:
    l.insert(1,'00')
  if 'S' not in s:
    l.insert(-1,'00')
  return ':'.join(l)
def video_details(v_ids):
    api_key='AIzaSyCWayCvrAx78Dr-hsRE3Huuy8EMNVhHfU0'
    youtube = build('youtube','v3',developerKey=api_key)
    v_data=[]
    for i in v_ids:
        request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id= i)
        response = request.execute()

        duration =response["items"][0]['contentDetails']['duration']
        numerical_duration=convert_dur(duration)
        
        video_data={ 'channel_name':response["items"][0]['snippet']['channelTitle'],
          'channel_id':response["items"][0]['snippet']['channelId'],
                    'video_id':response["items"][0]['id'],
                    'video_tit':response["items"][0]['snippet']['title'],
                    'video_desc':response["items"][0]['snippet']['description'],
                    'video_tag':response["items"][0]['etag'],
                    'video_views':response["items"][0]['statistics']['viewCount'],
                    'comment-count':response["items"][0]['statistics']['commentCount'],
        'publish_data':response['items'][0]['snippet']['publishedAt'],
        'video_like':response["items"][0]['statistics']['likeCount'],
        'duration':convert_dur(response["items"][0]['contentDetails']['duration']),
        'video_fav':response["items"][0]['statistics']['favoriteCount'],
      
            }
        v_data.append(video_data)
    return v_data
       


def comment_dat_get(video_ids):
    comments = []
    
    for i in video_ids:
        try:
            api_key='AIzaSyCWayCvrAx78Dr-hsRE3Huuy8EMNVhHfU0'
            youtube = build('youtube','v3',developerKey=api_key)
            request = youtube.commentThreads().list(part='snippet,replies',videoId=i,maxResults=100)
            response = request.execute()
            if len(response['items'])>0:
                for j in range(len(response['items'])):
                    comments.append({'channel_id':response['items'][j]['snippet']['channelId'],
                        'comment_id': response['items'][j]['snippet']['topLevelComment']['id'],
                        'video_id': i,
                        'Comment_Author': response['items'][j]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        'Comment_Text': response['items'][j]['snippet']['topLevelComment']['snippet']['textOriginal'],
                        'Comment_PublishedAt':response['items'][j]['snippet']['topLevelComment']['snippet']['publishedAt'].replace('Z',''),
                        'Comment_Likes': int(response['items'][j]['snippet']['topLevelComment']['snippet']['likeCount'])
                    })
                    
        except:

            pass
    return comments


conn_str='mongodb://localhost:27017'
try:
    client = pymongo.MongoClient(conn_str)
except Exception:
    print("Error:" + Exception)
myDb =client["Youtube_Channel_Information"]



def main(channel_id):
   
        
   ch_data=Channels(channel_id)
   v_ids=video_id(channel_id)
   print(v_ids)
   video_data=video_details(v_ids) 
   comment_data=comment_dat_get(v_ids)
   colln1=myDb["Channel Details"]
   colln1.insert_one({'channel_data':ch_data,'video_data':video_data,'comment_data':comment_data} )        
   return "UPLOAD SUCCESSFULLY" 


import mysql.connector
My_Sql_Db = mysql.connector.connect(host='localhost', port=3306,user='root',password='Pooja4*mohan',database='Youtube_Channel_Information')
cur=My_Sql_Db.cursor()
try:
    cur.execute('create database Youtube_Channel_Information')
except:
    print("database names already  created")




def Channel_table (channel_name_s):
        My_Sql_Db = mysql.connector.connect(host='localhost', port=3306,user='root',password='Pooja4*mohan',database='Youtube_Channel_Information')
        cur=My_Sql_Db.cursor()

        create_table='''create table if not exists Channel_details(channel_name varchar(100),Channel_id varchar(35) primary key,channel_sc bigint,
                channel_vc bigint,channel_vic bigint, channel_descn text,playlist_id varchar(35)) '''
        cur.execute(create_table)
        My_Sql_Db.commit()
        Channel_data_insert='''insert into Channel_details(channel_name ,Channel_id ,channel_sc ,
        channel_vc ,channel_vic , channel_descn ,playlist_id) values(%s,%s,%s,%s,%s,%s,%s)'''
        Channel_data=[]
        myDb =client['Youtube_Channel_Information']
        colln1=myDb['Channel Details']
        Single_Channel_data=[]
        myDb =client['Youtube_Channel_Information']
        colln1=myDb['Channel Details']
        for ch_data in colln1. find({"channel_data.channel_name":channel_name_s},{"_id":0,"channel_data":1}):
                Single_Channel_data.append(ch_data['channel_data'])
        for i in  Single_Channel_data:
                    value=tuple(i.values())
                    try:
                        cur.execute(Channel_data_insert,value)
                        My_Sql_Db.commit()
                    except:
                        news=f"User entered channel name{channel_name_s} are already exist"
                



# create video table
def video_table(channel_name_s):
    My_Sql_Db = mysql.connector.connect(host='localhost', port=3306,user='root',password='Pooja4*mohan',database='Youtube_Channel_Information')
    cur=My_Sql_Db.cursor() 

    create_video_table='''

    CREATE TABLE if not exists video_details (channel_name varchar(50),
        channel_id varchar(50),
        video_id varchar(50),
        video_title text,
        video_desc text,
        video_tag varchar(50),
        video_views bigint,
        comment_count bigint,
        publish_date datetime,
        video_like bigint,
        duration VARCHAR(30),
        video_fav bigint
    );'''
    cur.execute(create_video_table)
    My_Sql_Db.commit()
    #insert values into video table
    insert_video_data = '''INSERT INTO video_details (channel_name,channel_id,
        video_id, video_title, video_desc, video_tag,video_views,
        comment_count, publish_date, video_like,duration, video_fav
    ) VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s);'''
    single_video_data=[]
    myDb=client['Youtube_Channel_Information']
    colln1=myDb['Channel Details']
    for video_data in colln1.find({'video_data.channel_name':channel_name_s},{"_id":0}):
        for i in range(len(video_data["video_data"])):
            single_video_data.append(video_data["video_data"][i])
                
    for i in single_video_data:
        values = (
            i['channel_name'],
            i['channel_id'],
            i['video_id'],
            i['video_tit'],
            i['video_desc'],
            i['video_tag'],
            i['video_views'],
            i['comment-count'],
            i['publish_data'][:-1],
            i['video_like'],
            i['duration'],
            i['video_fav']
        )

        cur.execute(insert_video_data, values)

        My_Sql_Db.commit()   

#create comment table
def comment_table(channel_name_s):
    My_Sql_Db = mysql.connector.connect(host='localhost', port=3306,user='root',password='Pooja4*mohan',database='Youtube_Channel_Information')
    cur=My_Sql_Db.cursor()
    
    create_comment_table='''CREATE TABLE if not exists comment_details(channel_id varchar(30),comment_id text,
                    video_id varchar(30),
                    Comment_Author varchar(30),
                    Comment_Text text,
                    Comment_PublishedAt varchar(30),
                    Comment_Likes bigint)''' 
    cur.execute(create_comment_table)
    My_Sql_Db.commit()
    #insert comment data into comment table
    insert_comment_data='''insert into comment_details(channel_id,comment_id ,
                    video_id ,
                    Comment_Author,
                    Comment_Text ,
                    Comment_PublishedAt ,
                    Comment_Likes)
                    values(%s,%s,%s,%s,%s,%s,%s)'''
    single_comment_details=[]
    myDb=client['Youtube_Channel_Information']
    colln1=myDb['Channel Details']
    for comment_data in colln1.find({'channel_data.channel_name':channel_name_s},{"_id":0}):
            single_comment_details.append(comment_data['comment_data']) 
    for i in single_comment_details:
        value=tuple(single_comment_details[0][0].values())
        cur.execute(insert_comment_data,value)
        My_Sql_Db.commit()
    
            

def tables(channel_name_s):
    Channel_table(channel_name_s)
    video_table(channel_name_s)
    comment_table(channel_name_s)
    return "Tables creates succesfully"




#streamlit part
import streamlit as st
with st.sidebar:
    st.title(":white[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    
channel_id=st.text_input("Enter the channel ID")
if st.button("collect and store data"):
    v_ids=[]
    myDb =client['Youtube_Channel_Information']
    colln1=myDb['Channel Details']
    for ch_data in colln1. find({},{"_id":0,"channel_data":1}):
        v_ids.append(ch_data['channel_data']["channel_id"])
    if channel_id in v_ids:
            st.success("Channel details of the given channel id already exists")

    else:
            insert =main(channel_id)
            st.success(insert)
Channel_names_list=[]
myDb =client['Youtube_Channel_Information']
colln1=myDb['Channel Details']
for ch_data in colln1. find({},{"_id":0,"channel_data":1}):
         Channel_names_list.append(ch_data['channel_data']['channel_name'])
unique_channel_name=st.selectbox('select the channel',(Channel_names_list))

if st.button("Migrate to SQL"):
    Tables=tables(unique_channel_name)

    st.success(Tables)
    

         
    #  Sql connection
import mysql.connector
import streamlit as st
My_Sql_Db = mysql.connector.connect(host='localhost', port=3306,user='root',password='Pooja4*mohan',database='Youtube_Channel_Information')
cur=My_Sql_Db.cursor()

questions=st.selectbox("select your question",("1.All the videos and the channel name",
                                            "2.channels with most number of  videos",
                                            "3.10 most viewed videos",
                                            "4.comment in each videos",
                                            "5.videos with highest likes",
                                            "6.likes of all videos",
                                            "7.views of each channel",
                                            "8.videos published in the year of 2022",
                                            "9.average duration of all videos in each channel",
                                            "10.videos with highest number of comments"))


if questions=="1.All the videos and the channel name":
    query1='''select channel_details.channel_name as channelname,video_details.video_title as videotitles from channel_details
            inner join video_details on channel_details.channel_id=video_details.channel_id;'''
    cur.execute(query1)
    t1=cur.fetchall()
    df1=pd.DataFrame(t1,columns=["videotitle","channelname"])
    st.write(df1)
elif questions=="2.channels with most number of  videos":
    query2='''select channel_name as channelname,channel_vic as no_videos from channel_details
            order by channel_vic desc limit 10;'''
    cur.execute(query2)
    t2=cur.fetchall()
    df2=pd.DataFrame(t2,columns=["channelname","no_videos"])
    st.write(df2)
elif questions=="3.10 most viewed videos":
    query3='''SELECT channel_details.channel_name AS channelname ,video_details.video_views AS viewedvideos,
                video_details.video_title AS videotitles  FROM channel_details INNER JOIN video_details 
            ON channel_details.channel_id = video_details.channel_id;'''
    cur.execute(query3)
    t3=cur.fetchall()
    df3=pd.DataFrame(t3,columns=["viewedvideos","channelname","videotitles"])
    st.write(df3)
elif questions=="4.comment in each videos":
    query4='''select comment_count as Totalcomments,video_title as videoname from video_details;'''
    cur.execute(query4)
    t4=cur.fetchall()
    df4=pd.DataFrame(t4,columns=["Totalcomments","videoname"])
    st.write(df4)
elif questions=="5.videos with highest likes":
    query5='''select video_details.video_like as Totalvideolikes,video_details.video_title,channel_details.channel_name as channelname from channel_details
                inner join  video_details on channel_details.channel_id=video_details.channel_id;'''
    cur.execute(query5)
    t5=cur.fetchall()
    df5=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
    st.write(df5)
elif questions=="6.likes of all videos":
    query6='''select video_like as Totalvideolikes,video_title as videonames from video_details order by video_like desc limit 10;'''
    cur.execute(query6)
    t6=cur.fetchall()
    df6=pd.DataFrame(t6,columns=["Totalvideolikes","videonames"])
    st.write(df6)
elif questions=="7.views of each channel":
    query7='''select channel_vc as Total_no_of_views,channel_name as channelname from channel_details;   '''
    cur.execute(query7)
    t7=cur.fetchall()
    df7=pd.DataFrame(t7,columns=["Total_no_of_views","channelname"])
    st.write(df7)
elif questions=="8.videos published in the year of 2022":
    query8='''SELECT channel_details.channel_name AS channelname, video_details.publish_date, video_details.video_title 
                FROM channel_details INNER JOIN video_details ON channel_details.channel_id = video_details.channel_id 
                    WHERE EXTRACT(YEAR FROM video_details.publish_date) = 2020;'''
    cur.execute(query8)
    
    t8=cur.fetchall()
    df8=pd.DataFrame(t8,columns=["video title","video release","channelname"])
    st.write(df8)
elif questions=="9.average duration of all videos in each channel":
    query9='''SELECT channel_details.channel_name AS channelname, AVG(video_details.duration) AS averageduration 
              FROM channel_details INNER JOIN video_details ON channel_details.channel_id = video_details.channel_id 
              GROUP BY channel_details.channel_name ORDER BY averageduration DESC LIMIT 10;'''

    cur.execute(query9)
    t9=cur.fetchall()
    df9=pd.DataFrame(t9,columns=["channelname","averageduration"])
    st.write(df9)
elif questions=="10.videos with highest number of comments":
    query10='''SELECT video_details.video_title AS videonames,video_details.comment_count AS Total_no_of_comment,
            channel_details.channel_name AS channelname FROM video_details INNER JOIN channel_details
              ON channel_details.channel_id = video_details.channel_id order by video_title asc limit 30;'''
    cur.execute(query10)
    t10=cur.fetchall()
    df10=pd.DataFrame(t10,columns=["videonames ","Total_no_of_comment","channelname"])
    st.write(df10)

        

        