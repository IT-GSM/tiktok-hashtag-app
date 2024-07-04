from TikTokApi import TikTokApi
import asyncio
import os
import app
import json
from sqlalchemy import update
from datetime import datetime
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
import random


ms_token = "w0YaBzxVYAiVs4uWHi5DzjEcfWIQ96YS7F7JajFTAqz6xztmvhRZnplBtcCYmHcN4TUWBpjFlw11nuO8oFk1KkEQnvMQ-UgmbMitlwuPsl8ghz4_1zJ47lZh7k8H"
ms_token2 = "tg-gfxTNY1vFk8aX074Bx_fBpvBnQ3n0tiXc1CQsYIVVc0vJkVOeRTUrM62hlDcO_87fLtrP7QSw_8UXofYFoQyeiVB-bhFwrvI4kYhygig5KN1wk-zE3Oisnw3xOxCjUBJ3XiVEPbHKW4i3"
ms_token1 = "HB08JRnY9yq0ZOJNvaxxC_moJSoheE7wfRm1w-dOfiFruZnlkpgGFGSTMX_tHxLZDKS6WRekU4XGZULn3sAPhWzdu0liyU7k-StDAmQeQyzoC2TQLAHAT2tfQJnS"

#database_url = "postgresql://postgres:admin@localhost:5432/dbtiktok"
database_url = "postgresql://fbs:yah7WUy1Oi8G@192.168.11.202:5432/fbs"
engine = create_engine(database_url)

class HashInfo:  
    hashkey = []
    async def get_hashtag_videos(hashtag_name):       
        hash_out_data = []
        async with TikTokApi() as api:  
            sources = [''.join(hashname) for hashname in hashtag_name] 
            for source in sources:
                HashInfo.hashkey = source
                # print(HashInfo.hashkey)                   

                await api.create_sessions(ms_tokens=[ms_token,ms_token1,ms_token2], num_sessions=1, sleep_after=3,headless=False)
                        # trending = api.hashtag(name=hashtag)
                tag = api.hashtag(name=HashInfo.hashkey)
                hashtag_data = []
                async for video in tag.videos(count=50):

                    hashtag_data.append(video.as_dict)
                    hash_data = json.dumps(hashtag_data,indent=4)
                    hash_out_data = json.loads(hash_data)
                    # print(video)
                    # print(hash_out_data)

                    with app.app.app_context():
                        app.db.create_all()

                        for hashtag in hash_out_data:
                            #video_id = video_id 
                            hash_name = hashtag_name                             
                            hash_video_id = hashtag['id']                
                                #hash_video_url = comments['create_time']
                            hash_video_createTime = hashtag['createTime']
                            hash_video_duration = hashtag['video']['duration']
                            hash_contents_desc = hashtag['desc']
                                
                            author_id = hashtag['author']['id']
                            author_nickname = hashtag['author']['nickname']
                            author_uniqueId = hashtag['author']['uniqueId']
                            author_diggCount = hashtag['authorStats']['diggCount']
                            author_followerCount = hashtag['authorStats']['followerCount']
                            author_followingCount = hashtag['authorStats']['followingCount']
                            author_friendCount = hashtag['authorStats']['friendCount']
                            author_heartCount = hashtag['authorStats']['heart']
                            author_heart = hashtag['authorStats']['heartCount']
                            author_videoCount = hashtag['authorStats']['videoCount']

                            stats_collectCount = hashtag['stats']['collectCount']
                            stats_commentCount = hashtag['stats']['commentCount']
                            stats_diggCount = hashtag['stats']['diggCount']
                            stats_playCount = hashtag['stats']['playCount']
                            stats_shareCount = hashtag['stats']['shareCount']  

                            user_url = 'https://www.tiktok.com/@{}'.format(author_uniqueId),

                            hash_video_url = 'https://www.tiktok.com/@{}/video/{}'.format(author_nickname,hash_video_id),

                            # print(hash_name,hash_video_id,hash_video_createTime,hash_video_duration,
                            #       hash_contents_desc,author_id,author_nickname,author_uniqueId,author_diggCount,author_followerCount,
                            #       author_followingCount,author_friendCount,author_heartCount,author_heart,author_videoCount,
                            #       stats_collectCount,stats_commentCount,stats_diggCount,stats_playCount,stats_shareCount,hash_video_url)
                            # Perform the query
                            results = app.db.session.query(app.TikTokHashKey.id).filter(app.TikTokHashKey.hash_name == HashInfo.hashkey).all()

                            ids = [row.id for row in results]
                            # Ensure there is at least one ID in the list
                            if ids:
                                hash_id = int(ids[0])  # Convert the first ID to an integer
                                # print(hash_id)
                            else:
                                print("No IDs found")

                            hashtag_video = app.TikTokVideosInfo(                        
                                    # hash_name = hashtag_name,
                                    s_id = hash_id,
                                    video_id = hash_video_id,
                                    source_id = author_id,
                                    video_createtime = datetime.utcfromtimestamp(hash_video_createTime),
                                    video_description = hash_contents_desc,
                                    video_url = hash_video_url,
                                    video_author = author_nickname,
                                    video_duration = hash_video_duration,

                                    # hash_video_url = hash_video_url,
                                    # hash_video_createTime = datetime.utcfromtimestamp(hash_video_createTime),
                                    # hash_video_duration = hash_video_duration,
                                    # hash_contents_desc = hash_contents_desc,
                                    
                                    # author_id = author_id,
                                    # author_nickname = author_nickname,
                                    # author_uniqueId = author_uniqueId,
                                    # author_diggCount = author_diggCount,
                                    # author_followerCount = author_followerCount,
                                    # author_followingCount = author_followingCount,
                                    # author_friendCount = author_friendCount,
                                    # author_heartCount = author_heartCount,
                                    # author_heart = author_heart,
                                    # author_videoCount = author_videoCount,

                                    video_collectcount = stats_collectCount,
                                    video_commentcount = stats_commentCount,
                                    video_diggcount = stats_diggCount,
                                    video_playcount = stats_playCount,
                                    video_sharecount = stats_shareCount

                                    # stats_collectCount = stats_collectCount,
                                    # stats_commentCount = stats_commentCount,
                                    # stats_diggCount = stats_diggCount,
                                    # stats_playCount = stats_playCount,
                                    # stats_shareCount = stats_shareCount
                                ) 
                            # print(hashtag_video)               
                            # app.db.session.add(hashtag_video)
                            # print("Added hashtag {}".format(hash_video_id))
                            # app.db.session.commit()
                            
                            check_video = app.db.session.query(app.TikTokVideosInfo).filter(app.TikTokVideosInfo.video_id == hash_video_id).first()

                            if check_video is None:
                                try:
                                    app.db.session.add(hashtag_video)
                                    await asyncio.sleep(3) 
                                    app.db.session.commit()
                                    print("Added hashtag source is {} , this content id is {}".format(HashInfo.hashkey,hash_video_id))

                                    #content_id collect from table tiktokvideo info
                                    content = app.db.session.query(app.TikTokVideosInfo.id).filter(app.TikTokVideosInfo.video_id == hash_video_id).all()

                                    # Extracting the id values from the result
                                    ids = [row.id for row in content]                       

                                    # Reflect the  table from the database
                                    content_table = Table('all_content', metadata, autoload_with=engine)

                                    # Access the columns of the "content" table
                                    columns = content_table.columns.keys()

                                    # Print the column names
                                    content_column = columns[1]
                                    network_column = columns[2]
                                    # print("content_column {},network_column {}".format(ids,5))

                                    # Define the values to insert
                                    values_to_insert = [
                                        {content_column: content_id, network_column: 5} for content_id in ids
                                    ]

                                    # Create an insert statement
                                    insert_allcontent = content_table.insert().values(values_to_insert)

                                    # Execute the insert statement
                                    app.db.session.execute(insert_allcontent)
                                    print("Added content id values : {} for network id 5".format(ids))

                                    # app.db.session.close()                            
                                except Exception as e:
                                    print(f"Error updating data: {e}")
                                    app.db.session.rollback()
                            else:
                                    update_hashtag_video = update(app.TikTokVideosInfo).where(app.TikTokVideosInfo.video_id  == hash_video_id).values(
                                    s_id = hash_id,
                                    video_id = hash_video_id,
                                    source_id = author_id,
                                    video_createtime = datetime.utcfromtimestamp(hash_video_createTime),
                                    video_description = hash_contents_desc,
                                    video_url = hash_video_url,
                                    video_author = author_nickname,
                                    video_duration = hash_video_duration,
                                    
                                    video_collectcount = stats_collectCount,
                                    video_commentcount = stats_commentCount,
                                    video_diggcount = stats_diggCount,
                                    video_playcount = stats_playCount,
                                    video_sharecount = stats_shareCount
                                )                                    
                                    app.db.session.execute(update_hashtag_video)         
                                    await asyncio.sleep(3)                 
                                    app.db.session.commit()

                                    print("Updated hashtag source is {} , this content id is {}".format(HashInfo.hashkey,hash_video_id))                            

                                    # for column_name in columns:
                                    #      print(column_name)
                                    #      column_name[1] = ids
                                    #     print(column_name)

                                    # Retrieve all rows from the content table
                                    # all_content = session.query(content_table).all()

                                    # for ids, row in enumerate(all_content):
                                    #     row.content_id = ids
                                    #     print(row.content_id)

                                    # Commit the changes to the database
                                    app.db.session.close()
                            
                            # else:
                            #         app.db.session.add(hashtag_video)
                            #         await asyncio.sleep(3) 
                            #         app.db.session.commit()
                            #         print("Added hashtag {}".format(hash_video_id))

                            hashtag_info = app.TikTokUsersInfo(                        
                                    # hash_name = hashtag_name,
                                    # hash_video_id = hash_video_id,
                                    # hash_video_url = hash_video_url,
                                    # hash_video_createTime = datetime.utcfromtimestamp(hash_video_createTime),
                                    # hash_video_duration = hash_video_duration,
                                    # hash_contents_desc = hash_contents_desc,
                                    
                                    source_id = author_id,
                                    user_nickname = author_nickname,
                                    user_uniqueId = author_uniqueId,
                                    user_diggcount = author_diggCount,
                                    user_followercount = author_followerCount,
                                    user_followingcount = author_followingCount,
                                    user_friendcount = author_friendCount,
                                    user_heart = author_heartCount,
                                    # author_heart = author_heart,
                                    user_videocount = author_videoCount,
                                    user_url = user_url,

                                    # stats_collectCount = stats_collectCount,
                                    # stats_commentCount = stats_commentCount,
                                    # stats_diggCount = stats_diggCount,
                                    # stats_playCount = stats_playCount,
                                    # stats_shareCount = stats_shareCount
                                )

                            # app.db.session.add(hashtag_info)
                            # print("Added hashtag {}".format(author_id))
                            # app.db.session.commit()

                            check = app.db.session.query(app.TikTokUsersInfo).filter(app.TikTokUsersInfo.source_id == author_id).first()                
                                            
                            if check:
                                    update_hashtag_info = update(app.TikTokUsersInfo).where(app.TikTokUsersInfo.source_id  == author_id).values(
                                    # hash_name = hash_name,
                                    # hash_video_id = hash_video_id,
                                    # hash_video_url = hash_video_url,
                                    # hash_video_createTime = datetime.utcfromtimestamp(hash_video_createTime),
                                    # hash_video_duration = hash_video_duration,
                                    # hash_contents_desc = hash_contents_desc,
                                    
                                    source_id = author_id,
                                    user_nickname = author_nickname,
                                    user_uniqueId = author_uniqueId,
                                    user_diggcount = author_diggCount,
                                    user_followercount = author_followerCount,
                                    user_followingcount = author_followingCount,
                                    user_friendcount = author_friendCount,
                                    user_heart = author_heartCount,
                                    # author_heart = author_heart,
                                    user_videocount = author_videoCount,
                                    user_url = user_url,
                                    
                                    # stats_collectCount = stats_collectCount,
                                    # stats_commentCount = stats_commentCount,
                                    # stats_diggCount = stats_diggCount,
                                    # stats_playCount = stats_playCount,
                                    # stats_shareCount = stats_shareCount
                                )                                    
                                    app.db.session.execute(update_hashtag_info) 
                                    await asyncio.sleep(3)   
                                    app.db.session.commit() 
                                    app.db.session.close()     
                                    print("Updated hashtag {} , content create user name is {}".format(hashtag_name, str(author_nickname)))                  
                                    
                            else:
                                    app.db.session.add(hashtag_info)
                                    await asyncio.sleep(3)   
                                    app.db.session.commit()
                                    app.db.session.close()
                                    print("Added hashtag {} , content create user name is {}".format(hashtag_name, str(author_nickname)))
                        

if __name__ == "__main__":
    # asyncio.run(get_hashtag_videos(hashtag_name="စစ်မှုထမ်းဥပဒေ"))

    metadata = MetaData()
    users = Table('tbl_tk_hashtag_sources', metadata, autoload_with=engine)
    Session = sessionmaker(bind=engine)
    session = Session()
                        
    all_hashtag = session.query(users).with_entities(app.TikTokHashKey.hash_name).all()
    rand_hash = random.sample(all_hashtag,5)
    sources = [''.join(user) for user in rand_hash] 
    print(sources)
    # if sources:  # Check if the list is not empty
    #     print(sources[-1])  # Print the last index value
    # else:
    #     print("The list 'sources' is empty.")
    
    #sources = ["ဝင်းနိမ္မိတာရုံ"]
    source_name = input("Enter hash name : ")
    sources = [source_name]

    loop = asyncio.get_event_loop()
    loop.run_until_complete((HashInfo.get_hashtag_videos(sources)))

    loop.close()