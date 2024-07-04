from TikTokApi import TikTokApi
import asyncio
import json
import os
import logging
import random

from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table
from sqlalchemy.orm import sessionmaker

from sqlalchemy import update
import app
from datetime import datetime

#ms_token = os.environ.get(
#    "ms_token", None
#)  # set your own ms_token, think it might need to have visited a profile
#database_url = "postgresql://postgres:admin@localhost:5432/dbtiktok"
database_url = "postgresql://fbs:yah7WUy1Oi8G@172.32.253.129:5432/fbs"
engine = create_engine(database_url)

ms_token = "AySjOsUhnRUCms09JiJ47wIqlss6EXPeWjdz2otVANAWFCf52sAiJssicwKW4hFt3gI6XSYVe-bdh73KNszJJMYQBT-QOq_7TFMgWFnJM6inN6ATgMQ5"
ms_token2 = "tg-gfxTNY1vFk8aX074Bx_fBpvBnQ3n0tiXc1CQsYIVVc0vJkVOeRTUrM62hlDcO_87fLtrP7QSw_8UXofYFoQyeiVB-bhFwrvI4kYhygig5KN1wk-zE3Oisnw3xOxCjUBJ3XiVEPbHKW4i3"
ms_token1 = "6ZgMsdFEjgHnPcgKASehIfrCLwYKTVDvQJb-x4g8wo1EW8MY4F27aamjmYrYmsacZowsWEwqFxb-Z92K-jBCqM1rCyK8rr96227LVErJD4fLurIAt8tCUy8dY-_pFAa40aqF17ajISbjZOx7"# get your own ms_token from your cookies on tiktok.com
ms_token3 = "MZtQuTXtPnZeXELRKX0uv4M-JGapPCFpEVkHOkZ4smVPNBmdYnORwcmNn_M0o1NglximmmCnF8opj51CrQOOjTqL9uC8G7dg_YH-jRVFW8HiQE_twxxGMl2DrehBrgeXwUj6_g=="

# class UserInfo:
#     o_data = []
#     vo_data = []
#     source = []
#     source_id=[]
async def user_profile_data(all_users):   
    o_data = []
    async with TikTokApi() as api:      
                # sources = [''.join(user) for user in all_users] 
                # for source in sources:
                #     UserInfo.source = source
                #     print(source)                
                    #u = 'rfaburmese'
        await api.create_sessions(ms_tokens=[ms_token,ms_token1,ms_token2,ms_token3], num_sessions=1, sleep_after=5,headless=False)    
                   
        with app.app.app_context():
            app.db.create_all()
                    
            user = api.user(all_users)
                    #print(user)
            user_data = await user.info() 
            print(user_data)                  

            # r_data = json.dumps(user_data,indent=4)
            # o_data = json.loads(r_data) 

            # source_id = o_data["userInfo"]["user"]["id"]
            # print(source_id)

            # vcounts = o_data["userInfo"]["stats"]["videoCount"]  
            # print(vcounts)

                    ####users videos collect#######
                    # async for video in user.videos(count = 250):  
                    #print(video.as_dict)              
                        # user_videos.append(video.as_dict)                    
                    #### collect users data convert json format #######
                    # v_data = json.dumps(user_videos,indent=4)
                    # UserInfo.vo_data = json.loads(v_data)

            #await insert_video()            
                    #print(vo_data)

# async def insert_video():                
                        # with app.app.app_context():
                        #     app.db.create_all()                                              

                        #     source_id = UserInfo.o_data["userInfo"]["user"]["id"]
                        #     user_title = UserInfo.o_data["shareMeta"]["title"]
                        #     user_nickname = UserInfo.o_data["userInfo"]["user"]["nickname"]
                        #     user_uniqueId = UserInfo.o_data["userInfo"]["user"]["uniqueId"]
                        #     user_relation = UserInfo.o_data["userInfo"]["user"]["relation"]
                        #     user_diggcount = UserInfo.o_data["userInfo"]["stats"]["diggCount"]
                        #     user_followercount = UserInfo.o_data["userInfo"]["stats"]["followerCount"]
                        #     user_followingcount =  UserInfo.o_data["userInfo"]["stats"]["followingCount"]
                        #     user_friendcount = UserInfo.o_data["userInfo"]["stats"]["friendCount"]
                        #     user_heart = UserInfo.o_data["userInfo"]["stats"]["heart"]
                        #     # user_heartCount = UserInfo.o_data["userInfo"]["stats"]["heartCount"]
                        #     user_videocount = UserInfo.o_data["userInfo"]["stats"]["videoCount"] 
                        #     user_url = 'https://www.tiktok.com/@{}'.format(user_uniqueId)                           

                        #     users = app.TikTokUsersInfo(
                        #         source_id = source_id, 
                        #         user_title = user_title,
                        #         user_nickname = user_nickname,
                        #         user_uniqueId = user_uniqueId,
                        #         user_relation = user_relation,
                        #         user_diggcount = user_diggcount,
                        #         user_followercount = user_followercount,
                        #         user_followingcount = user_followingcount,
                        #         user_friendcount = user_friendcount,
                        #         user_heart = user_heart,
                        #         # user_heartCount = user_heartCount,
                        #         user_videocount = user_videocount,
                        #         user_url = user_url)                    

                            ###update user####
                            #uId = source_id
                            #cId = app.TikTokUsersInfo.source_id
                            #users_update = app.TikTokUsersInfo.source_id == source_id
                            #print(cId)
                            # result = app.db.session.query(app.TikTokUsersInfo).filter(app.TikTokUsersInfo.source_id == UserInfo.source_id).first()
                            # print(result)
                            # print(source_id)
                            # #result_video = app.db.session.query(app.TikTokVideosInfo).filter(app.TikTokVideosInfo.video_id == video_id).first()
                            #     #print(result_video)

                            # if result is None:
                            #     app.db.session.add(users)
                            #     await asyncio.sleep(3)
                            #     app.db.session.commit()
                            #     #print("user videocount is added{}".format(user_videocount))
                            #     print("user data source {} added successful.".format(user_uniqueId))
                                
                                # try:
                                #     app.db.session.execute(user_data_update)
                                #     app.db.session.commit()
                                #     print("user data source is updated successful.")
                                    
                                # except Exception as e:
                                #     print(f"Error updating data: {e}")
                                #     app.db.session.rollback() 
                                # try:
                                #     app.db.session.add(users)
                                #     app.db.session.commit()
                                #     print("user data source is added successful.")
                                # except Exception as e:
                                #     print(f"Error updating data: {e}")
                                #     app.db.session.rollback()
                                #print(result.source_id)
                            # else:
                            #     user_data_update = update(app.TikTokUsersInfo).where(source_id  == source_id).values(
                            #         source_id = source_id, 
                            #         user_title = user_title,
                            #         user_nickname = user_nickname,
                            #         user_uniqueId = user_uniqueId,
                            #         user_relation = user_relation,
                            #         user_diggcount = user_diggcount,
                            #         user_followercount = user_followercount,
                            #         user_followingcount = user_followingcount,
                            #         user_friendcount = user_friendcount,
                            #         user_heart = user_heart,
                            #         # user_heartCount = user_heartCount,
                            #         user_videocount = user_videocount,
                            #         user_url = user_url)  

                            #     print(result.source_id)                
                            #     app.db.session.execute(user_data_update)
                            #     app.db.session.commit()
                            #         #print("user videocount is updated{}".format(user_videocount))
                            #     print("user data source {} updated successful.".format(user_uniqueId))
                                # try:
                                #     app.db.session.add(users)
                                #     await asyncio.sleep(5)
                                #     app.db.session.commit()
                                #     print("user data source is added successful.")
                                    
                                # except Exception as e:
                                #     print(f"Error updating data: {e}")
                                #     app.db.session.rollback()                                            

                                                                                 
if __name__ == "__main__":
        # asyncio.run(UserInfo.user_profile_data(all_users="shwemm314"))
        metadata = MetaData()
        users = Table('tbl_tk_sources', metadata, autoload_with=engine)
        Session = sessionmaker(bind=engine)
        session = Session()
                        
        all_users = session.query(users).with_entities(app.TikTokSources.source_name).all()
        rand_source = random.sample(all_users,3)
        sources = [''.join(user) for user in rand_source] 
        print(sources)
        
        sources = ["elevenmedia"] #yangonmediagroup #1108 #elevenmedia

        loop = asyncio.get_event_loop()
        loop.run_until_complete((user_profile_data(sources)))

        loop.close()
        