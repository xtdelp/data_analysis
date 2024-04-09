# //li[@data-testid="title-details-languages"]//a/text()
import threading
from concurrent.futures.thread import ThreadPoolExecutor

import pymongo
from imdb_detail_crawler import find_req,detail_req

success_list = []
failed_list = []
lock = threading.Lock()
movie_table = pymongo.MongoClient()["thesis_data"]["international_movie_record"]
movie_title_lists = movie_table.distinct("title")
crawled_movie_title = pymongo.MongoClient()["thesis_data"]["movie_detail_imdb"].distinct("title")
failed_movie_lists = list(set(movie_title_lists) - set(crawled_movie_title))
print(len(failed_movie_lists))
collection = pymongo.MongoClient()["thesis_data"]["failed_movie_lists"]
collection.insert_one({"failed_mission": failed_movie_lists})
collected_data = pymongo.MongoClient()["thesis_data"]["movie_detail_imdb"]
print("失败任务已经存入数据库")

pool = ThreadPoolExecutor(max_workers=30)
for mission in failed_movie_lists:
    pool.submit(find_req, mission, collected_data)
pool.shutdown(wait=True)

# 所有任务完成，主线程继续执行
print("所有线程任务已完成")
print(failed_list, success_list)