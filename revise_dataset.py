from concurrent.futures.thread import ThreadPoolExecutor

import pandas as pd
import pymongo
import threading

import requests
from lxml import etree


class Revise():
    def __init__(self):
        self.table = pd.read_excel("./data_file/movie_detail_imdb.xlsx")
        self.row_dicts = self.table.to_dict(orient="records")
        self.collection = pymongo.MongoClient()["thesis_data"]["detail(revise_lang)"]
        self.tlock = threading.Lock()
        self.failed_lists = []
        self.success_list = []
        self.pool = ThreadPoolExecutor(max_workers=30)
        self.lock = threading.Lock()

    def find_req(self, keyword):
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,ja;q=0.7",
            "referer": "https://www.imdb.com/find/?q=%C3%83%C2%86on%20Flux&ref_=nv_sr_sm",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        }
        url = "https://www.imdb.com/find/"
        params = {
            "q": keyword,
            "ref_": "nv_sr_sm"
        }
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            print(response.status_code)
            tree = etree.HTML(response.text)
            detail_href = "https://www.imdb.com/" + tree.xpath(
                '//li[@class="ipc-metadata-list-summary-item ipc-metadata-list-summary-item--click find-result-item find-title-result"][1]//a/@href')[
                0]
            print(detail_href)
            print("已定位到数据位置，正在进入详情页......")
            self.detail_req(keyword, detail_href)
        else:
            print("数据定位失败！！！")
            self.lock.acquire()
            self.failed_lists.append(keyword)
            self.lock.release()

    # ----------------------------------------第二次请求详情页---------------------------------------------
    def detail_req(self, keyword, url):
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,ja;q=0.7",
            "referer": f"https://www.imdb.com/find/?q={keyword}&ref_=nv_sr_sm",
            "sec-ch-ua": "\"Google Chrome\";v=\"123\", \"Not:A-Brand\";v=\"8\", \"Chromium\";v=\"123\"",
            "sec-fetch-site": "same-origin",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        }
        # url = "https://www.imdb.com//title/tt0402022/"
        # params = {
        #     "ref_": "fn_al_tt_1"
        # }

        detail_response = requests.get(url, headers=headers)
        if detail_response.status_code == 200:
            print("当前请求成功，正在获取详情页的内容......")
            try:
                detail_tree = etree.HTML(detail_response.text)
                detail_dict = {}
                detail_dict["title"] = keyword
                detail_dict["rating"] = (detail_tree.xpath(
                    '//div[@data-testid="hero-rating-bar__aggregate-rating__score"]/span[1]/text()') or ["null"])[0]
                detail_dict["rating_number"] = \
                (detail_tree.xpath('//span[@class="ipc-btn__text"]/div/div[2]/div[3]/text()') or ["null"])[0]
                detail_dict["Director"] = \
                (detail_tree.xpath('//section[@data-testid="title-cast"]/ul/li[1]//a/text()') or ["null"])[0]
                detail_dict["lang"] = (detail_tree.xpath('//li[@data-testid="title-details-languages"]//a/text()') or ["null"])[0]
                detail_dict["budget"] = (detail_tree.xpath('//div[@data-testid="title-boxoffice-section"]/ul/li[1]//ul/li/span/text()') or ["null"])[0]
                runtime_lists = detail_tree.xpath('//section[@data-testid="TechSpecs"]/div[2]/ul/li[1]/div/text()')
                detail_dict["runtime"] = "".join(runtime_lists)
                print(detail_dict)
                self.collection.insert_one(detail_dict)
                print("详情页内容已插入成功！！！")
                self.lock.acquire()
                self.success_list.append(keyword)
                self.lock.release()
            except Exception as e:
                print("详情页分析失败！！", e)
                self.lock.acquire()
                self.failed_lists.append(keyword)
                self.lock.release()

        else:
            print(keyword, "详情页请求失败，当前状态码为", detail_response.status_code)
            self.lock.acquire()
            self.failed_lists.append(keyword)
            self.lock.release()

    def main(self):
        for mission in self.row_dicts:
            print(mission["lang"])
            if mission["lang"] == "empty":
                print("找到没有lang的数据",mission)
                self.pool.submit(self.find_req, mission["title"])
        self.pool.shutdown(wait=True)
        print("所有线程任务已完成")
        print(self.success_list, self.failed_lists)


if __name__ == "__main__":
    Revise().main()
