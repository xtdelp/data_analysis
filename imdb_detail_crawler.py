import requests
from lxml import etree
from urllib.parse import quote
import pandas as pd
import pymongo
import threading
from concurrent.futures import ThreadPoolExecutor

collection = pymongo.MongoClient()["thesis_data"]["movie_detail_imdb"]
success_list = []
failed_list = []
lock = threading.Lock()


def find_req(keyword, collection):
    keyword = keyword
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,ja;q=0.7",
        "referer": "https://www.imdb.com/find/?q=%C3%83%C2%86on%20Flux&ref_=nv_sr_sm",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    }

    # cookies = {
    #     "uu": "eyJpZCI6InV1ZTM3ZGYwZDA0MGU3NDE2YmI3N2IiLCJwcmVmZXJlbmNlcyI6eyJmaW5kX2luY2x1ZGVfYWR1bHQiOmZhbHNlfX0=",
    #     "session-id": "145-9356530-9478412",
    #     "ci": "e30",
    #     "_gcl_au": "1.1.131213338.1712471933",
    #     "ubid-main": "131-8206444-0348747",
    #     "session-id-time": "2082787201l",
    #     "ad-oo": "0",
    #     "__gads": "ID=1fed32f164e57714:T=1712474297:RT=1712474297:S=ALNI_MYOrwCSXpRByc85gl0E4RuN6Kz8Dw",
    #     "__gpi": "UID=00000de363637a23:T=1712474297:RT=1712474297:S=ALNI_MbT7axo7AyMTMDt6q01sNcdKWm22w",
    #     "__eoi": "ID=30f8f40e2401dc41:T=1712474297:RT=1712474297:S=AA-AfjbWG0N9h638fCZvK8ZmlFma",
    #     "_cc_id": "809f1a6d4ac60c82f4446b4da5c4bd86",
    #     "panoramaId_expiry": "1713079099650",
    #     "panoramaId": "95f7b5e57147ca61dc763c6446d4185ca02c6b1ff800929bf2c99798f8dc8995",
    #     "panoramaIdType": "panoDevice",
    #     "session-token": "f0H5W/yNM+7jbycxyZz4BX+a5q4QaetOw5mrMLuInqOednqljuo2waXbOM/N3p7XB+Famp7jzj3UFfmcoODknhcIkaIRiUmijnmSaGP0OGtIdlDTnJD1/XcbXLlSAqnxua5cQ4jhoqsjPEfLdHThdxCBF/O0Zeu4sv4NujdK5CnIIuhAU6vozOVRBN8poYZJ6jrtiPXpTT9h5JsM2yVuSuxx2a2Q5CVWfZssin5/+9YjiFpTfu+QPLbCUD4S/zrnDKLDPPnqWsGtkTP9Vw5Ay29xBSUbggL6/dlpuj84PzQUuQgA0vW1qKwukxDAnUSvdmB5FORZTAqVjS7iGHKmgVWOMmdS3N8l",
    #     "csm-hit": "adb:adblk_no&t:1712562038290&tb:EKX67DJ7VWMD5F67JYVJ+s-EKX67DJ7VWMD5F67JYVJ|1712562038289"
    # }
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
        detail_req(keyword, detail_href, collection)
    else:
        print("数据定位失败！！！")
        lock.acquire()
        failed_list.append(keyword)
        lock.release()




# ----------------------------------------第二次请求详情页---------------------------------------------
def detail_req(keyword, url, collection):
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
            detail_dict["rating"] = (detail_tree.xpath('//div[@data-testid="hero-rating-bar__aggregate-rating__score"]/span[1]/text()') or ["null"])[0]
            detail_dict["rating_number"] = (detail_tree.xpath('//span[@class="ipc-btn__text"]/div/div[2]/div[3]/text()') or ["null"])[0]
            detail_dict["Director"] = (detail_tree.xpath('//section[@data-testid="title-cast"]/ul/li[1]//a/text()') or ["null"])[0]
            detail_dict["lang"] = (detail_tree.xpath('//li[4]/div/ul/li/a/text()') or ["null"])[0]
            detail_dict["budget"] = (detail_tree.xpath('//div[2]/ul/li[1]/div/ul/li/span/text()') or ["null"])[0]
            runtime_lists = detail_tree.xpath('//section[@data-testid="TechSpecs"]/div[2]/ul/li[1]/div/text()')
            detail_dict["runtime"] = "".join(runtime_lists)
            print(detail_dict)
            collection.insert_one(detail_dict)
            print("详情页内容已插入成功！！！")
            lock.acquire()
            success_list.append(keyword)
            lock.release()
        except Exception as e:
            print("详情页分析失败！！", e)
            lock.acquire()
            failed_list.append(keyword)
            lock.release()

    else:
        print(keyword, "详情页请求失败，当前状态码为", detail_response.status_code)
        lock.acquire()
        failed_list.append(keyword)
        lock.release()


if __name__ == "__main__":
    movie_table = pymongo.MongoClient()["thesis_data"]["international_movie_record"]
    movie_title_lists = movie_table.distinct("title")
    pool = ThreadPoolExecutor(max_workers=10)
    for movie_title in movie_title_lists:
        pool.submit(find_req, movie_title, collection)
