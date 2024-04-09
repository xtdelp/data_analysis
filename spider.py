import requests
from lxml import etree
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 '
                  'Safari/537.36',
}

# for i in range(1977, 2024):
#     url = 'https://www.boxofficemojo.com/year/{}/?grossesOption=totalGrosses'
#     response = requests.get(url, headers)
#     movie_dict = {
#         "title": etree.xpath('//*[@id="table"]/div/table[2]/tbody/tr/td/a'),
#         "gross_bo": etree.xpath('//*[@id="table"]/div/table[2]/tbody/tr/td[6]'),
#         "open_week": etree.xpath('//*[@id="table"]/div/table[2]/tbody/tr/td[11]'),
#         "Distributor": etree.xpath('//*[@id="table"]/div/table[2]/tbody/tr/td[13]/a/text()'),
#         "year": i
#     }
def data_req(page, area):
    url = f"https://www.boxofficemojo.com/year/{page}/?area={area}&grossesOption=totalGrosses"
    response = requests.get(url, headers).text
    tree = etree.HTML(response)
    table = tree.xpath('//div[@class="a-section imdb-scroll-table-inner"]//tr[position()>1]')
    print(table)
    for element in table:
        movie_dict = {}
        movie_dict["year"] = page
        movie_dict["title"] = element.xpath('.//td[@class="a-text-left mojo-field-type-release mojo-cell-wide"]/a[@class="a-link-normal"]/text()')[0]
        movie_dict["gross_box_office"] = element.xpath('.//td[@class="a-text-right mojo-field-type-money mojo-estimatable"]/text()')[0]
        movie_dict["max_theater"] = element.xpath('.//td[@class="a-text-right mojo-field-type-positive_integer" and position() mod 2 = 1]/text()')[0]
        movie_dict["opening"] = element.xpath('.//td[@class="a-text-right mojo-field-type-money"]/text()')[0]
        movie_dict["open_theater"] = element.xpath('.//td[@class="a-text-right mojo-field-type-positive_integer" and position() mod 2 = 0]/text()')[0]
        movie_dict["open_week"] = element.xpath('.//td[@class="a-text-left mojo-field-type-date a-nowrap" and position() mod 2 = 1]/text()')[0]
        movie_dict["Distributor"] = element.xpath('.//td[@class="a-text-left mojo-field-type-studio"]/a/text()') or "null"
        print(movie_dict)
        collection.insert_one(movie_dict)

collection = MongoClient()["thesis_data"]["[AU]imdb_bo_data"]
pool = ThreadPoolExecutor(max_workers=10)
for i in range(2000, 2024):
    pool.submit(data_req, i, "AU")
