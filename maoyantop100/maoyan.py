import requests
from lxml import etree
from multiprocessing.dummy import Pool
import csv
import pymongo

class MaoyanSpider(object):

    def __init__(self):
        client = pymongo.MongoClient()
        database = client['maoyan']
        self.col = database['top100']
        self.item_list = []

    def run(self, url):
        '''
        构建调用函数，方便使用多线程
        :param url:
        :return:
        '''
        html = self.get_source(url)
        self.get_content(html)

    def get_source(self, url):

        '''获取网页的源代码
        :param url:
        :return: string
        '''
        head = {'user-agent': 'Mozilla/5.0'}
        html = requests.get(url, headers=head)
        html.raise_for_status()
        html.encoding = html.apparent_encoding
        return html.text


    def get_content(self, html):
        '''
        爬取每一部电影的信息
        :param html:
        :return: list
        '''
        selector = etree.HTML(html)
        movies_list = selector.xpath('//dl[@class = "board-wrapper"]/dd')
        for movie in movies_list:
            item = {}
            item['title'] = movie.xpath('div[@class = "board-item-main"]/div/div[@class = "movie-item-info"]/p[@class = "name"]/a/text()')[0]
            id = movie.xpath('div[@class = "board-item-main"]/div/div[@class = "movie-item-info"]/p[@class = "name"]/a/@href')[0]
            item['link'] = 'http://maoyan.com' + id
            item['star'] = movie.xpath('div[@class = "board-item-main"]/div/div[@class = "movie-item-info"]/p[@class = "star"]/text()')[0].replace(' ', '')
            item['time'] = movie.xpath('div[@class = "board-item-main"]/div/div[@class = "movie-item-info"]/p[@class = "releasetime"]/text()')[0][5:]
            integer = movie.xpath('div[@class = "board-item-main"]/div/div[@class = "movie-item-number score-num"]/p/i[@class = "integer"]/text()')[0]
            fraction = movie.xpath('div[@class = "board-item-main"]/div/div[@class = "movie-item-number score-num"]/p/i[@class = "fraction"]/text()')[0]
            item['score'] = str(integer) + str(fraction)
            self.item_list.append(item)

    def save_data(self):
        '''
        保存数据到csv文件中
        :return: csv
        '''
        for every_movie in self.item_list:
            self.col.insert(every_movie)
        # with open('result3.csv', 'w', encoding='UTF-8', newline='') as f:
        #     writer = csv.DictWriter(f, fieldnames=['title', 'link', 'star', 'time', 'score'])
        #     writer.writeheader()
        #     writer.writerows(self.item_list)
            # for every_movie in self.item_list:
            #     writer.writerow(every_movie)

def main():
    '''
    实例化对象，调用多线程
    :return: csv
    '''
    maoyan = MaoyanSpider()
    url = "http://maoyan.com/board/4?"
    url_list = []
    for i in range(10):
        new_url = url + 'offset=' + str(10*i)
        print('正在爬取：', new_url)
        url_list.append(new_url)

    pool = Pool(4)
    pool.map(maoyan.run, url_list)
    pool.close()
    pool.join()
    maoyan.save_data()

if __name__ == "__main__":
    main()
