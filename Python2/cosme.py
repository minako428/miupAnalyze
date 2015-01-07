# -*- coding: utf8 -*-

import common
import settings
import colored_logger
import sqlalchemy
import requests
import json
from pymongo import Connection

from lxml import html

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, DateTime, Integer, String, Float, ForeignKey, Text
from sqlalchemy.orm import relationship, backref
from sqlalchemy.orm.exc import MultipleResultsFound

base_url = u"http://www.cosme.net/product/product_id/"
log = colored_logger.get_logger(u"cosme.py")
Base = declarative_base()
connect = Connection('localhost', 27017)
db = connect.test
connect.drop_database(db)
db = connect.test
product = db.product

class Cosme(Base):
    __tablename__ = u"cosmes"

    id = Column(Integer, primary_key=True)
    title = Column(String(255), index=True)
    manufacturer = Column(String(255), index=True)
    sell_page = Column(String(255), unique=True, index=True)
    rating = Column(String(255), unique=True, index=True)
    capacity = Column(String(255), index=True)
    price = Column(String(255), index=True)
    url = Column(String(255), unique=True, index=True)
    category = Column(String(255), index=True)
    date = Column(String(255), index=True)

    def __init__(self, url, id):
        self.url = url
        self.id = id
        self.reviews = []

    @classmethod
    def from_item_pages(cls, keyword):

        top_url = u"{0}{1}/top".format(base_url, keyword)
        a = Cosme(top_url, keyword)
        #source = requests.get(top_url).content
        #source = source.decode('utf-8')
        source = common.get_source(top_url)
        tree = html.fromstring(source)
        #print(source)
        #print(top_url)
        a.id = keyword
        a.title = common.get_text_content(tree.xpath(u"//strong[@itemprop='title']"))
        a.manufacturer = common.get_text_content(tree.xpath(u"//dl[@class='maker clearfix']/dd/a"))
        a.sell_page = tree.xpath(u"//dl[@class='official-site clearfix']/dd/a")[0].attrib['href']
        a.rating = common.get_text_content(tree.xpath(u"//*/div[@class='info-desc']/p[@class='average avr-5_5']"))
        a.capacity = common.get_text_content(tree.xpath(u"//ul[@class='info-rating']/li[@class='clearfix']/p[@class='info-desc']"))
        a.price = a.capacity.encode('utf_8').split('・')[1]
        a.capacity = a.capacity.encode('utf_8').split('・')[0]
        a.date = tree.xpath(u"//*/ul[@class='info-rating']/li[@class='clearfix']/p[@class='info-desc']")[1].text_content()
        a.category = tree.xpath(u"//*/dl[@class='item-category clearfix']/dd/span/a")[2].text_content()

        log.warning(u'searching "%s"', keyword)
        page_number = 0
        urls = []
        while True:
            url = u"{0}{1}/reviews/p/{2}".format(base_url, keyword, page_number)
            source = common.get_source(url)
            new_urls = parse_search_page(url, source)
            urls = urls + new_urls
            page_number += 1
            print(page_number)
            if len(new_urls) < 1:
                break
        #review_page_urls = [s.replace(u"anime", u"anime_review") for s in urls]
        #log.warning(u'the search results:')
        #log.info(review_page_urls)

        if len(urls) == 0:
            return a
        for url in urls:
            try:
                review_source = common.get_source(url)
                review_tree = html.fromstring(review_source)
                review_element = common.get_text_content(review_tree.xpath(u"//*/p[@class='read']"))
                a.reviews.append(review_element)
            except Exception, e:
                if e.__str__() != u"not_found":
                    log.debug(u"{0}, url = {1}".format(e, url))
                break
        return a

def parse_search_page(url, source):
    log.info(u'parsing %s', url)
    tree = html.fromstring(source)
    tmp_urls = []
    divs = tree.xpath(u"//*/span[@class='read-more']")
    for d in divs:
        tmp_urls.append(d.xpath(u"a")[0].attrib['href'])
    return tmp_urls

def add_to_db(cosme):
    post=json.dumps({'id': cosme.id,
          'title': cosme.title.encode('utf_8'),
          'manufacturer': cosme.manufacturer.encode('utf_8'),
          'sell_page': cosme.sell_page.encode('utf_8'),
          'rating': cosme.rating.encode('utf_8'),
          'capacity': cosme.capacity.encode('utf_8'),
          'price': cosme.price,
          'url': cosme.url.encode('utf_8'),
          'category': cosme.category.encode('utf_8'),
          'date': cosme.date.encode('utf_8')}, sort_keys=False)
    post = json.loads(post)
    db.product.insert(post)
    print(db.product.find_one())
    print(db.product.find_one(({"rating": "5.5"})))

if __name__ == u"__main__":
    import sys
    start = int(sys.argv[1])
    end = int(sys.argv[2])
    for keyword in range(start,end):
       add_to_db(Cosme.from_item_pages(keyword))
    connect.disconnect()
