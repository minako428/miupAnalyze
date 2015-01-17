# -*- coding: utf8 -*-

import common
import settings
import colored_logger
import sqlalchemy
import requests
import json
import sys
import re
import datetime
from pymongo import Connection

from lxml import html

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, DateTime, Integer, String, Float, ForeignKey, Text
from sqlalchemy.orm import relationship, backref
from sqlalchemy.orm.exc import MultipleResultsFound

base_url = u"http://www.cosme.net/product/product_id/"
log = colored_logger.get_logger(u"cosme.py")
Base = declarative_base()
connect = Connection('157.7.143.101',27017)
username = 'miup'
password = 'miupmiup222'
connect['cosme'].authenticate(username, password)
db = connect.cosme

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

    def __init__(self, url):
        self.url = url
        self.reviews = []

    @classmethod
    def from_item_pages(cls, url):

        top_url = url
        print(url)
        a = Cosme(top_url)
        source = common.get_source(top_url)
        tree = html.fromstring(source)
        a.id = re.split(u'/',url)[5]
        a.title = common.get_text_content(tree.xpath(u"//strong[@itemprop='title']"))
        a.manufacturer = common.get_text_content(tree.xpath(u"//dl[@class='maker clearfix']/dd/a"))
        temp = tree.xpath(u"//dl[@class='official-site clearfix']/dd/a")
        if len(temp)>0 : a.sell_page = temp[0].attrib['href']
        else : a.sell_page = u''
        a.rating = common.get_text_content(tree.xpath(u"//*/div[@class='info-desc']/p[@itemprop='ratingValue']"))
        if a.rating is None: return
        if a.rating == u'0': return
        print(u'Get Reviews')
        a.capacity = common.get_text_content(tree.xpath(u"//ul[@class='info-rating']/li[@class='clearfix']/p[@class='info-desc']"))
        a.capacity = re.sub(u'ｍl',u'ml',a.capacity)
        if u'・' in a.capacity :
             a.price = a.capacity.encode('utf_8').split('・')[1]
             a.capacity = a.capacity.encode('utf_8').split('・')[0]
        elif u'ml' in a.capacity :
            a.price = u'0円'
        elif u'円' in a.capacity :
            a.price = a.capacity
            a.capacity = u'0ml'
        a.date = tree.xpath(u"//*/ul[@class='info-rating']/li[@class='clearfix']/p[@class='info-desc']")[1].text_content()
        a.date = re.sub(u'NEW','',a.date)
        a.date = re.split(u' ',a.date)[0]
        a.date  = (int(re.split(u'/',a.date)[0])-2000) * 365 + int(re.split(u'/',a.date)[1]) * 30 + int(re.split(u'/',a.date)[2])
        a.category = tree.xpath(u"//*/dl[@class='item-category clearfix']/dd/span/a")[2].text_content()

        page_number = 0
        urls = []
        while True:
            url = u"{0}{1}/reviews/p/{2}".format(base_url, a.id, page_number)
            source = common.get_source(url)
            new_urls = parse_search_page(url, source)
            urls = urls + new_urls
            page_number += 1
            print(page_number)
            if len(new_urls) < 1:
                break

        if len(urls) == 0:
            return
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
        add_to_db(a)

def get_urls(id):
    d = datetime.datetime.today()
    urls = []
    for sort in range(0,1000):
        top_url = u"http://www.cosme.net/item/item_id/{0}/products/page/{1}/srt/3".format(id, sort)
        source = common.get_source(top_url)
        tree = html.fromstring(source)
        item_date = re.sub(u'発売日：',u'',tree.xpath(u"//span[@class='sell']")[0].text_content())
        item_date  = (int(re.split(u'/',item_date)[0])-2000) * 365 + int(re.split(u'/',item_date)[1]) * 30 + int(re.split(u'/',item_date)[2])
        if item_date < (d.year - 2000 - 1) * 365 + d.month * 30 + d.day: break
        pages = tree.xpath(u"//div[@class='inner']/p[@class='item-head']/span[@class='item']/a")
        for page in pages:
            urls.append(page.attrib['href'])
    return(urls)

def parse_search_page(url, source):
    log.info(u'parsing %s', url)
    tree = html.fromstring(source)
    tmp_urls = []
    divs = tree.xpath(u"//*/span[@class='read-more']")
    for d in divs:
        tmp_urls.append(d.xpath(u"a")[0].attrib['href'])
    return tmp_urls

def add_to_db(cosme):
    db.cosme.remove({u'id' : cosme.id})
    post=json.dumps({'id': cosme.id,
          'title': cosme.title.encode('utf_8'),
          'manufacturer': cosme.manufacturer.encode('utf_8'),
          'sell_page': cosme.sell_page.encode('utf_8'),
          'rating': cosme.rating,
          'capacity': cosme.capacity.encode('utf_8'),
          'price': cosme.price,
          'url': cosme.url.encode('utf_8'),
          'category': cosme.category.encode('utf_8'),
          'date': cosme.date,
          'review': cosme.reviews}, sort_keys=False)
    post = json.loads(post)
    db.cosme.insert(post)

if __name__ == u"__main__":
    ids = {u'1003',u'1071'}
    urls = []
    for id in ids:
        urls = urls + get_urls(id)
    for url in urls:
        Cosme.from_item_pages(url)
    connect.disconnect()
