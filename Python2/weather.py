# -*- coding: utf8 -*-
import common
import colored_logger
import json
import re
import sys
import datetime
import array
from pymongo import Connection
from lxml import html
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String

base_url=u'http://www.data.jma.go.jp/obd/stats/etrn/view/'
log = colored_logger.get_logger(u"weather.py")
Base = declarative_base()
connect = Connection('157.7.143.101',27017)
username = 'miup'
password = 'miupmiup222'
connect['cosme'].authenticate(username, password)
db = connect.cosme

class Weather(Base):
    __tablename__ = u"weather"

    prec_id = Column(Integer, primary_key=True)
    block_id = Column(Integer, primary_key=True)
    place_name = Column(String(255), index=True)
    date = Column(Integer, primary_key=True)
    hour = Column(Integer, primary_key=True)
    hPa_rand = float()
    hPa_sea = float()
    rain = float()
    temperature = float()
    dew_point = float()
    vapor_pressure = float()
    humidity = float()
    wind_velocity = float()
    wind_direction = Column(String(255), index=True)
    HoS = float()
    solar_irradiance = float()
    snowfall = float()
    snow = float()
    weather = u'Unknown'
    DoC = float()
    See = float()

    def __init__(self, url):
        self.url = url

    @classmethod
    def get_weather(cls, url, prec, block, date, source):
        exists = db.weather.find.one({u'prec_id' : prec,
                       u'block_id' : block,
                       u'date' : date.strftime('%Y/%m/%d'),
        })
        if exists == exists : return True
        print(url)
        tree = html.fromstring(source)
        cnames_derived_astext = tree.xpath(u"//th[@scope='col']")
        cnames_derived = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
        order = (u'時',u'現地',u'海面',u'降水量',u'気温',u'露点',u'蒸気圧',u'湿度',
                 u'風速',u'風向',u'日照',u'全天',u'降雪',u'積雪',u'天気',u'雲量',u'視程')
        for word in cnames_derived_astext :
            word = word.text
            for i in range(0,len(order)):
                if order[i] in word:
                    cnames_derived[i] = 1
                    break
        for hour in range(1,24) :
            count = 0
            temp = tree.xpath(u"//td[@class='data_0_0']")
            a = Weather(url)
            a.hour = hour
            a.prec_id = prec
            a.block_id = block
            a.place_name = re.split(u'　',common.get_text_content(tree.xpath(u"//caption[@class='m']")))[0]
            a.date = date
            for j in range(1,len(cnames_derived)) :
                if cnames_derived[j] == 0 : continue
                val = temp[count].text
                if val == '--'	: val = 0
                if val == '///'	: val = 0
                if val != val	: val = 0
                if j == 1 : a.hPa_rand = val
                elif j == 2 : a.hPa_sea = val
                elif j == 3 : a.rain = val
                elif j == 4 : a.temperature = val
                elif j == 5 : a.dew_point = val
                elif j == 6 : a.vapor_pressure = val
                elif j == 7 : a.humidity = val
                elif j == 8 : a.wind_velocity = val
                elif j == 9 : a.wind_direction = val
                elif j == 10 : a.HoS = val
                elif j == 11 : a.solar_irradiance = val
                elif j == 12 : a.snowfall = val
                elif j == 13 : a.snow = val
                elif j == 14 : a.weather = temp[count].attrib['alt']
                elif j == 15 : a.DoC = val
                elif j == 16 : a.See = val
                count += 1
            add_to_db(a)
        return False

def add_to_db(weather):
    post=json.dumps({u'prec_id' : weather.prec_id,
        u'block_id' : weather.block_id,
        u'place_name' : weather.place_name.encode('utf8'),
        u'date' : weather.date.strftime('%Y/%m/%d'),
        u'hour' : weather.hour,
        u'hPa_rand' : weather.hPa_rand,
        u'hPa_sea' : weather.hPa_sea,
        u'rain' : weather.rain,
        u'temperature' : weather.temperature,
        u'dew_point' : weather.dew_point,
        u'vapor_pressure' : weather.vapor_pressure,
        u'humidity' : weather.humidity,
        u'wind_velocity' : weather.wind_velocity,
        u'wind_direction' : weather.wind_direction,
        u'HoS' : weather.HoS,
        u'solar_irradiance' : weather.solar_irradiance,
        u'snowfall' : weather.snowfall,
        u'snow' : weather.snow,
        u'weather' : weather.weather,
        u'DoC' : weather.DoC,
        u'See' : weather.See}, sort_keys=False)
    post = json.loads(post)
    db.weather.insert(post)

def add_to_db_1st(prec, block):
    db.weather_place.remove({u'prec_id' : prec, u'block_id' : block})
    post=json.dumps({u'prec_id' : prec, u'block_id' : block}, sort_keys=False)
    post = json.loads(post)
    db.weather_place.insert(post)

def get_prec_block():
    for prec in range(11,100):
        top_url=u'http://www.data.jma.go.jp/obd/stats/etrn/select/prefecture.php?prec_no={0}&block_no=&year=2014&month=&day=1&view='.format(prec)
        source = common.get_source(top_url)
        if len(source) < 6500 : continue
        for block in range(1,2000):
            date = datetime.date(2013,1,1)
            if block < 10 : url = u'{0}hourly_a1.php?prec_no={1}&block_no=000{2}&year={3}&month={4}&day={5}&view='.format(base_url,prec,block,date.year,date.month,date.day)
            elif block < 100 : url = u'{0}hourly_a1.php?prec_no={1}&block_no=00{2}&year={3}&month={4}&day={5}&view='.format(base_url,prec,block,date.year,date.month,date.day)
            elif block < 1000 : url = u'{0}hourly_a1.php?prec_no={1}&block_no=0{2}&year={3}&month={4}&day={5}&view='.format(base_url,prec,block,date.year,date.month,date.day)
            else : url = u'{0}hourly_a1.php?prec_no={1}&block_no={2}&year={3}&month={4}&day={5}&view='.format(base_url,prec,block,date.year,date.month,date.day)
            source = common.get_source(url)
            if len(source) < 6500 : continue
            add_to_db(prec, block)
        for block in range(47000,48000):
            date = start
            url = u'{0}hourly_s1.php?prec_no={1}&block_no={2}&year={3}&month={4}&day={5}&view='.format(base_url,prec,block,date.year,date.month,date.day)
            source = common.get_source(url)
            if len(source) < 6500 : continue
            add_to_db_1st(prec, block)
    connect.disconnect()

if __name__ == "__main__":
    start = datetime.date.today() - datetime.timedelta(1)
    end = datetime.date(2013,1,1)
    if False :
        get_prec_block()
        sys.exit()
    for pair in db.weather_place.find():
        prec = pair['prec_id']
        block = pair['block_id']
        date = start
        while date != end:
            if block < 10 : url = u'{0}hourly_a1.php?prec_no={1}&block_no=000{2}&year={3}&month={4}&day={5}&view='.format(base_url,prec,block,date.year,date.month,date.day)
            elif block < 100 : url = u'{0}hourly_a1.php?prec_no={1}&block_no=00{2}&year={3}&month={4}&day={5}&view='.format(base_url,prec,block,date.year,date.month,date.day)
            elif block < 1000 : url = u'{0}hourly_a1.php?prec_no={1}&block_no=0{2}&year={3}&month={4}&day={5}&view='.format(base_url,prec,block,date.year,date.month,date.day)
            elif block > 40000 : url = u'{0}hourly_s1.php?prec_no={1}&block_no={2}&year={3}&month={4}&day={5}&view='.format(base_url,prec,block,date.year,date.month,date.day)
            else : url = u'{0}hourly_a1.php?prec_no={1}&block_no={2}&year={3}&month={4}&day={5}&view='.format(base_url,prec,block,date.year,date.month,date.day)
            source = common.get_source_content(url)
            if len(source) < 6500 : continue
            if Weather.get_weather(url, prec, block, date, source) : break
            date = date - datetime.timedelta(1)
    connect.disconnect()
