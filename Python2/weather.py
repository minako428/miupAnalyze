# -*- coding: utf8 -*-
import common
import colored_logger
import json
import re
import datetime
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
    date = datetime()
    hour = Column(Integer, primary_key=True)
    hPa_rand = Column(float, primary_key=True)
    hPa_sea = Column(float, primary_key=True)
    rain = Column(float, primary_key=True)
    temperature = Column(float, primary_key=True)
    dew_point = Column(float, primary_key=True)
    vapor_pressure = Column(float, primary_key=True)
    humidity = Column(float, primary_key=True)
    wind_velocity = Column(float, primary_key=True)
    wind_direction = Column(String(255), index=True)
    HoS = Column(float, primary_key=True)
    solar_irradiance = Column(float, primary_key=True)
    snowfall = Column(float, primary_key=True)
    snow = Column(float, primary_key=True)
    weather = Column(String(255), index=True)
    DoC = Column(float, primary_key=True)
    See = Column(float, primary_key=True)

    def __init__(self, url):
        self.url = url

    @classmethod
    def get_weather(url, prec, block):
        print(url)
        source = common.get_source(url)
        tree = html.fromstring(source)
        cnames_derived_astext = tree.xpath(u"//th[@scope='col']")
        cnames_derived = []
        for word in cnames_derived_astext :
            if u'気圧' in word :
                cnames_derived.append([u'現地',u'海面'])
            elif u'風向' in word :
                cnames_derived.append([u'風速',u'風向'])
            elif u'雪' in word :
                cnames_derived.append([u'降雪',u'積雪'])
            elif u'時' in word : continue
            else : cnames_derived.appned(word)
        for hour in range(1,24) :
            a.hour = hour
            for j in range(1,len(cnames_derived)) :
                j_word = cnames_derived[j]
                a = Weather(url)
                a.prec_id = prec
                a.block_id = block
                a.place_name = common.get_text_content(tree.xpath(u"//caption[@class='m']"))
                a.date = date
                temp = common.get_text_content(tree.xpath(u"//td[@class='data_0_0']")[len(cnames_derived) * (hour - 1) + j])

                if u'現地' in j_word : a.hPa_rand = temp
                elif u'海面' in j_word : a.hPa_sea = temp
                elif u'降水量' in j_word : a.rain = temp
                elif u'気温' in j_word : a.temperature = temp
                elif u'露点' in j_word : a.dew_point = temp
                elif u'蒸気圧' in j_word : a.vapor_pressure = temp
                elif u'湿度' in j_word : a.humidity = temp
                elif u'風向' in j_word : a.wind_velocity = temp
                elif u'風速' in j_word : a.wind_direction = temp
                elif u'日照' in j_word : a.HoS = temp
                elif u'全天' in j_word : a.solar_irradiance = temp
                elif u'降雪' in j_word : a.snowfall = temp
                elif u'積雪' in j_word : a.snow = temp
                elif u'天気' in j_word : a.weather = temp
                elif u'雲量' in j_word : a.DoC = temp
                elif u'視程' in j_word : a.See = temp
            add_to_db(a)

def add_to_db(weather):
    db.weather.remove({u'hour' : weather.hour},{u'block_id' : weather.prec_id},
                      {u'place_name' : weather.place_name},{u'date' : weather.date})
    post=json.dumps({'prec_id' : weather.prec_id,
        'block_id' : weather.block_id,
        'place_name' : weather.place_name.encode('utf8'),
        'date' : weather.date,
        'hour' : weather.hour,
        'hPa_rand' : weather.hPa_rand,
        'hPa_sea' : weather.hPa_sea,
        'rain' : weather.rain,
        'temperature' : weather.temperature,
        'dew_point' : weather.dew_point,
        'vapor_pressure' : weather.vapor_pressure,
        'humidity' : weather.humidity,
        'wind_velocity' : weather.wind_velocity,
        'wind_direction' : weather.wind_direction,
        'HoS' : weather.HoS,
        'solar_irradiance' : weather.solar_irradiance,
        'snowfall' : weather.snowfall,
        'snow' : weather.snow,
        'weather' : weather.weather.enconde('utf8'),
        'DoC' : weather.DoC,
        'See' : weather.See}, sort_keys=False)
    post = json.loads(post)
    db.weather.insert(post)

if __name__ == "__main__":
    start = datetime.date(2014,1,1)
    end = datetime.date.today()
    for prec in range(1,100):
        for block in range(1,2000):
            date = start
            while date != end:
                url = u'{0}hourly_s1.php?prec_no={1}&block_no={2}&year={3}&month={4}&day={5}&view='.format(base_url,prec,block,date.year,date.month,date.day)
                source = common.get_source(url)
                if u'ページを表示することが出来ませんでした' in source : continue
                Weather.get_weather(url, prec, block, date)
    connect.disconnect()