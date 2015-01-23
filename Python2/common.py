# -*- coding: utf8 -*-

import requests
import os
import codecs
import logging as log

import settings

data_folder = settings.data_folder


def get_archive_path_from_url(url):
    arr = url.split(u'/')
    if arr[-1] == u"":
        arr[-1] = u"index.html"
    return data_folder + u'/'.join(arr[2:])


def get_source_content(url):
    html = requests.get(url)
    html.encording = html.apparent_encoding
    return html.content

def get_source(url):
    html = requests.get(url)
    return html.text

def get_source_cache(url):
    archive_path = get_archive_path_from_url(url)
    if not os.path.exists(archive_path):
        # download from the web and save the text
        html = requests.get(url)
        try:
            os.makedirs(u'/'.join(archive_path.split(u'/')[:-1]))
        except OSError:
            pass
        f = codecs.open(archive_path, u'w', u"utf-8")
        f.write(html.text)
        f.close()
        return html.text
    else:
        # get the text from the archive file
        f = codecs.open(archive_path, u'r', u"utf-8")
        text = f.read()
        f.close()
        return text


def get_text_content(html_elements):
    return do_against_html_elements(html_elements, u"text_content")

def do_against_html_elements(html_elements, method_name):
    u"""
    例:
    html_elements = review_element.xpath(".//h3[@class='ateval_summary_detail']")
    html_elements[0].text_content()
    """
    try:
        return unicode(getattr(html_elements[0], method_name)())
    except Exception, e:
        log.debug(e)
        return None


def create_db_session(Base):
    from sqlalchemy import create_engine
    engine = create_engine(settings.db_connection_string, echo=False)

    # make tables
    Base.metadata.create_all(engine)

    from sqlalchemy.orm import sessionmaker
    Session = sessionmaker(bind=engine)
    session = Session()
    return session


def to_absolute_url(base_url, url):
    if url.find(u"http://") == -1:
        new_url = base_url + u"/" + url
        new_url = new_url.replace(u"///", u"/")
    else:
        new_url = url
    new_url = new_url.replace(u"//", u"/")
    new_url = new_url.replace(u"http:/", u"http://")
    return new_url


def convert_date(date_str):
    import datetime
    if date_str.find(u"日") != -1:
        # return datetime.datetime.strptime(date_str, u"%Y年%m月%d日").date()
        year, rest = date_str.split(u"年")
        month, rest = rest.split(u"月")
        day, rest = rest.split(u"日")
        return datetime.datetime(int(year), int(month), int(day))
    elif date_str.find(u"月") != -1:
        #return datetime.datetime.strptime(date_str, u"%Y年%m月").date()
        year, month = date_str.split(u"年")
        return datetime.datetime(int(year), int(month[:-1]), 1)
    elif date_str.find(u"年") != -1:
        #return datetime.datetime.strptime(date_str, u"%Y年").date()
        year, __rest = date_str.split(u"年")
        return datetime.datetime(int(year), 1, 1)
    elif date_str.count(u"-") == 2 and date_str.count(u":") == 0:
        return datetime.datetime.strptime(date_str, u"%Y-%m-%d").date()
    elif date_str.count(u"-") == 2 and date_str.count(u":") == 1:
        return datetime.datetime.strptime(date_str, u"%Y-%m-%d %H:%M")
    else:
        log.error(date_str)
        raise Exception(u"convert_error")
