# -*- coding: utf8 -*-

from os import path

current_path = path.dirname(path.abspath(__file__))

# ダウンロードしたhtmlファイルをキャッシュする場所
data_folder = current_path + u"/cache/"

# for SQLite3
#db_connection_string = "sqlite:///data.db"

# for MySQL
# http://docs.sqlalchemy.org/en/rel_0_9/core/engines.html#mysql
user_name = "user_name"
password = "password"
host = "localhost"
db_name = "review_scraper"
db_connection_string = u"mysql://{0}:{1}@{2}/{3}?charset=utf8".format(user_name, password, host, db_name)

