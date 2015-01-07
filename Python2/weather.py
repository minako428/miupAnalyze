# coding: UTF-8
import urllib2
from bs4 import BeautifulSoup
import csv
import datetime
#------------------------------------------------------------------------------
def str2float(str):
    try:
        return float(str)
    except:
        return '--'    
#------------------------------------------------------------------------------
def main(url):
    # サーバーから気象データのページを取得
    html = urllib2.urlopen(url).read()
    soup = BeautifulSoup(html)
    trs = soup.find('table', { 'class' : 'data2_s' })
 
    list = []
 
    # 1レコードづつ処理する
    for tr in trs.findAll('tr')[2:]:
        tds = tr.findAll('td')
 
        if tds[1].string == None:   # その月がまだ終わってない場合、途中でデータがなくなる
            break;
 
        dic = {}
        #dic['day']              = str(tds[0].find('a').string) # 日付
        if name == 'osaka' or name == 'kobe':
            dic['precipitation'] = str2float(tds[3].string) # 降水量
            dic['temperature']   = str2float(tds[4].string) # 気温
        else:
            dic['precipitation'] = str2float(tds[1].string) # 降水量
            dic['temperature']   = str2float(tds[2].string) # 気温
        list.append(dic)
    
    for dic in list:
        print name,date,dic['precipitation'],dic['temperature']
        writer.writerow([date,dic['precipitation'],dic['temperature']])
#------------------------------------------------------------------------------
if __name__ == "__main__":
    # 取得地点の設定（URLを用意した地点）
    names = ['osaka', 'hirakata', 'yao', 'sakai', 'ibaraki', 'toyonaka', 'kobe', 'nishimoniya']
    # 取得開始・終了時間
    start = datetime.date(2009,2,1)
    end   = datetime.date(2010,4,1) #この日は取得されない
 
    for name in names:
        # 出力ファイル名
        with open(name+'.csv','w') as f:
            writer = csv.writer(f, lineterminator='\n')
            writer.writerow(['date','precipitation','temperature'])
            date = start
            while date != end:
                # 取得地点のURL
                if name == 'osaka':         url = 'http://www.data.jma.go.jp/obd/stats/etrn/view/hourly_s1.php?prec_no=62&block_no=47772&year=%d&month=%d&day=%d&view='%(date.year,date.month,date.day)
                if name == 'hirakata':      url = 'http://www.data.jma.go.jp/obd/stats/etrn/view/hourly_a1.php?prec_no=62&block_no=1065&year=%d&month=%d&day=%d&view='%(date.year,date.month,date.day)
                if name == 'yao':           url = 'http://www.data.jma.go.jp/obd/stats/etrn/view/hourly_a1.php?prec_no=62&block_no=1470&year=%d&month=%d&day=%d&view='%(date.year,date.month,date.day)
                if name == 'sakai':         url = 'http://www.data.jma.go.jp/obd/stats/etrn/view/hourly_a1.php?prec_no=62&block_no=1062&year=%d&month=%d&day=%d&view='%(date.year,date.month,date.day)
                if name == 'ibaraki':       url = 'http://www.data.jma.go.jp/obd/stats/etrn/view/hourly_a1.php?prec_no=62&block_no=1602&year=%d&month=%d&day=%d&view='%(date.year,date.month,date.day)
                if name == 'toyonaka':      url = 'http://www.data.jma.go.jp/obd/stats/etrn/view/hourly_a1.php?prec_no=62&block_no=0602&year=%d&month=%d&day=%d&view='%(date.year,date.month,date.day)
                if name == 'kobe':          url = 'http://www.data.jma.go.jp/obd/stats/etrn/view/hourly_s1.php?prec_no=63&block_no=47770&year=%d&month=%d&day=%d&view='%(date.year,date.month,date.day)
                if name == 'nishimoniya':   url = 'http://www.data.jma.go.jp/obd/stats/etrn/view/hourly_a1.php?prec_no=63&block_no=1588&year=%d&month=%d&day=%d&view='%(date.year,date.month,date.day)
                main(url)
                date += datetime.timedelta(1)