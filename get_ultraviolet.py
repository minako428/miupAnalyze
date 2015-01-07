import urllib
import urllib2
import os.path
import sys
from HTMLParser import HTMLParser
import tesseract

def download(url):
    img = urllib.urlopen(url)
    localfile = open(os.path.basename(url),'wb')
    localfile.write(img.read())
    img.close()
    localfile.close()

class imgParser(HTMLParser):

    def __init__(self):
        HTMLParser.__init__(self)

    def handle_starttag(self,tagname,attribute):
        if tagname.lower() == "img":
            for i in attribute:
                if i[0].lower() == "src":
                    img_url=i[1]
                    f = open("collection_url.txt","a")
                    f.write("%s\t"%img_url)
                    f.close()

if __name__ == "__main__":

    api = tesseract.TessBaseAPI()
    api.Init(".","eng",tesseract.OEM_DEFAULT)
    api.SetVariable("tessedit_char_whitelist", "0123456789abcdefghijklmnopqrstuvwxyz")
    api.SetPageSegMode(tesseract.PSM_AUTO)

    mImgFile = "test.jpg"
    mBuffer=open(mImgFile,"rb").read()
    result = tesseract.ProcessPagesBuffer(mBuffer,len(mBuffer),api)
    print "result(ProcessPagesBuffer)=",result

    months=['1','2','3','4','5','6','7','8','9','10','11','12']
    years=['2010','2011','2012','2013','2014']
    places=['Sap','Tsu','Nah']
    for place in places:
        for year in years:
            for month in months:

                serch_url=u"{0}{1}/top".format("http://www.data.jma.go.jp/gmd/env/uvhp/obs/{1}{2}/{3}{1}{2}{3}.png", year)
                htmldata=urllib2.urlopen(serch_url)

                parser = imgParser()
                parser.feed(htmldata.read())

                parser.close()
                htmldata.close()

                f = open("collection_url.txt","r")
                for row in f:
                    row_url = row.split('\t')
                    len_url = len(row_url)
                f.close()

                number_url = []

                for i in range(0,(len_url-1)):
                    number_url.append(row_url[i])

                for j in range(0,(len_url-1)):
                    url = number_url[j]
                    download(url)

                os.remove("collection_url.txt")