from HTMLParser import HTMLParser
import re
from string import rsplit, strip, split
import csv
import codecs
import exceptions
DEBUG = False

def LOG (prow):
    if DEBUG:
        print prow


name = r'([A-Z][a-z]+.)+'
prop = r'([A-Za-z])+'
year = r'^(.*)[0-9][0-9][0-9][0-9](.*)$'
date = r'[A-Z][a-z]+ [0-9][0-9], [0-9][0-9][0-9][0-9]'

months = ['january', 'february', 'march',  'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december']




class TableParser(HTMLParser):
    def __init__(self, dataKeyword, nms):
        HTMLParser.__init__(self)
        self.in_table = False
        self.in_data = False
        self.in_td = False
        self.dataAttr = dataKeyword
        self.related_objs = nms

        self.re_obj = re.compile(r"^(.*)" + dataKeyword + "(.*)$",re.I)
        self.re_year = re.compile(r"^(.*)" + year + "(.*)$",re.I)
        self.in_td_string = ""
        self.res_dict = {}
        self.curr_v = ""
        self.curr_v_tag = ""
        self.br_last = False #to mark that we passed new potetial related object

    def handle_starttag(self, tag, attrs):
        try:
            if tag == 'table' and ('class', 'infobox vcard') in attrs:
                self.in_table = True
            if tag == 'td':
                self.in_td = True
                return

            if self.in_table and self.in_data:
                if tag == 'br':
                    self.br_last = True
                    return
                for (k,v) in attrs:
                    if k == 'title':
                        if re.match(prop, v):


                            self.in_td_string += ("\n " + v + " -- from tag --  : ")
                            if v in self.related_objs:
                                self.res_dict[v] = []
                                self.curr_v = v
                                self.curr_v_tag = tag
                            return
                        splitted = rsplit(v,"\\| |,|;|-")
                        for s in splitted:
                            if re.match(year, s) or (s in months) or self.re_year.match(v):
                                self.in_td_string += v + "-- from tag -- "
                                return
        except Exception as e:
            print "in handle_starttag"
            print e
        
            


    def handle_data(self, data):
        try:
            if self.in_table :
                if self.in_data and data != "":
                    #splitted = rsplit(data,"\,\.-\(\)")
                    datan = data
                    if "(" in datan:
                        datan = filter(None, re.split("[\(]+", datan))[0]
                    splitted = filter(None, re.split("[,\-!?:\(\)]+", datan))
                    nums = re.findall(r'\d+', data)
                    for s in splitted:
                        ccv = strip(s,'" \n')
                        ccv = strip(ccv,'" ')
                        if re.match(name, ccv):
                            if (ccv not  in  self.res_dict and self.curr_v_tag == "") or (self.br_last and self.curr_v_tag == ""):
                                self.res_dict[ccv] = []
                                self.curr_v = ccv
                                self.br_last = False

                    for n in nums:
                        if re.match(year, n) or self.re_year.match(n):
                            self.in_td_string += (n + " - ")
                            if not self.curr_v == "":
                                self.res_dict[self.curr_v].append(n)
                    return
                if self.re_obj.match(data):
                    self.in_data = True
                    self.in_td_string += ("prop root: " + data + "\n")
                    return
        except Exception as e:
            print "in handle_data"
            print e


    def handle_endtag(self, tag):
        try:
            if tag == 'table':
                self.in_table = False
                self.in_data = False
            if tag == 'tr':
                self.in_data = False
            if tag == 'td':
                self.in_td = False
                #LOG( self.in_td_string)
                self.in_td_string = ""
            if tag == self.curr_v_tag:
                self.curr_v_tag = ""
        except Exception as e:
            print "in handle_endtag"
            print e