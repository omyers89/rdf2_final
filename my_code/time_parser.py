import urllib
from HTMLParser import HTMLParser
import re
from string import rsplit, strip, split
import csv
import codecs
import exceptions
import nltk
from my_code.Utils import *
from nltk.stem.snowball import SnowballStemmer
from nltk.stem.wordnet import WordNetLemmatizer
DEBUG = False


def LOG(prow):
    if DEBUG:
        print prow


name = r'([A-Z][a-z]+.)+'
prop = r'([A-Za-z])+'
year = r'^(.*)[0-9][0-9][0-9][0-9](.*)$'
date = r'[A-Z][a-z]+ [0-9][0-9], [0-9][0-9][0-9][0-9]'

months = ['january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november',
          'december']

DBPEDIA_URL_UP = "http://dbpedia.org/sparql"

def get_obj_labels(objects_uris):
    label_list = []
    for ouri in objects_uris:
        obj_lables = get_lable_for_obj(ouri)
        for o in obj_lables:
            label_list.append(o)
    return label_list


def get_wiki_redirect_for_obj(ou):
    local_sprql = SPARQLWrapper(DBPEDIA_URL_UP)
    ouriu = ou.encode('utf-8')
    r_list = []
    query_text = ("""
                    PREFIX dbo: <http://dbpedia.org/ontology/>
                    SELECT distinct ?o WHERE {
                        ?o dbo:wikiPageRedirects <%s>.
                    } 
                    LIMIT 5 """ % ouriu)
    local_sprql.setQuery(query_text)
    local_sprql.setReturnFormat(JSON)
    results_inner = local_sprql.query().convert()
    for inner_res in results_inner["results"]["bindings"]:
        # s = inner_res["s"]["value"]
        o = inner_res["o"]["value"]
        r_list.append(o)

    return r_list

def get_comment_for_prop(prop_uri):
    local_sprql = SPARQLWrapper(DBPEDIA_URL_UP)
    puriu = prop_uri.encode('utf-8')
    r_list = []
    query_text = ("""
                    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                    SELECT distinct ?c WHERE {
                        <%s> rdfs:comment ?c.
                        FILTER (langMatches( lang(?c), "en" )).
                    }  """ % puriu)
    local_sprql.setQuery(query_text)
    local_sprql.setReturnFormat(JSON)
    results_inner = local_sprql.query().convert()
    for inner_res in results_inner["results"]["bindings"]:
        # s = inner_res["s"]["value"]
        c = inner_res["c"]["value"]
        r_list.append(c)
    print r_list
    return r_list


def get_wiki_redirects(objects_uris):
    all_wiki_red = []
    for ou in objects_uris:
        redirect_list = get_wiki_redirect_for_obj(ou)
        for r in redirect_list:
            all_wiki_red.append(get_subj_from_uri(r))
    for ob in objects_uris:
        all_wiki_red.append(get_subj_from_uri(ob))
    return list(set(all_wiki_red))


def get_comment_keyword(prop_uri):
    comm_list = get_comment_for_prop(prop_uri)
    candidates = []
    for cm in comm_list:
        text = nltk.word_tokenize(cm)
        tags = nltk.pos_tag(text)
        for w,t in tags:
            if t in ['VBD','NNS', 'VBN','NN']:
                wl = WordNetLemmatizer().lemmatize(w)
                candidates.append(wl)
    return candidates


def get_wiki_redirects_normal(wiki_reds):
    normal_objs = [o.replace('_', ' ') for o in wiki_reds]
    return normal_objs


def get_key_words(subj_uri, prop_uri):
    objects_uris, normal_names = get_related_objects_from_uri(subj_uri, prop_uri)
    obj_labels = get_obj_labels(objects_uris)
    wiki_reds = get_wiki_redirects(objects_uris)
    wiki_reds_normal = get_wiki_redirects_normal(wiki_reds)
    prop_name = get_lable_for_obj(prop_uri)
    prop_comment = get_comment_keyword(prop_uri)
    all_keyword_list = list(set(normal_names+obj_labels+prop_name+prop_comment))
    all_reds_list = list(set(wiki_reds+wiki_reds_normal))
    return all_keyword_list,all_reds_list

def get_time_prep_dict_for_sp(subj_uri, prop_uri):
    subj = get_subj_from_uri(subj_uri)
    urls = "https://en.wikipedia.org/wiki/" + subj
    response = urllib.urlopen(urls).read()
    try:
        all_keywrds, all_redirects = get_key_words(subj_uri, prop_uri)
        LOG(["all_keywords:", all_keywrds, "all_redirects: ", all_redirects])
        tp = TimeParser(all_keywrds, all_redirects)
        part_string = make_unicode(response)
        tp.feed(part_string)
    except Exception as e:
        LOG(e)
        LOG(subj)
    return tp.res_dict

class TimeParser(HTMLParser):
    def __init__(self, dataKeywords,all_redirects):
        HTMLParser.__init__(self)
        self.in_p = False
        self.relevant_sentence = False
        self.dataKeywords = [re.compile(r"^(.*)" + dk + "(.*)$", re.I) for dk in dataKeywords]
        self.redirects = [re.compile(r"^(.*)" + rd + "(.*)$", re.I) for rd in all_redirects]
        self.relevant_words = []
        self.res_dict = {
            "on":0,
            "in":0,
            "at":0,
            "since":0,
            "for":0,
            "ago":0,
            "before":0,
            "to":0,
            "past":0,
            "from":0,
            "till":0,
            "until":0,
            "by":0,
            "year":0,
            "sec":0
            }
        self.year_re = re.compile(r"^(.*)" + year + "(.*)$",re.I)
        self.sec_words = [ re.compile(r"^(.*)" + "first" + "(.*)$",re.I),
                           re.compile(r"^(.*)" + "second" + "(.*)$",re.I),
                           re.compile(r"^(.*)" + "third" + "(.*)$",re.I),
                           re.compile(r"^(.*)" + "last" + "(.*)$",re.I)]
        self.curr_v = ""
        self.curr_v_tag = ""
        self.br_last = False  # to mark that we passed new potential related object

    def prep_match_and_update(self, val):
        for k in self.res_dict:
            if k == "year":
                if self.year_re.match(val):
                    self.res_dict[k] = self.res_dict[k]+1
                    return
            elif k=="sec":
                for re_sec in self.sec_words:
                    if re_sec.match(val):
                        self.res_dict[k] = self.res_dict[k]+1
                        return
            else:
                kws = " " + k + " "
                if kws in val.lower():
                    self.res_dict[k] = self.res_dict[k] + 1
                    return


    def match_tag_to_obj_names(self, tag):
        for re_red in self.redirects:
            if re_red.match(tag):
                return True
        return False

    def handle_starttag(self, tag, attrs):
        try:
            if tag == 'p':
                # we are in a new paragraph should start recording
                self.in_p = True
            if tag == 'a' and self.in_p:
                for (k,v) in attrs:
                    if (k == "href" or k=="tag") and self.match_tag_to_obj_names(v):
                        self.relevant_sentence = True
                return
        except Exception as e:
            print "in handle_starttag"
            print e


    def update_and_flush(self):
        if self.relevant_sentence:
            for w in self.relevant_words:
                self.prep_match_and_update(w)
            self.relevant_sentence = False
            LOG ("in update and flush, relevant sentence")
            LOG (self.relevant_words)
        self.relevant_words = []


    def handle_data(self, data):
        try:
            if self.in_p:
                # need to record the data
                if data != "":
                    # splitted = rsplit(data,"\,\.-\(\)")
                    datan = data
                    splitted = filter(None, re.split("[,\-!?:\(\)]+", datan))
                    # now we ave all words in the data
                    for s in splitted:
                        ccv = strip(s, '" \n')
                        ccv = strip(ccv, '" ')
                        self.relevant_words.append(ccv)
                        if "." in ccv:
                            self.update_and_flush()  # this update res dict flush words, un-set relevant sentence

        except Exception as e:
            print "in handle_data"
            print e

    def handle_endtag(self, tag):
        try:
            if tag == 'p':
                self.update_and_flush()
                self.in_p = False
        except Exception as e:
            print "in handle_endtag"
            print e




if __name__ == "__main__":
    # y = r"[0-9][0-9][0-9][0-9]"
    # re_year = re.compile(r"^(.*)" + year + "(.*)$", re.I)
    # if re.match(re_year, "199s7.bc"):
    #     print "match"
    # else:
    #     print "no match"
    #
    #
    # comment_list = [""]
    #
    #
    # puri_list_for_test = ["http://dbpedia.org/ontology/almaMater", "http://dbpedia.org/ontology/spouse",
    #                     "http://dbpedia.org/ontology/birthPlace",
    #                       "http://dbpedia.org/ontology/spouse",
    #                       "http://dbpedia.org/ontology/residence",
    #                       "http://dbpedia.org/ontology/academicAdvisor",
    #                       "http://dbpedia.org/ontology/board",
    #                       "http://dbpedia.org/ontology/college",
    #                       "http://dbpedia.org/ontology/currentMember",
    #                       "http://dbpedia.org/ontology/discipline",
    #                       "http://dbpedia.org/ontology/employer",
    #                       "http://dbpedia.org/ontology/hometown",
    #                       "http://dbpedia.org/ontology/appointer",
    #                       "http://dbpedia.org/ontology/club",
    #                       "http://dbpedia.org/ontology/coach",
    #                       "http://dbpedia.org/ontology/draftTeam",
    #
    #                       ]
    # for pu in puri_list_for_test:
    #     print "in comm for:", pu
    #     comm_list = get_comment_for_prop(pu)
    #     for cm in comm_list:
    #         text = nltk.word_tokenize(cm)
    #         print nltk.pos_tag(text)

    #print get_wiki_redirects(["http://dbpedia.org/resource/Melania_Trump","http://dbpedia.org/resource/Donald_Trump"])
    # print get_lable_for_obj("http://dbpedia.org/ontology/almaMater")
    # print get_comment_for_prop("http://dbpedia.org/ontology/almaMater")
    '''
    list of words and tags:
    (u'attended', 'VBD')
    (u'schools', 'NNS')
    (u'married', 'VBN')
    (u'born', 'VBN')
    (u'residence', 'NN')
    (u'married', 'VBN')     
    '''
    ress = get_time_prep_dict_for_sp("http://dbpedia.org/resource/Donald_Trump", "http://dbpedia.org/ontology/residence")
    for r in ress.items():
        print r
    #
    # words = ['attended','schools','married','born']
    # snowball_stemmer = SnowballStemmer("english")
    # for word in words:
    #     print "\n\nOriginal Word =>", word
    #     print "WordNet Lemmatizer=>", WordNetLemmatizer().lemmatize(word)
    #     print "snowball stemmer=>", snowball_stemmer.stem(word)
    #
    #     print "WordNet & snowball Lemmatizer=>", snowball_stemmer.stem(WordNetLemmatizer().lemmatize(word))
