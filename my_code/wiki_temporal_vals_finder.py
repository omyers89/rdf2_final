import csv
from string import rsplit
from threading import Thread, Lock
from time import sleep
from miner_base import *

DBPEDIA_URL = "http://tdk3.csf.technion.ac.il:8890/sparql"
DBPEDIA_URL_UP = "http://dbpedia.org/sparql"
WIKI_DAT_URL = "https://query.wikidata.org/bigdata/namespace/wdq/sparql"

QUICK = False
DEBUG = False

"""
the idea of this module is to find the properties that in new versions of DBpedia has objects related to them that are
not exist in the previous sets.

"""

prop_temp_dict = {}

d_lock = Lock()


def get_suspected_temporal_types():
    global prop_temp_dict
    sparql = SPARQLWrapper(WIKI_DAT_URL)
    query_text = ("""
                PREFIX wd: <http://www.wikidata.org/entity/>
                PREFIX wdt: <http://www.wikidata.org/prop/direct/>
                PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
                
                SELECT DISTINCT ?prop WHERE {
                  ?person wdt:P31 wd:Q5.
                  ?person ?prop ?obj.
                  ?obj pq:P580 ?starttime.
                  ?obj pq:P582 ?endtime.
                }
                LIMIT 140""")
    sparql.setQuery(query_text)
    sparql.setReturnFormat(JSON)
    results_inner = sparql.query().convert()
    for inner_res in results_inner["results"]["bindings"]:
        if "prop" in inner_res:
            prop = inner_res["prop"]["value"]
            prop_temp_dict[prop] = {'tot':0, 'st_count':0, 'db_prop':"", 'res':0}
    print len(prop_temp_dict)



def count_st_count_for_prop(prop):
    cnt=0
    sparql = SPARQLWrapper(WIKI_DAT_URL)
    #TODO: make end time temporal and add point in time
    query_text = ("""
                    PREFIX p: <http://www.wikidata.org/prop/>
                    PREFIX wd: <http://www.wikidata.org/entity/>
                    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
                    PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
                    SELECT (COUNT(?person) AS ?cnt) WHERE {
                      ?person wdt:P31 wd:Q5.
                      ?person p:%s ?obj.
                      ?obj pq:P580 ?starttime.
                      ?obj pq:P582 ?endtime.
                        
                    }""" % prop)
    sparql.setQuery(query_text)
    sparql.setReturnFormat(JSON)
    results_inner = sparql.query().convert()
    for inner_res in results_inner["results"]["bindings"]:
        if "cnt" in inner_res:
            cnt = int(inner_res["cnt"]["value"])
            # prop_temp_dict[prop]['st_count'] = cnt
    if DEBUG:
        print "prop: ", prop, "cnt: ", cnt
    return cnt


def count_tot_for_prop(prop):
    tot=0
    sparql = SPARQLWrapper(WIKI_DAT_URL)
    query_text = ("""
                    PREFIX p: <http://www.wikidata.org/prop/>
                    PREFIX wd: <http://www.wikidata.org/entity/>
                    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
                    PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
                    SELECT (COUNT(?person) AS ?cnt) WHERE {
                      ?person wdt:P31 wd:Q5.
                      ?person p:%s ?obj.
                    }""" % prop)
    sparql.setQuery(query_text)
    sparql.setReturnFormat(JSON)
    results_inner = sparql.query().convert()
    for inner_res in results_inner["results"]["bindings"]:
        if "cnt" in inner_res:
            tot = int(inner_res["cnt"]["value"])
            # prop_temp_dict[prop]['tot'] = tot
    if DEBUG:
        print "prop: ", prop, "tot: ", tot
    return tot

def make_prop_temp_dict():

    get_suspected_temporal_types()
    iterp = 0
    thread_dict = {}
    j = 0
    i=0
    for p_uri in prop_temp_dict:
        i+=1
        j+=1
        p = get_subj_from_uri(p_uri)
        if p != "P26" and QUICK:
            continue
        t = Thread(target=update_prop_dict, args=(p_uri,p, ))
        thread_dict[i] = t
        t.start()
        if j > 8:
            for ih, th in thread_dict.items():
                th.join()
            thread_dict = {}
            j = 0
        iterp +=1
        print iterp
        if QUICK and iterp > 3:
            break
    for ih, th in thread_dict.items():
        th.join()

def update_prop_dict(p_uri, p):
    cnt = count_st_count_for_prop(p)
    tot = count_tot_for_prop(p)
    prop_temp_dict[p_uri]["tot"] = tot
    prop_temp_dict[p_uri]["st_count"] = cnt
    db_prop = get_wiki_prop_from_db_uri(p)
    prop_temp_dict[p_uri]["db_prop"] = db_prop
    res = -1
    if tot != 0:
        res = float(cnt) / tot
    prop_temp_dict[p_uri]["res"] = res


def get_subj_from_uri(uri_strin):
    subj = rsplit(uri_strin, "/")[-1]
    return subj

def get_wiki_prop_from_db_uri(wiki_uri):
    '''
    :param s: specific subject uri
    :return: dictionary of property-object
    '''
    sdb=""
    sparql = SPARQLWrapper(DBPEDIA_URL)
    query_text = ("""
        PREFIX wd: <http://www.wikidata.org/entity/>
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        SELECT DISTINCT ?sdb  WHERE {
            ?sdb owl:equivalentProperty wd:%s .
        }""" % (wiki_uri))
    sparql.setQuery(query_text)
    sparql.setReturnFormat(JSON)
    results_inner = sparql.query().convert()
    for inner_res in results_inner["results"]["bindings"]:
        if "sdb" in inner_res:
            sdb = inner_res["sdb"]["value"]

    return sdb


def print_res_dict_to_csv(res_dict):
    csvp_name = "../results/CSVS/temporal_res_dict.csv"
    with open(csvp_name, 'w') as csvfile:
        fieldnames = ['wiki_uri', 'dbp_uri', 'res', 'tot']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for wiki_key, rd in res_dict.items():
            wiki_uri = (wiki_key).encode('utf-8')
            dbp_uri = rd['db_prop'].encode('utf-8').strip()
            res = rd['res']
            tot = rd['tot']
            data = {'wiki_uri': wiki_uri, 'dbp_uri':dbp_uri, 'res':res, 'tot':tot}
            writer.writerow(data)

    csvfile.close()



if __name__ == "__main__":
    if len(sys.argv) > 1:
        QUICK = True
    make_prop_temp_dict()
    print_res_dict_to_csv(prop_temp_dict)