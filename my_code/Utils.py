from json import JSONEncoder
from string import rsplit

import exceptions

from SPARQLWrapper import SPARQLWrapper, JSON

from my_code.miner_base import MinerBase

DBPEDIA_URL_UP = "http://dbpedia.org/sparql"
class GraphObjectEncoder(JSONEncoder):
    def default(self, o):
        return o.__dict__

subjectsPerson = {'person': "http://dbpedia.org/ontology/Person"}

def get_subj_from_uri( uri_strin):
    subj = rsplit(uri_strin, "/")[-1]
    return subj


def get_related_objects_from_uri(subj_uri, prop_uri):
    mm = MinerBase(DBPEDIA_URL_UP)
    try:
        list_of_related_objects = mm.get_objects_WT_for_s_p(prop_uri, subj_uri)
        if len(list_of_related_objects) == 0:
            list_of_related_objects = mm.get_objects_NT_for_s_p(prop_uri, subj_uri)
            if len(list_of_related_objects) == 0:
                ont_prop = "http://dbpedia.org/ontology/" + get_subj_from_uri(prop_uri)
                list_of_related_objects = mm.get_objects_WT_for_s_p(ont_prop, subj_uri)
                if len(list_of_related_objects) == 0:
                    list_of_related_objects = mm.get_objects_NT_for_s_p(ont_prop, subj_uri)
    except exceptions.Exception:
        print "sparql error... "
        return []
    names_ = [get_subj_from_uri(x) for x in list_of_related_objects]
    names = [n.replace('_', ' ') for n in names_]
    return list_of_related_objects, names

def get_lable_for_obj(ouri):
    local_sprql = SPARQLWrapper(DBPEDIA_URL_UP)
    ouriu = ouri.encode('utf-8')
    l_list = []
    query_text = ("""
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                SELECT distinct ?l  WHERE {
                    <%s> rdfs:label ?l . 
                    FILTER (langMatches( lang(?l), "en" )).
                } """ % ouriu)
    local_sprql.setQuery(query_text)
    local_sprql.setReturnFormat(JSON)
    results_inner = local_sprql.query().convert()
    for inner_res in results_inner["results"]["bindings"]:
        # s = inner_res["s"]["value"]
        l = inner_res["l"]["value"]
        l_list.append(l)

    return l_list

# def LOG(prow):
#     log_file_name = "../results/log.txt"
#     with open(log_file_name, "a") as myfile:
#         myfile.write(str(prow)  + "\n")


def make_unicode(inp):
    if type(inp) != unicode:
        inp =  inp.decode('utf-8')
        return inp
    else:
        return inp


if __name__ == "__main__":
    listt = get_lable_for_obj("http://dbpedia.org/resource/Donald_Trump")
    for l in listt:
        print l
