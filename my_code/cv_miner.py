from SPARQLWrapper import SPARQLWrapper, JSON

import cProfile
import pickle, pstats, StringIO
import sys
import os
import time
from threading import Thread
from Utils import *
from miner_base import MinerBase
DBPEDIA_URL = "http://tdk3.csf.technion.ac.il:8890/sparql"
DBPEDIA_URL_UP = "http://dbpedia.org/sparql"
SMAL_URL = "http://cultura.linkeddata.es/sparql"
DEBUG = False
PROFILER = True


def LOG(s):
    print s


class Cv_Miner(MinerBase):
    def __init__(self, kb, subj, s_uri):
        MinerBase.__init__(self, kb)
        self.subject = subj
        self.subject_uri = s_uri
        self.cv_props = {}
        self.hs_profiler = cProfile.Profile()

    def get_single_os(self, s):
        '''
        :param s: specific subject uri
        :return: dictionary of property-object
        '''

        single_po_dict = {}
        query_text = ("""
            SELECT DISTINCT ?p1 ?o1 WHERE
            {
                {
                    SELECT ?p1 (count (distinct ?o) as ?cnt1) WHERE
                    {
                        <%s> ?p1 ?o.
                    } GROUP BY ?p1
                }
                FILTER (?cnt1 = 1)
                <%s> ?p1 ?o1.
                ?o1 a ?t.
            }""" % (s, s))


        self.sparql.setQuery(query_text)
        self.sparql.setReturnFormat(JSON)
        results_inner = self.sparql.query().convert()
        for inner_res in results_inner["results"]["bindings"]:
            p1 = inner_res["p1"]["value"]
            o1 = inner_res["o1"]["value"]
            if p1 in single_po_dict:
                LOG("{} is not single".format(p1))
                single_po_dict.pop(p1)
            else:
                single_po_dict[p1] = o1

        return single_po_dict

    def get_multy_os(self, s):
        '''
            :param s: specific subject uri
            :return: dictionary of property-objectList
            '''

        multy_po_dict = {}
        query_text = ("""
                SELECT DISTINCT ?p2 ?o2 WHERE
                {
                    {
                        SELECT ?p2 (count (distinct ?o) as ?cnt2) WHERE
                        {
                            <%s> ?p2 ?o.
                        } GROUP BY ?p2
                    }
                    FILTER (?cnt2 > 1)
                    <%s> ?p2 ?o2.
                    ?o2 a ?t.
                }""" % (s, s))

        self.sparql.setQuery(query_text)
        self.sparql.setReturnFormat(JSON)
        results_inner = self.sparql.query().convert()
        for inner_res in results_inner["results"]["bindings"]:
            p2 = inner_res["p2"]["value"]
            o2 = inner_res["o2"]["value"]
            if p2 not in multy_po_dict:
                multy_po_dict[p2] = []
            multy_po_dict[p2].append(o2)

        return multy_po_dict


    def update_cv_props(self, single_po, multy_po):
        for p, o in single_po.items():
            for pm, ol in multy_po.items():
                if (p, pm) not in self.cv_props:
                    self.cv_props[(p, pm)] = (0, 0)
                c,t = self.cv_props[(p,pm)]
                if o in ol:
                    self.cv_props[(p,pm)] = (c+1,t+1)
                else:
                    self.cv_props[(p, pm)] = (c,t+1)



    def mine_cv_rules(self,quick, TH=0.85):
        self.hs_profiler.enable()
        print "mining rules for {}".format(self.subject)
        s_dump_name = "../results/" + self.subject + "/" + self.subject + "_top.dump"
        p_dump_name = "../results/" + self.subject + "/" + self.subject + "_prop.dump"
        # get the 100 most popular properties for type person in dbp
        p_dict = self.get_p_dict_from_dump(quick, p_dump_name)
        s_dict = self.get_s_dict_from_dump(quick, s_dump_name)
        one_and_only = {}  # A dictionary for the percentage of props that has one value of any type related to them.
        multiple_values_same_type = {}  # A dictionary for the percentage of props that has multiple values of the same type
        one_of_a_type = {}
        rules50_60 = {}  # A dic
        rules_wierd = {}
        rules_wierd_dbo = {}
        one_of_a_kind = {}
        low_props = {}
        progress = 0
        p_size = len(p_dict)
        t0 = time.time()
        p_indx = 0


        #### for p_dict need to get only ps that are in the single value rules.
        for s in s_dict:
            #single_po: dictionary of {p: o}
            single_po = self.get_single_os(s)
            # multy_po: dictionary of {p: [o1, o2, o3...]}
            multy_po = self.get_multy_os(s)

            self.update_cv_props(single_po, multy_po)
        res_dict={}
        for (p,pm), (c ,t) in self.cv_props.items():
            ratio = (float(c) / t)
            if ratio > TH:
                res_dict[(p,pm)] = ratio
        self.hs_profiler.disable()
        return res_dict

    def get_stats(self):
        s = StringIO.StringIO()
        sortby = 'cumulative'
        ps = pstats.Stats(self.hs_profiler, stream=s).sort_stats(sortby)
        ps.print_stats()
        print s.getvalue()

if __name__ == '__main__':

    # from find_inconsistencies import fix_graphic
    DEBUG = True
    quick = True
    db = DBPEDIA_URL

    for d in [{'politician': "http://dbpedia.org/ontology/Politician"}]:

        # for d in dictionariesq:
        for s, suri in d.items():
            # t = Thread(target=mine_all_rules, args=(DBPEDIA_URL, s, suri, quick,))
            # t.start()
            cvm = Cv_Miner(DBPEDIA_URL_UP, s, suri)
            res = cvm.mine_cv_rules(quick)
            print 'profiling: {}'.format(s)
            cvm.get_stats()
            dump_name = "../results/" + s + "/" + s + "_cv_dict.dump"
            p_file = open(dump_name, 'w')
            pickle.dump(res, p_file)
            p_file.close()

