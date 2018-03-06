import csv
import re

from miner_base import *
from string import rsplit
from threading import Lock, Thread
import sys

DBPEDIA_URL_UP = "http://dbpedia.org/sparql"
DEBUG = False
quick_param = False
FULL = True

DBT = ["any", "dbp", "dbo"]
f_lock = Lock()

class NewDistFeatureMiner(MinerBase):

    def __init__(self, kb, subj, s_uri, file_suff="dbo"):
        MinerBase.__init__(self,kb)
        self.subject = subj
        self.subject_uri = s_uri
        self.p_count = 0
        self.p_only_one_counter = 0
        self.p_multy_objs_same_type_counter = 0
        self.p_objs_unique_type_counter = 0
        self.p_has_primitive_type = 0
        self.file_suffix = file_suff
        self.feature_types_init = {"http://dbpedia.org/ontology/Person":0,
                               "http://dbpedia.org/ontology/Organisation":0,
                                    "http://dbpedia.org/ontology/Company":0,
                                    "http://dbpedia.org/ontology/SportsTeam":0,
                                    "http://dbpedia.org/ontology/EducationalInstitution":0,
                                    "http://dbpedia.org/ontology/PoliticalParty":0,
                                "http://dbpedia.org/ontology/Work":0,
                                   "http://dbpedia.org/ontology/Document":0,
                                   "http://dbpedia.org/ontology/MusicalWork":0,
                                   "http://dbpedia.org/ontology/Film":0,
                                   "http://dbpedia.org/ontology/Event":0,
                                   "http://dbpedia.org/ontology/SocietalEvent":0,
                                   "http://dbpedia.org/ontology/Activity":0,
                                   "http://dbpedia.org/ontology/Place":0,
                                   "http://dbpedia.org/ontology/PopulatedPlace":0,
                                   "http://dbpedia.org/ontology/Country":0,
                                   "http://dbpedia.org/ontology/ArchitecturalStructure":0,
                                   "http://dbpedia.org/ontology/NaturalPlace":0}

        self.p_type_counters = {"http://dbpedia.org/ontology/Person":0,
                               "http://dbpedia.org/ontology/Organisation":0,
                                    "http://dbpedia.org/ontology/Company":0,
                                    "http://dbpedia.org/ontology/SportsTeam":0,
                                    "http://dbpedia.org/ontology/EducationalInstitution":0,
                                    "http://dbpedia.org/ontology/PoliticalParty":0,
                                "http://dbpedia.org/ontology/Work":0,
                                    "http://dbpedia.org/ontology/Document":0,
                                    "http://dbpedia.org/ontology/MusicalWork":0,
                                    "http://dbpedia.org/ontology/Film":0,
                                "http://dbpedia.org/ontology/Event":0,
                                    "http://dbpedia.org/ontology/SocietalEvent":0,
                                "http://dbpedia.org/ontology/Activity":0,
                                "http://dbpedia.org/ontology/Place":0,
                                    "http://dbpedia.org/ontology/PopulatedPlace":0,
                                        "http://dbpedia.org/ontology/Country":0,
                                    "http://dbpedia.org/ontology/ArchitecturalStructure":0,
                                    "http://dbpedia.org/ontology/NaturalPlace":0}

    def mine_features(self, quick):
        # CHECKED !
        s_dict_file = open("../dumps/p_top_s_dict_" + self.file_suffix +".dump", 'r')
        p_top_s_dict = pickle.load(s_dict_file)
        s_dict_file.close()

        dump_name = "../dumps/prop_features_" + self.file_suffix +".dump"
        if not os.path.exists(dump_name):
            feature_dictionary = {}
        else:
            r_dict_file = open(dump_name, 'r')
            feature_dictionary = pickle.load(r_dict_file)
            r_dict_file.close()

        # diff_dump_name = "../dumps/diff_props_" + self.file_suffix +"_all.dump"
        # diff_dict_file = open(diff_dump_name, 'r')
        # p_filtered_dict = pickle.load(diff_dict_file)
        # diff_dict_file.close()
        p_filtered_dict = self.load_filtered_props()

        p_indx = 0
        missed_ps = []
        for p, s_dicts in p_top_s_dict.items():
            if (p in feature_dictionary) or (p not in p_filtered_dict and not FULL) :
                print p , " - skipped"
                continue
            if DEBUG and not (p == "http://dbpedia.org/ontology/birthPlace" or p =="http://dbpedia.org/ontology/spouse"):
                continue
            try:
                feature_dictionary[p] = self.get_fetures_for_prop(quick, p, s_dicts)
            except:
                missed_ps.append(p)

            sys.stdout.write("\b p #{} done".format(p_indx))
            sys.stdout.write("\r")
            sys.stdout.flush()
            p_indx += 1

            dump_name = "../dumps/prop_features_" + self.file_suffix +".dump"
            r_dict_file = open(dump_name, 'w')
            pickle.dump(feature_dictionary, r_dict_file)
            r_dict_file.close()

            if quick and p_indx > 2:
                break
        return feature_dictionary, missed_ps


    def load_filtered_props(self):
        if FULL:
            return {}
        classified_properies_dict = {}
        full_path = os.path.realpath(__file__)
        path, filename = os.path.split(full_path)
        all_path = os.path.join(path, '../results/CSVS/fetures_class_sum_'+self.file_suffix+ '.csv')
        with open(all_path, mode='r') as infile:
            reader = csv.reader(infile)
            for row in reader:
                classified_properies_dict[row[0]] = 1
        return classified_properies_dict

    def get_subj_from_uri(self,uri_strin):
        tsubj = rsplit(uri_strin,"/")[-1]
        return tsubj

    def atomic_counter_inc(self,  p_count, p_multy_objs_same_type_counter, p_objs_unique_type_counter,
                           p_only_one_counter, p_has_primitive_type,type_dict_for_prop):
        # CHECKED !
        global f_lock
        f_lock.acquire()
        self.p_count += p_count
        self.p_multy_objs_same_type_counter += p_multy_objs_same_type_counter
        self.p_objs_unique_type_counter += p_objs_unique_type_counter
        self.p_only_one_counter += p_only_one_counter
        self.p_has_primitive_type += p_has_primitive_type
        for k,v in type_dict_for_prop.items():
            self.p_type_counters[k] += v
        f_lock.release()


    def get_fetures_for_prop(self, quick, prop_uri,s_dict, nx=-1):
        # CHECKED !
        print "mining features for {}".format(prop_uri)
        max_s_iter = 1000 if nx == -1 else nx
        self.p_count = 0
        self.p_only_one_counter = 0
        self.p_multy_objs_same_type_counter = 0
        self.p_objs_unique_type_counter = 0
        self.p_has_primitive_type = 0
        self.p_type_counters = self.feature_types_init.copy()
        thread_dict = {}
        j=0
        for i, s in enumerate(s_dict):
            j+=1
            t = Thread(target=self.update_counter, args=(prop_uri, s,))
            thread_dict[i] = t
            t.start()

            if DEBUG:
                txt = "\b S loop progress: {}".format(i)
                sys.stdout.write(txt)
                sys.stdout.write("\r")
                sys.stdout.flush()
            if j > 10:
                for ih, th in thread_dict.items():
                    th.join()
                thread_dict = {}
                j = 0
                if DEBUG:
                    print "flushed:"
                    print self.p_count, ";", self.p_multy_objs_same_type_counter, ";", self.p_objs_unique_type_counter, ";", self.p_only_one_counter, ";", self.p_has_primitive_type
                if self.p_count >=max_s_iter:
                    break
            if quick and i > 100:
                break
        for ih, th in thread_dict.items():
            th.join()

        if self.p_count > 0:
            feature_dict = {"p_count": self.p_count,
                                "p_only_one_counter": float(self.p_only_one_counter) / self.p_count,
                                 "p_multy_objs_same_type_counter": float(self.p_multy_objs_same_type_counter) / self.p_count,
                                 "p_objs_unique_type_counter": float(self.p_objs_unique_type_counter) / self.p_count,
                                 "p_has_primitive_type": float(self.p_has_primitive_type) / self.p_count}
        else:
            feature_dict = {"p_count": self.p_count,
                                "p_only_one_counter": -1,
                                            "p_multy_objs_same_type_counter": -1,
                                            "p_objs_unique_type_counter": -1,
                                            "p_has_primitive_type": -1}
        feature_dict["p_type_counters"] = self.p_type_counters.copy()
        print "final"
        print self.p_count, ";",self.p_multy_objs_same_type_counter, ";",self.p_objs_unique_type_counter,";", self.p_only_one_counter, ";", self.p_has_primitive_type
        return feature_dict

    def update_counter(self, prop_uri, s):
        #CHECKED !
        p_count = 0
        p_multy_objs_same_type_counter=0
        p_objs_unique_type_counter=0
        p_only_one_counter=0
        p_has_primitive_type = 0
        o_list_WT = self.get_objects_WT_for_s_p(prop_uri, s)
        o_list_NT = self.get_objects_NT_for_s_p(prop_uri, s)
        if len(o_list_NT) > 0 and len(o_list_WT) == 0:
            p_count = 1
            p_has_primitive_type = 1
            if len(o_list_NT) > 1:
                p_multy_objs_same_type_counter = 1
            else:
                p_only_one_counter = 1

            if len(o_list_WT) == 0:
                self.atomic_counter_inc(p_count, p_multy_objs_same_type_counter, p_objs_unique_type_counter,
                                        p_only_one_counter, p_has_primitive_type,{})
                return
        if len(o_list_WT) > 0:
            # means that there is at least one object related to the subject.
            p_count = 1
        obj_rdf_types_dict = self.get_min_rdf_types_for_o(o_list_WT)
        if len(o_list_WT) > 1:
            # check if there is only one object of every type
            dbo_t_uniques = self.check_multiple_vals_same_type(o_list_WT, obj_rdf_types_dict)  # for specific person and property find the unique types!

            if not dbo_t_uniques:
                p_multy_objs_same_type_counter = 1
            else:
                p_objs_unique_type_counter = 1
                # if not unique then there are multiple objects of the same type.

        # taking care of the first feature
        elif len(o_list_WT) == 1:
            p_only_one_counter = 1

        obj_rdf_all_types = self.get_all_rdf_types_for_o(o_list_WT)
        type_dict_for_prop = self.get_type_dit_for_prop(obj_rdf_all_types)
        self.atomic_counter_inc(p_count, p_multy_objs_same_type_counter, p_objs_unique_type_counter, p_only_one_counter, p_has_primitive_type,type_dict_for_prop)


    def get_type_dit_for_prop(self, obj_rdf_all_types):
        tmp_type_dic_for_prop = self.feature_types_init.copy()
        for o,ts in obj_rdf_all_types.items():
            for t in ts:
                if t in self.feature_types_init:
                    tmp_type_dic_for_prop[t] = 1
        return tmp_type_dic_for_prop


    def check_multiple_vals_same_type(self, o_list, o_dict_t):
        # CHECKED !
        res_dict = {}
        for o in o_list:
            if o in o_dict_t:
                for t in o_dict_t[o]:
                    # if (t in res_dict) or single:
                    if t in res_dict:
                        res_dict[t] = False  # this is the second time t in res_dict so not unique!
                        return False
                    else:
                        res_dict[t] = True  # this is the first time t in res_dict so unique so far!
        return True

    def get_top_for_prop(self,p,uri="http://dbpedia.org/ontology/Person", f_limit=1000):
        # CHECKED !
        sparql = SPARQLWrapper(DBPEDIA_URL_UP)
        limit = 100000
        s_f_limit = str(f_limit)

        slimit = str(limit)

        query_text = ("""
        SELECT ?s(COUNT(*)AS ?scnt)
        WHERE
        {
            {
                SELECT DISTINCT ?s ?p
                WHERE
                {
                    {
                        SELECT DISTINCT ?s
                        WHERE
                        {
                        ?s a <%s>;
                        <%s> ?o.
                        } LIMIT %s
                    }
                    ?s ?p ?o.
                }
            }
        } GROUP BY ?s
        ORDER BY DESC(?scnt)
        LIMIT %s""" % (uri, p, slimit,  s_f_limit))

        sparql.setQuery(query_text)
        sparql.setReturnFormat(JSON)
        results_inner = sparql.query().convert()
        all_dict = results_inner["results"]["bindings"]
        top_s_dict={}

        for inner_res in all_dict:
            s = (inner_res["s"]["value"]).encode('utf-8').strip()
            cnt = inner_res["scnt"]["value"]
            top_s_dict[s] = cnt

        return top_s_dict


def print_features_to_csv(file_suffix="dbo"):
    diff_dump_name = "../dumps/prop_features_" + file_suffix +".dump"
    diff_dict_file = open(diff_dump_name, 'r')
    p_feture_dict = pickle.load(diff_dict_file)
    diff_dict_file.close()
    csvp_name = "../results/CSVS/fetures_" + file_suffix +".csv"
    with open(csvp_name, 'w') as csvfile2:
        fieldnames = ['uri', 'p_only_one_counter', 'p_multy_objs_same_type_counter', 'p_objs_unique_type_counter',
                      'p_has_primitive_type',
                      "http://dbpedia.org/ontology/Person",
                      "http://dbpedia.org/ontology/Organisation",
                      "http://dbpedia.org/ontology/Company",
                      "http://dbpedia.org/ontology/SportsTeam",
                      "http://dbpedia.org/ontology/EducationalInstitution",
                      "http://dbpedia.org/ontology/PoliticalParty",
                      "http://dbpedia.org/ontology/Work",
                      "http://dbpedia.org/ontology/Document",
                      "http://dbpedia.org/ontology/MusicalWork",
                      "http://dbpedia.org/ontology/Film",
                      "http://dbpedia.org/ontology/Event",
                      "http://dbpedia.org/ontology/SocietalEvent",
                      "http://dbpedia.org/ontology/Activity",
                      "http://dbpedia.org/ontology/Place",
                      "http://dbpedia.org/ontology/PopulatedPlace",
                      "http://dbpedia.org/ontology/Country",
                      "http://dbpedia.org/ontology/ArchitecturalStructure",
                      "http://dbpedia.org/ontology/NaturalPlace",
                      'p_count']
        writer = csv.DictWriter(csvfile2, fieldnames=fieldnames)
        writer.writeheader()
        for p, c in p_feture_dict.items():
            p_uri = (p).encode('utf-8')
            data = c

            data['uri'] = p_uri
            for pp, vv in c['p_type_counters'].items():
                data[pp] = vv
            data.pop('p_type_counters', None)
            writer.writerow(data)

    csvfile2.close()

if __name__ == "__main__":
    # # this script will mine all features of all properties of person
    # if len(sys.argv) > 1:
    #     quick_param = True
    # FM = NewDistFeatureMiner(DBPEDIA_URL_UP, 'person', "http://dbpedia.org/ontology/Person")
    # fd, missed = FM.mine_features(quick=quick_param)
    # if len(missed) > 0:
    #     # try again:
    #     fd, missed = FM.mine_features(quick=quick_param)
    #
    # print "tried twice ps left:", missed
    print_features_to_csv()
    #features = FM.get_fetures_for_prop(False, "http://dbpedia.org/ontology/spouse", 200)
    #print_features_to_csv()

    #time words:
