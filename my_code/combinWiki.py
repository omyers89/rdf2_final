from string import rsplit
import csv
from miner_base import *
import codecs
import exceptions

DBPEDIA_URL = "http://dbpedia.org/sparql"
WIKI_DAT_URL = "https://query.wikidata.org/bigdata/namespace/wdq/sparql"
DEBUG = False



name = r'([A-Z][a-z]+.)+'
prop = r'([A-Za-z])+'
year = r'^(.*)[0-9][0-9][0-9][0-9](.*)$'
date = r'[A-Z][a-z]+ [0-9][0-9], [0-9][0-9][0-9][0-9]'

months = ['january', 'february', 'march',  'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december']

class rel_wikiData_finder:
    def __init__(self, isubj, isubj_db_uri, iinc_path):
        self.subj = isubj
        self.mm = MinerBase(DBPEDIA_URL_UP)
        self.inc_path = iinc_path
        self.true_dict = {}
        self.fix_truth = {}

    def LOG(self, prow):
        log_file_name = "../results/" + self.subj + "/" + self.subj + "_cmbi_log.txt"
        if DEBUG:
            print prow
        else:
            with open(log_file_name, "a") as myfile:
                myfile.write(str(prow) + "\n")

    def get_subj_from_uri(self, uri_strin):
        subj = rsplit(uri_strin, "/")[-1]
        return subj

    def get_related_objects_from_uri(self, row):

        try:
            list_of_related_objects = self.mm.get_objects_WT_for_s_p(row[1], row[0])
        except (exceptions.Exception):
            self.LOG ( "sparql error... ")
            return []
        return list_of_related_objects


    def get_wiki_subj_from_db_uri(self, db_uri):
        '''
        :param s: specific subject uri
        :return: dictionary of property-object
        '''
        sparql = SPARQLWrapper(DBPEDIA_URL)
        sa_list = []
        query_text = ("""
            PREFIX owl: <http://www.w3.org/2002/07/owl#>
            SELECT ?sa WHERE {
            <%s> owl:sameAs ?sa.
            filter regex(?sa, "^http://www.wikidata.org" , "i").
            }""" % (db_uri))
        sparql.setQuery(query_text)
        sparql.setReturnFormat(JSON)
        results_inner = sparql.query().convert()
        for inner_res in results_inner["results"]["bindings"]:
            sa = inner_res["sa"]["value"]
            sa_list.append(sa)
        return sa_list

    def get_wiki_prop_from_db_uri(self, db_uri):
        '''
        :param s: specific subject uri
        :return: dictionary of property-object
        '''
        sparql = SPARQLWrapper(DBPEDIA_URL)
        pr_list = []
        query_text = ("""
            PREFIX owl: <http://www.w3.org/2002/07/owl#>
            SELECT ?sa WHERE {
            <%s> owl:equivalentProperty ?sa.
            filter regex(?sa, "^http://www.wikidata.org" , "i").
            }""" % (db_uri))
        sparql.setQuery(query_text)
        sparql.setReturnFormat(JSON)
        results_inner = sparql.query().convert()
        for inner_res in results_inner["results"]["bindings"]:
            sa = inner_res["sa"]["value"]
            pr_list.append(sa)
        return pr_list

    def get_wikis(self, subj_uri, prop_uri, related_obj_list):
        self.LOG( ["Subject: ", subj_uri, "Prop: ", prop_uri])
        #true_dict[(subj_uri, prop_uri)] = None
        wikiSubj = self.get_wiki_subj_from_db_uri(subj_uri)
        wiki_prop = self.get_wiki_prop_from_db_uri(prop_uri)

        other_obj_wiki_list = []
        for x in related_obj_list:
            wiki_objs = self.get_wiki_subj_from_db_uri(x)
            if len(wiki_objs) == 1:
                other_obj_wiki_list.append(wiki_objs[0])

        if len(wikiSubj) > 1 or len(wiki_prop) > 1:
            self.LOG("some wikiss")
        if len(wikiSubj) == 0 or len(wiki_prop) == 0:
            self.LOG("zero wikiss")
            return

        return wikiSubj[0], self.get_subj_from_uri(wiki_prop[0]), other_obj_wiki_list

    def start_end_queri(self, w_subj, w_prop, w_obj):

        sparql = SPARQLWrapper(WIKI_DAT_URL)
        sa_list = {"start_time":"un-known start time", "end_time":"un-known end time" }
        query_text = ("""
            PREFIX wd: <http://www.wikidata.org/entity/>
            PREFIX wikibase: <http://wikiba.se/ontology#>
            PREFIX p: <http://www.wikidata.org/prop/>
            PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
            PREFIX bd: <http://www.bigdata.com/rdf#>
            PREFIX s: <http://www.wikidata.org/prop/statement/>
    
            SELECT ?starttime ?endtime
            WHERE
            {
                <%s> p:%s ?statement.
                    ?statement s:%s <%s> .
                ?statement pq:P580 ?starttime.
                OPTIONAL{?statement pq:P582 ?endtime.}
                SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
            }""" % (w_subj,w_prop,w_prop, w_obj))
        sparql.setQuery(query_text)
        sparql.setReturnFormat(JSON)
        results_inner = sparql.query().convert()
        for inner_res in results_inner["results"]["bindings"]:
            if "starttime" in inner_res:
                sa_list["start_time"] = inner_res["starttime"]["value"]
            if "endtime" in inner_res:
                sa_list["end_time"] = inner_res["endtime"]["value"]

        if len(results_inner["results"]["bindings"]) > 1:
           self.LOG("erroor in start_end_queri")
        return sa_list["start_time"], sa_list["end_time"]


    def get_all_start_end(self, w_subj, w_prop, w_obj):
        start = "un-known start time"
        end = "un-known end time"
        reg_start, reg_end = self.start_end_queri(w_subj, w_prop, w_obj)
        rev_start, rev_end = self.start_end_queri(w_obj, w_prop, w_subj)
        if reg_start != "un-known start time" or rev_start == "un-known start time" or reg_start == rev_start :
            start = reg_start
        else:
            start = rev_start

        if reg_end != "un-known end time" or rev_end == "un-known end time" or reg_end == rev_end :
            end = reg_end
        else:
            end = rev_end

        return start, end


    def example(self, subj, prop,objs):
        wikis = self.get_wikis(subj, prop,objs)
        if wikis == None:
            return
        ws,wp,wo_list = wikis
        s = self.get_subj_from_uri(subj)
        p = self.get_subj_from_uri(prop)

        if len(wo_list) != len(objs):
            self.LOG("obj list length not equal")
            return
        self.true_dict[(s,p)]= {}
        for wo,dbo in zip(wo_list,objs):
            o = self.get_subj_from_uri(dbo)
            st, ed = self.get_all_start_end(ws, wp, wo)
            self.true_dict[(s,p)][o]={"start_time":st, "end_time": ed}

    def write_truth_to_csv(self, subj_name, dicts, load=False):
        if load:
            rf_name = "../dumps/" + "/" + subj_name + "_truth_single_wikidata.dump"
            if not os.path.exists(rf_name):
                return
            incs_file = open(rf_name, 'r')
            incos = pickle.load(incs_file)
            incs_file.close()
        else:
            incos = dicts
        csvf_name = "../results/" + subj_name + "/" + subj_name + "_truth_single_wiki.csv"
        with open(csvf_name, 'w') as csvfile:
            fieldnames = ['subject','property', 'current_or_latest', 'start_time']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for (su,pu), (pt,stm) in incos.items():
                uni_su = su.encode('utf-8')
                uni_pu = pu.encode('utf-8')
                cur = pt.encode('utf-8')
                tm = stm.encode('utf-8')
                writer.writerow({'subject': uni_su, 'property': uni_pu, 'current_or_latest': cur,'start_time': tm})
                    # print {'subj': uni_su, 'p1': uni_p1, 'p2': uni_p2}

        csvfile.close()

    def get_latest_from_true_dict(self, v_dict):
        """
        the function gets a dictionay with start and end time of the related objects:
        returns the one with the latest start time
        """
        max = ""
        max_obj = ""
        for k,v in v_dict.items():
            if max == "" and v["start_time"] != "un-known start time":
                max = v["start_time"]
                max_obj = k
            elif max != "" and v["start_time"] != "un-known start time" and max < v["start_time"]:
                max = v["start_time"]
                max_obj = k
        return max_obj, max

    def auto_fix(self):
        self.LOG( 'auto_fix')
        violation_dict = {}
        with codecs.open(self.inc_path, mode='r', encoding=None, errors='replace', buffering=1) as csvfile:
            spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
            i = 0
            for row in spamreader:

                row = ', '.join(row).split(',')
                self.LOG( row)
                if len(row) != 2 or ('\\' in row[0]) or i== 0:
                        i+=1
                        continue
                violation_dict[tuple(row)] = {}
                violation_dict[tuple(row)]['subjet_title'] = subj = row[0]
                violation_dict[tuple(row)]['property'] = prop = row[1]
                violation_dict[tuple(row)]['related_objects'] =  objs = self.get_related_objects_from_uri(row)
                if len(objs) == 0:
                    self.LOG ( "continued")
                    i += 1
                    continue
                #if subj == 'Zara_Bate':
                #    print 'Barbara_Follett_(politician)'
                #    example(subj, prop,objs, true_dict)
                try:
                    self.example(subj, prop, objs)
                except:
                    self.LOG ( "bad example")
                i += 1
                if i % 10 == 0:
                    sys.stdout.write("\b iter number: {}".format(i))
                    sys.stdout.write("\r")
                    sys.stdout.flush()
                if i > 40 and DEBUG:
                    break

        dir_name = "../dumps"
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
        dump_name = self.subj + "_truth_single_wikidata.dump"
        inc_file = open(dir_name + "/" + dump_name, 'w')
        pickle.dump(self.true_dict, inc_file)
        inc_file.close()

        for k,v in self.true_dict.items():
            #returns the current or latest object
            try:
                cur_late, stm = self.get_latest_from_true_dict(v)
            except:
                self.LOG ( "bad latest")
            self.fix_truth[k] = (cur_late, stm)

        self.write_truth_to_csv(self.subj,self.fix_truth,False)

if __name__ == '__main__':
    rwf = rel_wikiData_finder('person',
                          "http://dbpedia.org/ontology/Person",
                          '../results/person/Person_temporal_csv.csv')

    rwf.auto_fix()
    #auto_fix()
    #print response
    #example("Donald_Trump", "Spouse" )
