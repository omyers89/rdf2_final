from SPARQLWrapper import SPARQLWrapper, JSON
import pickle
import sys
import os
import time


DBPEDIA_URL_UP = "http://dbpedia.org/sparql"

def LOG(s):
    print s

class MinerBase:

    def __init__(self,kb ):
        self.knowledge_base = kb
        #self.subject = subj
        #self.subject_uri = s_uri
        self.sparql = SPARQLWrapper(kb)

############GET DATA FROM DUMP ################
    def __get_top_15_props(self, ps, n=5):
        p_dict_ret = {}
        for i, p in enumerate(ps):
            cur = ps[p]
            p_dict_ret[p] = int(cur)
            if i > n:
                m = min(p_dict_ret, key=p_dict_ret.get)
                p_dict_ret.pop(m, None)
        return p_dict_ret


    def get_p_dict_from_dump(self,quick, dump_name, nx=-1):
        """
        :param quick: boolean - for quick debug checks
        :param dump_name: full path to the dump file to load
        :return: list of propertiesp to work with
        """
        p_dict_file = open(dump_name, 'r')
        p_dict = pickle.load(p_dict_file)
        p_dict_file.close()

        if nx != -1 :
            return self.__get_top_15_props(p_dict, n=nx)
        if quick:
            return self.__get_top_15_props(p_dict, n=10)
        else:
            return self.__get_top_15_props(p_dict, n=100)




    def check_if_s_got_p(self, s, p):
        local_sprql = SPARQLWrapper(self.knowledge_base)
        res_list = []
        local_sprql.setQuery("""
                    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

                    SELECT  ?o
                    WHERE{
                      <%s>  <%s>   ?o.
                        }
                    } """ % (s, p))

        # need to filter the types to informative ones.
        local_sprql.setReturnFormat(JSON)
        results = local_sprql.query().convert()

        for result in results["results"]["bindings"]:
            o = result["t"]["value"]
            res_list.append(o)

        return len(res_list) > 0



    def get_s_dict_from_dump(self, quick, dump_name, nx=-1):
        """
        :param quick: boolean - for quick debug checks
        :param dump_name: full path to the dump file to load
        :return: list of subjects to work with
        """

        s_dict_file = open(dump_name, 'r')
        s_dict = pickle.load(s_dict_file)

        s_dict_file.close()
        if nx != -1 :
            return self.__get_top_15_props(s_dict, n=nx)
        if quick:
            return self.__get_top_15_props(s_dict, n=50)
        else:
            return self.__get_top_15_props(s_dict, n=5000)

    ##########################################################


##################  WORK WITH OBJECTS ############################


    def get_ot_unique_dict(self, o_list, o_dict_t):
        res_dict = {}
        # single= False
        # if len(os) == 1:
        #     single = True
        for o in o_list:
            if o in o_dict_t:
                for t in o_dict_t[o]:
                    #if (t in res_dict) or single:
                    if t in res_dict:
                        res_dict[t] = False #this is the second time t in res_dict so not unique!
                    else:
                        res_dict[t] = True #this is the first time t in res_dict so unique so far!
        return res_dict

    def get_objects_WT_for_s_p(self, p, s):
        """
        given s-subject and p-property return a list of objects that are related to the subject.
        :param p: a property (dbo: or dbp:)
        :param s: a specific subject
        :return: list of objects [o1, o2, o3, .. ] where (s, p, o_i) is in the KB and o_i has a rdf type!
        """
        local_sprql = SPARQLWrapper(self.knowledge_base)
        pu = p.encode('utf-8')
        o_list = []
        query_text = ("""
                    SELECT DISTINCT ?o
                    WHERE{
                            <%s> <%s> ?o .
                            ?o a ?t .
                        } """ % (s, pu))
        #FILTER (regex(?t, "^http://dbpedia.org/")) maybe removed
        # I figured out that a good filter for the type of the object has to  be of "^http://dbpedia.org/ontology"
        # in oreder to get valuable results
        local_sprql.setQuery(query_text)
        local_sprql.setReturnFormat(JSON)
        results_inner = local_sprql.query().convert()
        for inner_res in results_inner["results"]["bindings"]:
            # s = inner_res["s"]["value"]
            o = inner_res["o"]["value"]
            o_list.append(o)

        return o_list

    def get_objects_NT_for_s_p(self, p, s):
        """
        given s-subject and p-property return a list of objects that are related to the subject.
        :param p: a property (dbo: or dbp:)
        :param s: a specific subject
        :return: list of objects [o1, o2, o3, .. ] where (s, p, o_i) is in the KB
        """
        local_sprql = SPARQLWrapper(self.knowledge_base)

        o_list = []
        query_text = ("""
                    SELECT DISTINCT ?o
                    WHERE{
                            <%s> <%s> ?o .
                        } """ % (s, p))
        #FILTER (regex(?t, "^http://dbpedia.org/")) maybe removed
        # I figured out that a good filter for the type of the object has to  be of "^http://dbpedia.org/ontology"
        # in oreder to get valuable results
        local_sprql.setQuery(query_text)
        local_sprql.setReturnFormat(JSON)
        results_inner = local_sprql.query().convert()
        for inner_res in results_inner["results"]["bindings"]:
            # s = inner_res["s"]["value"]
            o = inner_res["o"]["value"]
            o_list.append(o)

        return o_list


    def get_p_and_objs_for_s(self, s):
        """
                given s-subject  return a list of properties and the objects that are related to the subject.
                :param s: a specific subject
                :return: dictionary of properties where every value list of objects [o1, o2, o3, .. ] where (s, p, o_i) is in the KB
                """
        local_sprql = SPARQLWrapper(self.knowledge_base)
        p_dict_o_list = {}
        query_text = ("""
                            SELECT ?p ?o
                            WHERE{
                                    <%s> ?p ?o .
                                    ?o a ?t .
                                } """ % s)
        # FILTER (regex(?t, "^http://dbpedia.org/")) maybe removed
        # I figured out that a good filter for the type of the object has to  be of "^http://dbpedia.org/ontology"
        # in oreder to get valuable results
        local_sprql.setQuery(query_text)
        local_sprql.setReturnFormat(JSON)
        results_inner = local_sprql.query().convert()
        for inner_res in results_inner["results"]["bindings"]:
            p = (inner_res["p"]["value"]).encode('utf-8')
            o = (inner_res["o"]["value"]).encode('utf-8')
            if p not in p_dict_o_list:
                p_dict_o_list[p] = []
            p_dict_o_list[p].append(o)

        return p_dict_o_list


    def get_min_rdf_types_for_o(self, o_list):
        """
        Given list of object and a specific knowledge base creates a dictionary of o and the list of dbo:type that
        defines it

        :param o_list: list of object for specific relation
        :param db: the KB we query
        :return: o_dict dictionar {'<object>' : [c1,c2,c3...] (type list)
        """
        local_sprql = SPARQLWrapper(self.knowledge_base)
        o_dict = {}
        for o in o_list:
            o_dict[o] = []
            local_sprql.setQuery("""
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

            SELECT  DISTINCT ?t
            WHERE{
              <%s>  a   ?t .
                FILTER (regex(?t, "^http://dbpedia.org/", "i"))
                FILTER NOT EXISTS {
                    ?subtype ^a ?o ;
                    rdfs:subClassOf ?t .
                    FILTER ( ?subtype != ?t )
                }
            } """ % o)

            #need to filter the types to informative ones.
            local_sprql.setReturnFormat(JSON)
            results = local_sprql.query().convert()

            for result in results["results"]["bindings"]:
                c = result["t"]["value"]
                o_dict[o].append(c)
        return o_dict

    def get_all_rdf_types_for_o(self, o_list):
        """
        Given list of object and a specific knowledge base creates a dictionary of o and the list of dbo:type that
        defines it

        :param o_list: list of object for specific relation
        :param db: the KB we query
        :return: o_dict dictionar {'<object>' : [c1,c2,c3...] (type list)
        """
        local_sprql = SPARQLWrapper(self.knowledge_base)
        o_dict = {}
        for o in o_list:
            o_dict[o] = []
            local_sprql.setQuery("""
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

            SELECT  DISTINCT ?t
            WHERE{
              <%s>  a   ?t .
                FILTER (regex(?t, "^http://dbpedia.org/ontology", "i"))
            } """ % o)

            #need to filter the types to informative ones.
            local_sprql.setReturnFormat(JSON)
            results = local_sprql.query().convert()

            for result in results["results"]["bindings"]:
                c = result["t"]["value"]
                o_dict[o].append(c)
        return o_dict

    def get_dbo_types_for_o(self, o_list):

        """
        Given list of object and a specific knowledge base creates a dictionary of o and the list of dbo:type that
        defines it

        :param o_list: list of object for specific relation
        :param db: the KB we query
        :return: o_dict dictionar {'<object>' : [c1,c2,c3...] (type list)
        """
        local_sprql = SPARQLWrapper(self.knowledge_base)
        o_dict = {}
        for o in o_list:
            o_dict[o] = []
            local_sprql.setQuery("""
                                SELECT  DISTINCT  ?c
                                WHERE{
                                    <%s>  <http://dbpedia.org/ontology/type>   ?c .
                                    FILTER regex(?c, "^http://dbpedia.org", "i")
                                }
                            """ % o)

            # need to filter the types to informative ones.
            local_sprql.setReturnFormat(JSON)
            results = local_sprql.query().convert()

            for result in results["results"]["bindings"]:
                c = result["c"]["value"]
                o_dict[o].append(c)
        return o_dict

##############################################################

