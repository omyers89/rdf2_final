from SPARQLWrapper import SPARQLWrapper, JSON
import os
import pickle
import sys
from miner_base import MinerBase


DBPEDIA_URL_UP= "http://dbpedia.org/sparql"
DBPEDIA_URL = "http://tdk3.csf.technion.ac.il:8890/sparql"

DEBUG = False
QUICK = False

DBT = ["any", "dbp", "dbo"]

def get_top_1_percent(i, top_s_dict,uri,p, f_limit = 1000):
    # CHECKED !
    sparql = SPARQLWrapper(DBPEDIA_URL)

    limit = 10000
    offset = i * limit
    s_f_limit = str(f_limit)

    slimit = str(limit)
    soffset = str(offset)
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
                    OFFSET %s
                }
                ?s ?p ?o.
            }
        }
    } GROUP BY ?s
    ORDER BY DESC(?scnt)
    LIMIT %s""" % (uri,p,slimit, soffset, s_f_limit))

    sparql.setQuery(query_text)
    sparql.setReturnFormat(JSON)
    results_inner = sparql.query().convert()
    all_dict = results_inner["results"]["bindings"]
    for inner_res in all_dict:
        s = (inner_res["s"]["value"]).encode('utf-8').strip()
        cnt = inner_res["scnt"]["value"]
        top_s_dict[s] = cnt

    if DEBUG:
        sys.stdout.write("\b in  get_top_1_percent len(all_dict):{}, ofset:{} done".format(len(all_dict), soffset))
        sys.stdout.write("\r")
        sys.stdout.flush()
    if len(all_dict) > 10:
        return True
    return False


def get_f_limits(uri, p):
    # CHECKED !
    cnt = 1
    sparql = SPARQLWrapper(DBPEDIA_URL)
    query_text = ("""
        SELECT (COUNT(*)AS ?scnt)
        WHERE
        {
            {
                SELECT DISTINCT ?s
                WHERE
                {
                    ?s a <%s>.
                    ?s <%s> ?o
                }
            }
        }
        """ % (uri, p))
    # print query_text
    sparql.setQuery(query_text)
    sparql.setReturnFormat(JSON)
    results_inner = sparql.query().convert()
    all_dict = results_inner["results"]["bindings"]
    for inner_res in all_dict:
        cnt = inner_res["scnt"]["value"]

    #check approx how many round need total subjects over 10000 objects tested pe round
    r = float(cnt)/ float(10000)

    #if cnt is less than 10000 set number of rounds to 1 otherwise to round up.
    num_of_rounds = max(round(r), 1)

    #we want total of 2000 so every round take only 2000/num_of_rounds
    lim_per_round = 2000/num_of_rounds
    # just to make sure we dont miss anyone

    if DEBUG:
        print " limit: " ,int(lim_per_round), " count: ", int(cnt)

    return num_of_rounds, int(lim_per_round), int(cnt)


def get_top_15_props( ps, n=2000):
    # CHECKED !
    p_dict_ret = {}
    for i, p in enumerate(ps):
        cur = ps[p]
        if i > n:
            m = min(p_dict_ret, key=p_dict_ret.get)
            if (p_dict_ret[m] < cur):
                if DEBUG:
                    print "in get top 15 props: ", m,":", p_dict_ret[m], " was popped, " , p,":", cur, " was entered"
                p_dict_ret.pop(m, None)
                p_dict_ret[p] = int(cur)
        else:
            p_dict_ret[p] = int(cur)
    return p_dict_ret


def get_all_top_of(db_t):
    if db_t not in DBT:
        print "DBT Error"
        return
    # CHECKED !
    uri = "http://dbpedia.org/ontology/Person"
    print "in get_all_top_of:"
    dump_name = "../dumps/top_200_props_"+ db_t + ".dump"
    if not os.path.exists(dump_name):
        return
    p_dict_file = open(dump_name, 'r')
    all_ps = pickle.load(p_dict_file)
    p_dict_file.close()

    p_res_all_s = {}

    dbg_count = 0
    for p in all_ps:
        if DEBUG:
            sys.stdout.write("\b DBG count is:{}".format(dbg_count))
            sys.stdout.write("\r")
            sys.stdout.flush()
        pu = p.strip()
        dbg_count +=1
        top_subjects = {}
        try:
            rounds, limits, maxs = get_f_limits(uri,pu)
            i = 0
            flag = get_top_1_percent(i, top_subjects, uri,pu,limits)
            while flag:
                i += 1
                flag = get_top_1_percent(i, top_subjects, uri,pu,limits)
                if i>rounds or i*limits>maxs or (QUICK and i> 5):
                    flag = False
        except Exception as e:
            print "error in p:", pu, e

        p_res_all_s[pu]=get_top_15_props(top_subjects)

        s_dict_file = open("../dumps/p_top_s_dict_" + db_t + ".dump", 'w')
        pickle.dump(p_res_all_s, s_dict_file)
        s_dict_file.close()

        if QUICK and dbg_count > 20:
            break

##################################### deprecated #####################
def get_all_top_of_any(uri ="http://dbpedia.org/ontology/Person"):
    # CHECKED !

    print "in get_all_top_of_any:"
    dump_name = "../dumps/top_200_props_any.dump"
    if not os.path.exists(dump_name):
        return
    p_dict_file = open(dump_name, 'r')
    all_ps = pickle.load(p_dict_file)
    p_dict_file.close()

    p_res_all_s = {}

    dbg_count = 0
    for p in all_ps:
        if DEBUG:
            sys.stdout.write("\b DBG count is:{}".format(dbg_count))
            sys.stdout.write("\r")
            sys.stdout.flush()
        pu = p.strip()
        dbg_count +=1
        top_subjects = {}
        try:
            rounds, limits, maxs = get_f_limits(uri,pu)
            i = 0
            flag = get_top_1_percent(i, top_subjects, uri,pu,limits)
            while flag:
                i += 1
                flag = get_top_1_percent(i, top_subjects, uri,pu,limits)
                if i>rounds or i*limits>maxs or (QUICK and i> 5):
                    flag = False
        except Exception as e:
            print "error in p:", pu, e

        p_res_all_s[pu]=get_top_15_props(top_subjects)

        s_dict_file = open("../dumps/p_top_s_dict_any.dump", 'w')
        pickle.dump(p_res_all_s, s_dict_file)
        s_dict_file.close()

        if QUICK and dbg_count > 20:
            break


def get_all_top_of_dbp(uri = "http://dbpedia.org/ontology/Person"):
    # ** same as above just for DBP
    # CHECKED !

    print "in get_all_top_of_dbp:"
    dump_name = "../dumps/top_200_props_dbp.dump"
    if not os.path.exists(dump_name):
        return
    p_dict_file = open(dump_name, 'r')
    all_ps = pickle.load(p_dict_file)
    p_dict_file.close()

    p_res_all_s = {}

    dbg_count = 0
    for p in all_ps:
        if DEBUG:
            sys.stdout.write("\b DBG count is:{}".format(dbg_count))
            sys.stdout.write("\r")
            sys.stdout.flush()
        pu = p.strip()
        dbg_count +=1
        top_subjects = {}
        try:
            rounds, limits, maxs = get_f_limits(uri,pu)
            i = 0
            flag = get_top_1_percent(i, top_subjects, uri,pu,limits)
            while flag:
                i += 1
                flag = get_top_1_percent(i, top_subjects, uri,pu,limits)
                if i>rounds or i*limits>maxs or (QUICK and i> 5):
                    flag = False
        except Exception as e:
            print "error in p:", pu, e

        p_res_all_s[pu]=get_top_15_props(top_subjects)

        s_dict_file = open("../dumps/p_top_s_dict_dbp.dump", 'w')
        pickle.dump(p_res_all_s, s_dict_file)
        s_dict_file.close()

        if QUICK and dbg_count > 20:
            break


def get_all_top_of_dbo(uri = "http://dbpedia.org/ontology/Person"):
    # ** same as above just for DBP
    # CHECKED !

    print "in get_all_top_of_dbo:"
    dump_name = "../dumps/top_200_props_dbo.dump"
    if not os.path.exists(dump_name):
        return
    p_dict_file = open(dump_name, 'r')
    all_ps = pickle.load(p_dict_file)
    p_dict_file.close()

    p_res_all_s = {}

    dbg_count = 0
    for p in all_ps:
        if DEBUG:
            sys.stdout.write("\b DBG count is:{}".format(dbg_count))
            sys.stdout.write("\r")
            sys.stdout.flush()
        pu = p.strip()
        dbg_count +=1
        top_subjects = {}
        try:
            rounds, limits, maxs = get_f_limits(uri,pu)
            i = 0
            flag = get_top_1_percent(i, top_subjects, uri,pu,limits)
            while flag:
                i += 1
                flag = get_top_1_percent(i, top_subjects, uri,pu,limits)
                if i>rounds or i*limits>maxs or (QUICK and i> 5):
                    flag = False
        except Exception as e:
            print "error in p:", pu, e

        p_res_all_s[pu]=get_top_15_props(top_subjects)

        s_dict_file = open("../dumps/p_top_s_dict_dbo.dump", 'w')
        pickle.dump(p_res_all_s, s_dict_file)
        s_dict_file.close()

        if QUICK and dbg_count > 20:
            break
########################################################################


def get_best_props_dbp(i, top_p_dict, uri ="http://dbpedia.org/ontology/Person", f_limit = 200):
    # *** deprecated
    sparql = SPARQLWrapper(DBPEDIA_URL)

    limit = 100000
    offset = i * limit
    s_f_limit = str(f_limit)

    slimit = str(limit)
    soffset = str(offset)

    query_text = ("""
                SELECT ?p (COUNT (?p) AS ?cnt)
                WHERE {
                        {
                        SELECT DISTINCT ?s ?p
                        WHERE {
                            ?s a <%s>;
                                ?p ?o.
                            ?o a ?t
                        FILTER regex(?p, "^http://dbpedia.org/property", "i")
                    }LIMIT %s
                    OFFSET %s
                }
                }GROUP BY ?p
                 ORDER BY DESC(?cnt)
                 LIMIT %s
                """ % (uri, slimit, soffset, s_f_limit))
    sparql.setQuery(query_text)
    sparql.setReturnFormat(JSON)
    results_inner = sparql.query().convert()

    t_count = 0
    for inner_res in results_inner["results"]["bindings"]:
        p = (inner_res["p"]["value"]).encode('utf-8').strip()
        cnt = inner_res["cnt"]["value"]
        if p not in top_p_dict:
            top_p_dict[p] = 0
        top_p_dict[p] += int(cnt)
        t_count += int(cnt)
    if DEBUG:
        sys.stdout.write("\b in  get_best_props_dbp len(all_dict):{}, ofset:{} done".format(len(top_p_dict), soffset))
        sys.stdout.write("\r")
        sys.stdout.flush()
    return t_count>=200


def get_best_props_dbo(i, top_p_dict, uri ="http://dbpedia.org/ontology/Person", f_limit = 200):
    # *** deprecated
    sparql = SPARQLWrapper(DBPEDIA_URL)

    limit = 100000
    offset = i * limit
    s_f_limit = str(f_limit)

    slimit = str(limit)
    soffset = str(offset)

    query_text = ("""
                SELECT ?p (COUNT (?p) AS ?cnt)
                WHERE {
                        {
                        SELECT DISTINCT ?s ?p
                        WHERE {
                            ?s a <%s>;
                                ?p ?o.
                            ?o a ?t
                        FILTER regex(?p, "^http://dbpedia.org/ontology", "i")
                    }LIMIT %s
                    OFFSET %s
                }
                }GROUP BY ?p
                 ORDER BY DESC(?cnt)
                 LIMIT %s
                """ % (uri, slimit, soffset, s_f_limit))
    sparql.setQuery(query_text)
    sparql.setReturnFormat(JSON)
    results_inner = sparql.query().convert()

    t_count = 0
    for inner_res in results_inner["results"]["bindings"]:
        p = (inner_res["p"]["value"]).encode('utf-8').strip()
        cnt = inner_res["cnt"]["value"]
        if p not in top_p_dict:
            top_p_dict[p] = 0
        top_p_dict[p] += int(cnt)
        t_count += int(cnt)
    if DEBUG:
        sys.stdout.write("\b in  get_best_props_dbp len(all_dict):{}, ofset:{} done".format(len(top_p_dict), soffset))
        sys.stdout.write("\r")
        sys.stdout.flush()
    return t_count>=200


def get_best_prop_any(i, top_p_dict, uri ="http://dbpedia.org/ontology/Person", f_limit = 200):
    # CHECKED !
    sparql = SPARQLWrapper(DBPEDIA_URL)

    limit = 100000
    offset = i * limit
    s_f_limit = str(f_limit)

    slimit = str(limit)
    soffset = str(offset)

    # get any P and take only the best 200
    query_text = ("""
                SELECT ?p (COUNT (?p) AS ?cnt)
                WHERE {
                        {
                        SELECT DISTINCT ?s ?p
                        WHERE {
                            ?s a <%s>;
                                ?p ?o.
                            ?o a ?t
                    }LIMIT %s
                    OFFSET %s
                }
                }GROUP BY ?p
                 ORDER BY DESC(?cnt)
                 LIMIT %s
                """ % (uri, slimit, soffset, s_f_limit))
    sparql.setQuery(query_text)
    sparql.setReturnFormat(JSON)
    results_inner = sparql.query().convert()

    t_count = 0
    for inner_res in results_inner["results"]["bindings"]:
        p = (inner_res["p"]["value"]).encode('utf-8').strip()
        cnt = inner_res["cnt"]["value"]
        if p not in top_p_dict:
            top_p_dict[p] = 0
        top_p_dict[p] += int(cnt)
        t_count += int(cnt)
    if DEBUG:
        sys.stdout.write("\b in  get_best_props_dbp len(all_dict):{}, ofset:{} done".format(len(top_p_dict), soffset))
        sys.stdout.write("\r")
        sys.stdout.flush()
    return t_count>=200


def get_all_top_props_any(uri ="http://dbpedia.org/ontology/Person", dir_name="person"):
    # CHECKED !
    print "in get_all_top_props_any"
    p_dict={}
    top_num = 200
    iter_flag = True
    # number of rouns ehch round check 100000 * 35 rounds that is 3.5 M persons.
    r=35
    if QUICK:
        r=2
    for i in range(r):
        if DEBUG:
            sys.stdout.write("\b i is: {} ".format(i))
            sys.stdout.write("\r")
            sys.stdout.flush()
        if iter_flag:
            try:
                iter_flag = get_best_prop_any(i, p_dict, uri, top_num)
            except Exception as e:
                print "error in iter: " ,  i , " exception: ",e

    # save results:
    dump_name = "all_top_prop_any.dump"
    dir_name = "../dumps/"
    p_dict_file = open(dir_name + dump_name, 'w')
    pickle.dump(p_dict, p_dict_file)
    p_dict_file.close()


def get_all_top_props_dbp(uri ="http://dbpedia.org/ontology/Person", dir_name="person"):
    #** same as above just for DBP
    # CHECKED !
    print "in get_all_top_props_dbp"
    p_dict={}
    top_num = 200
    iter_flag = True
    # number of rouns ehch round check 100000 * 35 rounds that is 3.5 M persons.
    r=35
    if QUICK:
        r=2
    for i in range(r):
        if DEBUG:
            sys.stdout.write("\b i is: {} ".format(i))
            sys.stdout.write("\r")
            sys.stdout.flush()
        if iter_flag:
            try:
                iter_flag = get_best_props_dbp(i, p_dict, uri, top_num)
            except Exception as e:
                print "error in iter: " ,  i , " exception: ",e

    # save results:
    dump_name = "all_top_prop_dbp.dump"
    dir_name = "../dumps/"
    p_dict_file = open(dir_name + dump_name, 'w')
    pickle.dump(p_dict, p_dict_file)
    p_dict_file.close()


def get_all_top_props_dbo(uri ="http://dbpedia.org/ontology/Person", dir_name="person"):
    #** same as above just for DBP
    # CHECKED !
    print "in get_all_top_props_dbo"
    p_dict={}
    top_num = 200
    iter_flag = True
    # number of rouns ehch round check 100000 * 35 rounds that is 3.5 M persons.
    r=35
    if QUICK:
        r=2
    for i in range(r):
        if DEBUG:
            sys.stdout.write("\b i is: {} ".format(i))
            sys.stdout.write("\r")
            sys.stdout.flush()
        if iter_flag:
            try:
                iter_flag = get_best_props_dbo(i, p_dict, uri, top_num)
            except Exception as e:
                print "error in iter: " ,  i , " exception: ",e

    # save results:
    dump_name = "all_top_prop_dbo.dump"
    dir_name = "../dumps/"
    p_dict_file = open(dir_name + dump_name, 'w')
    pickle.dump(p_dict, p_dict_file)
    p_dict_file.close()


def get_best_200_props_any():
    # CHECKED !

    print "get_best_200_props_any"
    # load dict of all props
    p_dict_file = open("../dumps/all_top_prop_any.dump", 'r')
    p_dict = pickle.load(p_dict_file)
    p_dict_file.close()

    p_list = p_dict.items()
    sorted_by_second = sorted(p_list, key=lambda tup: tup[1])

    final_list = sorted_by_second[-200:]
    p_dict_best = dict(final_list)

    dump_name = "top_200_props_any.dump"
    dir_name = "../dumps/"
    p_dict_file = open(dir_name + dump_name, 'w')
    pickle.dump(p_dict_best, p_dict_file)
    p_dict_file.close()


def get_best_200_props_dbp():
    # ** same as above just for DBP
    # CHECKED !

    print "get_best_200_props_any"
    # load dict of all props
    p_dict_file = open("../dumps/all_top_prop_dbp.dump", 'r')
    p_dict = pickle.load(p_dict_file)
    p_dict_file.close()

    p_list = p_dict.items()
    sorted_by_second = sorted(p_list, key=lambda tup: tup[1])

    final_list = sorted_by_second[-200:]
    p_dict_best = dict(final_list)

    dump_name = "top_200_props_dbp.dump"
    dir_name = "../dumps/"
    p_dict_file = open(dir_name + dump_name, 'w')
    pickle.dump(p_dict_best, p_dict_file)
    p_dict_file.close()


def get_best_200_props_dbo():
    # ** same as above just for DBO
    # CHECKED !

    print "get_best_200_props_any"
    # load dict of all props
    p_dict_file = open("../dumps/all_top_prop_dbo.dump", 'r')
    p_dict = pickle.load(p_dict_file)
    p_dict_file.close()

    p_list = p_dict.items()
    sorted_by_second = sorted(p_list, key=lambda tup: tup[1])

    final_list = sorted_by_second[-200:]
    p_dict_best = dict(final_list)

    dump_name = "top_200_props_dbo.dump"
    dir_name = "../dumps/"
    p_dict_file = open(dir_name + dump_name, 'w')
    pickle.dump(p_dict_best, p_dict_file)
    p_dict_file.close()


def validate_all_top_of(db_t):
    if db_t not in DBT:
        print "DBT Error"
        return
    p_dict_file = open("../dumps/p_top_s_dict_" + db_t + ".dump", 'r')
    p_dict_best = pickle.load( p_dict_file)
    p_dict_file.close()

    mb = MinerBase(DBPEDIA_URL)
    for p,s_dict in p_dict_best.items():
        for s in s_dict:
            obl = mb.get_objects_WT_for_s_p(p, s.strip())
            if len(obl) == 0:
                print "bad prop:",p ,"s:", s


if __name__ == '__main__':
    if len(sys.argv) > 1:
        QUICK = True
        print "quick"
    else:
        print "not quick"
    #
    # get_all_top_props_any()
    # get_best_200_props_any()
    # get_all_top_of_any()
    get_all_top_props_dbo()
    get_best_200_props_dbo()
    get_all_top_of("dbo")
    if DEBUG:
        validate_all_top_of("dbo")











