import csv
import re
from string import rsplit
from threading import Thread, Lock
from time import sleep
from miner_base import *

DBPEDIA_URL = "http://tdk3.csf.technion.ac.il:8890/sparql"
DBPEDIA_URL_UP = "http://dbpedia.org/sparql"

QUICK = False
DEBUG = False

"""
the idea of this module is to find the properties that in new versions of DBpedia has objects related to them that are
not exist in the previous sets.

"""

prop_dif_dict = {}

d_lock = Lock()


def atomic_prop_dif_update(p, local_prop_dif_dict):
    global prop_dif_dict
    global d_lock
    d_lock.acquire()
    if p not in prop_dif_dict:
        prop_dif_dict[p] = {"diff": 0, "tot": 0, "miss": 0}
    prop_dif_dict[p]["tot"] += local_prop_dif_dict["tot"]
    prop_dif_dict[p]["diff"] += local_prop_dif_dict["diff"]
    prop_dif_dict[p]["miss"] += local_prop_dif_dict["miss"]
    d_lock.release()

def get_subj_from_uri( uri_strin):
    subj = rsplit(uri_strin, "/")[-1]
    return subj


def get_clean_obj_list(full_obj_list):
    clean_obj_list = []
    for fo in full_obj_list:
        after_slash = get_subj_from_uri(fo)
        temp_obj_list = after_slash.split(",")
        for o in temp_obj_list:
            o = o.replace("_", " ")
            o = o.strip()
            clean_obj_list.append(o)
    return clean_obj_list


def obj_ok(ob):
    num = r'^(.*)[0-9]+(.*)$'
    re_num = re.compile(num, re.I)
    if re_num.match(ob):
        return False
    return True



def t_comp(s, p, miner_up, miner_old):
    try:
        obj_list_new = miner_up.get_objects_NT_for_s_p(p, s)
        obj_list_old = miner_old.get_objects_NT_for_s_p(p, s)
        clean_obj_list_new = get_clean_obj_list(obj_list_new)
        clean_obj_list_old = get_clean_obj_list(obj_list_old)
        #print "obj old: ", clean_obj_list_old
        #print "obj new: ", clean_obj_list_new

    except:

        print "Error in t_comp"
        sleep(2)
        return
    local_prop_dif_dict = {"diff": 0, "tot": 0, "miss": 0}
    if len(clean_obj_list_old) == 0 or len(clean_obj_list_new) == 0:
        local_prop_dif_dict["miss"] = 1
        # if DEBUG and QUICK:
        #     print "in t_comp, miss in s: ", s
        atomic_prop_dif_update(p, local_prop_dif_dict)
        return
    for on in clean_obj_list_new:
        if not obj_ok(on):
            continue
        local_prop_dif_dict["tot"] = 1
        if on not in clean_obj_list_old:
            local_prop_dif_dict["diff"] = 1
            if DEBUG:
                print "in t_comp, diff in s: ", s, "p is: ",p
                print "obj old: ", clean_obj_list_old
                print "obj new: ", clean_obj_list_new
            break

    atomic_prop_dif_update(p,local_prop_dif_dict)




def comp_small():

    s_dict_file = open("../dumps/p_top_s_dict_dbp.dump", 'r')
    p_top_s_dict = pickle.load(s_dict_file)
    s_dict_file.close()

    bdg_count=0
    for p, s_dict in p_top_s_dict.items():
        if p in prop_dif_dict and prop_dif_dict[p]['tot'] != 0:
            continue
        bdg_count += 1

        txt = "\b P loop progress: {}".format(bdg_count)
        sys.stdout.write(txt)
        sys.stdout.write("\r")
        sys.stdout.flush()
        comp_diff_for_prop(p,s_dict)

        if QUICK and bdg_count > 5:
            break

    dump_name = "../dumps/diff_props_dbp_all.dump"
    p_dict_file = open(dump_name, 'w')
    pickle.dump(prop_dif_dict, p_dict_file)
    p_dict_file.close()






def comp_diff_for_prop(prop_uri,s_dict):
    thread_dict = {}
    j = 0
    i = 0
    bdg_count = 0
    pu = prop_uri.strip()

    bdg_count += 1
    miner_up = MinerBase(DBPEDIA_URL_UP)
    miner_old = MinerBase(DBPEDIA_URL)
    for s in s_dict:
        su = s.strip()
        i += 1
        j += 1
        t = Thread(target=t_comp, args=(su, pu, miner_up, miner_old,))
        thread_dict[i] = t
        t.start()

        if j > 10:
            for ih, th in thread_dict.items():
                th.join()
            thread_dict = {}
            j = 0
            if QUICK and i > 40:
                break
    for ih, th in thread_dict.items():
        th.join()


def retry_from_dump():
    global prop_dif_dict
    dump_name = "../dumps/diff_props_dbp_all.dump"
    p_dict_file = open(dump_name, 'r')
    prop_dif_dict = pickle.load(p_dict_file)
    p_dict_file.close()
    comp_small()



def debug_get_diff_for_prop(p_uri):
    global QUICK
    global DEBUG

    QUICK = True
    DEBUG = True

    miner_up = MinerBase(DBPEDIA_URL_UP)
    miner_old = MinerBase(DBPEDIA_URL)

    s_dict_file = open("../dumps/p_top_s_dict_dbp.dump", 'r')
    p_top_s_dict = pickle.load(s_dict_file)
    s_dict_file.close()
    comp_diff_for_prop(p_uri, p_top_s_dict[p_uri])


def print_diff_to_csv():
    diff_dump_name = "../dumps/diff_props_dbp_all.dump"
    diff_dict_file = open(diff_dump_name, 'r')
    p_diff_dict = pickle.load(diff_dict_file)
    diff_dict_file.close()
    csvp_name = "../results/CSVS/diff_props_all.csv"
    with open(csvp_name, 'w') as csvfile2:
        fieldnames = ['uri', 'diff', 'tot', 'miss']
        writer = csv.DictWriter(csvfile2, fieldnames=fieldnames)
        writer.writeheader()
        for p, c in p_diff_dict.items():
            subj_uri = p
            data = c
            c['uri'] = subj_uri
            writer.writerow(data)

    csvfile2.close()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        QUICK = True
    #debug_get_diff_for_prop('http://dbpedia.org/property/spouse')
    # for k, v in prop_dif_dict.items():
    #     print k, v
    retry_from_dump()
    #print_diff_to_csv()