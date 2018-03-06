import csv

from miner_base import *

DBPEDIA_URL_UP = "http://dbpedia.org/sparql"

"""
TODO:
1. check if dumps exists
2. if not run complete dumps.
3. check if related objects exists 
4. print 10 examples of subjects and related objects for props
5. *** check with wikidata for properties that has some hints for being temporal: start time.

"""

def get_subj_objs_for_prop():
    s_dict_file = open("../dumps/p_top_s_dict_dbp.dump", 'r')
    p_top_s_dict = pickle.load(s_dict_file)
    s_dict_file.close()
    mn = MinerBase(DBPEDIA_URL_UP)

    diff_dump_name = "../dumps/diff_props_dbp.dump"
    diff_dict_file = open(diff_dump_name, 'r')
    p_filtered_dict = pickle.load(diff_dict_file)
    diff_dict_file.close()

    with open("../results/CSVS/prop_summ.csv", 'w') as csvfile1:
        fieldnames = ['prop', 'subj', 'obj', 'diff', 'tot']
        writer = csv.DictWriter(csvfile1, fieldnames=fieldnames)
        writer.writeheader()
        for p, s_dict in p_top_s_dict.items():
            for s in s_dict:
                objs = mn.get_objects_WT_for_s_p(p, s)
                p_uni = p
                subj = s
                obj = "obj"
                if len(objs) >0:
                    obj = objs[0].encode('utf-8')
                diff = "diff"
                tot = "tot"
                if p in p_filtered_dict:
                    diff = p_filtered_dict[p]["diff"]
                    tot = p_filtered_dict[p]["tot"]

                data = {'prop': p_uni, 'subj':subj, 'obj':obj, 'diff':diff, 'tot':tot}
                print data
                writer.writerow(data)
                break
    csvfile1.close()




if __name__ == '__main__':
    get_subj_objs_for_prop()
