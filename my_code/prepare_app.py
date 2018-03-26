import csv

from miner_base import *

DBPEDIA_URL_UP = "http://dbpedia.org/sparql"

"""
TODO:
1. prepare training data and dump it.
    training data:
    a. take filtered list from csv.
    b. merge all relevant features by feature dictionary
2. make function that create model with one feature family out.
3. CV with 70/30 or 80/20
4. print stats and confusion matrix.
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

def get_classified_prop_dict(file_suffix="dbo"):
    classified_properies_dict = {}
    full_path = os.path.realpath(__file__)
    path, filename = os.path.split(full_path)
    all_path = os.path.join(path, '../results/CSVS/all_features_' + file_suffix + '.csv')
    with open(all_path, mode='r') as infile:
        reader = csv.reader(infile)
        for row in reader:
            if "tag" in row:
                continue
            #prob = float(row[1])
            tag = int(row[1])
            if tag == -1:
                continue
            if row[0] == "-1" :
                break
            seg = 1
            classified_properies_dict[row[0]] = {'seg': seg, 'class': tag, 'obj_sat_f': {}, 'obj_type_f':{}, 'time_prep_f':{}}
    return classified_properies_dict





def make_training_data():
    classified_properies_dict = get_classified_prop_dict()


if __name__ == '__main__':
    get_subj_objs_for_prop()
