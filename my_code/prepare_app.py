import csv

from miner_base import *

DBPEDIA_URL_UP = "http://dbpedia.org/sparql"

"""
TODO:
1. prepare training data and dump it.
    training data:
    a. take filtered list from csv. - V
    b. merge all relevant features by feature dictionary - V
2. make function that create model with one feature family out. - V
3. CV with 70/30 or 80/20 - V
4. print stats and confusion matrix. - V
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
    '''

    :param file_suffix: can be dbo, dbp,any
    :return: a dictionary with the filtered list - with only good properties, and tagging
    '''
    classified_properies_dict = {}
    full_path = os.path.realpath(__file__)
    path, filename = os.path.split(full_path)
    all_path = os.path.join(path, '../results/CSVS/all_features_' + file_suffix + '.csv')
    with open(all_path, mode='r') as infile:
        reader = csv.reader(infile)
        for row in reader:
            if "tag" in row:
                continue
            if row[0] == "-1":
                break
            #prob = float(row[1])
            tag = int(row[1])
            if tag == -1:
                continue

            seg = 1
            classified_properies_dict[row[0]] = {'seg': seg, 'class': tag, 'obj_sat_f': {}, 'obj_type_f':{}, 'time_prep_f':{}}
    return classified_properies_dict


def load_features(classified_properies_dict):
    prop_dump_name = "../dumps/prop_features_dbo.dump"
    prop_feature_dict_file = open(prop_dump_name, 'r')
    prop_feature_dict = pickle.load(prop_feature_dict_file)
    prop_feature_dict_file.close()
    pred_dump_name = "../dumps/prop_features_pred.dump"
    pred_feature_dict_file = open(pred_dump_name, 'r')
    pred_feature_dict = pickle.load(pred_feature_dict_file)
    pred_feature_dict_file.close()
    for k,v in classified_properies_dict.items():
        if (k not in prop_feature_dict) or (k not in pred_feature_dict):
            continue
        osf = prop_feature_dict[k].copy()

        pcount = osf['p_count']
        classified_properies_dict[k]['p_count'] = pcount
        osf.pop('p_type_counters', None)
        osf.pop('p_count', None)
        otf = prop_feature_dict[k]['p_type_counters'].copy()
        tpf = pred_feature_dict[k]['pred_dict_counters'].copy()
        if 'http://dbpedia.org/ontology/NaturalEvent' in otf:
            otf.pop('http://dbpedia.org/ontology/NaturalEvent',None)
        for t,f in otf.items():
            otf[t] = float(f)/pcount
        for p,ff in tpf.items():
            tpf[p] = float(ff)/pcount
        classified_properies_dict[k]['obj_sat_f'] = osf
        classified_properies_dict[k]['obj_type_f'] = otf
        classified_properies_dict[k]['time_prep_f'] = tpf
    return classified_properies_dict


# def normalize_features(features_loaded):
#     for k, v in features_loaded.items():
#         for fk, fv in v['obj_sat_f']:
#             v['obj_sat_f'][fk]
#
#         classified_properies_dict[k]['obj_type_f'] = otf
#         classified_properies_dict[k]['time_prep_f'] = tpf


def make_training_data():
    classified_properies_dict = get_classified_prop_dict()
    features_loaded = load_features(classified_properies_dict)
    dump_name = "../dumps/training_set.dump"
    r_dict_file = open(dump_name, 'w')
    pickle.dump(features_loaded, r_dict_file)
    r_dict_file.close()
    return  features_loaded


if __name__ == '__main__':
    features_loaded = make_training_data()
    i=0
    for k,v in features_loaded.items():
        print "i: %d, k: %s, v:" % (i,k,)
        for kk, vv in v.items():
            print kk,":", vv
        i+=1

