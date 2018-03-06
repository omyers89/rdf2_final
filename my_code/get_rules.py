import csv
import pickle
import os
from Utils import dictionaries, dictionariest



def print_rules_to_csv(subj):
   
    
    rf_name = "../results/" + subj + "/" + subj + "_rules.dump"
    if not os.path.exists(rf_name):
        return
    rules_file = open(rf_name, 'r')
    all_rules = pickle.load(rules_file)
    if len(all_rules) < 6: return
   # good, r60_70, r50_60, weird , ons, lows= all_rules
    rules_file.close()
    #all_rules_list = (rules70_, rules60_70, rules70_dbo, rules_wierd, rules_wierd_dbo, one_of_a_kind, low_props)
    csv_names = ['good.csv', 'r60_70.csv', 'rtop_dbot.csv', 'weird.csv', 'rules_wierd_dbo.csv',  'ons.csv', 'lows.csv']

    for rd, csvn in zip(all_rules, csv_names):
        csvf_name = subj + "/" + csvn
        with open(csvf_name, 'w') as csvfile1:
            fieldnames = ['Property', 'Type', 'Ratio', 'support']
            writer = csv.DictWriter(csvfile1, fieldnames=fieldnames)

            writer.writeheader()
            if csvn == 'ons.csv':
                for r, rt in rd.items():
                    prop = r.encode('utf-8')
                    pos = float(rt)
                    data = {'Property': prop, 'Type': "", 'Ratio': pos, 'support': ""}
                    writer.writerow(data)
                continue
            else:
                for k, r in rd.items():
                    prop = (r['p']).encode('utf-8')
                    typet = (r['t']).encode('utf-8')
                    pos = float(r['pos'])
                    tot = float(r['tot'])
                    ratio = pos/tot
                    data = {'Property': prop, 'Type': typet, 'Ratio': ratio, 'support': tot}
                    writer.writerow(data)

        csvfile1.close()


def print_f_rules_to_csv(subj):
    rf_name = "../results/" + subj + "/" + subj + "_f_rules.dump"
    if not os.path.exists(rf_name):
        return
    rules_file = open(rf_name, 'r')
    all_p_rules_tup = pickle.load(rules_file)
    rules_file.close()

    all_p_rules, op_sim_dict = all_p_rules_tup;
    csvf_name = "../results/" + subj + "/" + subj + "_f_rules.csv"

    with open(csvf_name, 'w') as csvfile1:
        fieldnames = ['p1', 'p2', 'Ratio']
        writer = csv.DictWriter(csvfile1, fieldnames=fieldnames)

        writer.writeheader()
        for (p1,p2), r in all_p_rules.items():
            p1_uni = (p1).encode('utf-8')
            p2_uni = (p2).encode('utf-8')

            sim = float(r['sim'])
            tot = float(r['tot'])

            data = {'p1': p1_uni, 'p2': p2_uni, 'Ratio': sim/tot}
            writer.writerow(data)

    csvfile1.close()


def print_cv_rules_to_csv(subj):
    rf_name = "../results/" + subj + "/" + subj + "_cv_dict.dump"
    if not os.path.exists(rf_name):
        return
    rules_file = open(rf_name, 'r')
    all_p_rules_tup = pickle.load(rules_file)
    rules_file.close()

    all_p_rules = all_p_rules_tup;
    csvf_name = "../results/" + subj + "/" + subj + "_cv_rules.csv"

    with open(csvf_name, 'w') as csvfile1:
        fieldnames = ['p1', 'p2', 'Ratio']
        writer = csv.DictWriter(csvfile1, fieldnames=fieldnames)

        writer.writeheader()
        for (p1,p2), r in all_p_rules.items():
            p1_uni = (p1).encode('utf-8')
            p2_uni = (p2).encode('utf-8')
            rat = r

            data = {'p1': p1_uni, 'p2': p2_uni, 'Ratio': rat}
            writer.writerow(data)

    csvfile1.close()




def get_all_rules(subj):
    print_rules_to_csv(subj)
    print_f_rules_to_csv(subj)

def print_features_to_csv(subj):
    rf_name = "../results/" + subj + "/" + subj + "_features.dump"
    if not os.path.exists(rf_name):
        return
    rules_file = open(rf_name, 'r')
    all_p_rules_tup = pickle.load(rules_file)
    rules_file.close()

    all_p_rules = all_p_rules_tup
    csvf_name = "../results/" + subj + "/" + subj + "_features.csv"

    with open(csvf_name, 'w') as csvfile1:
        fieldnames = ['prop', 'p_only_one_counter', 'p_multy_objs_same_type_counter', 'p_objs_unique_type_counter']
        writer = csv.DictWriter(csvfile1, fieldnames=fieldnames)
        writer.writeheader()
        for p, v in all_p_rules.items():
            p_uni = p.encode('utf-8')
            f1_uni = v['p_only_one_counter']
            f2_uni = v['p_multy_objs_same_type_counter']
            f3_uni = v['p_objs_unique_type_counter']

            data = {'prop': p_uni, 'p_only_one_counter': f1_uni, 'p_multy_objs_same_type_counter': f2_uni,
                    'p_objs_unique_type_counter': f3_uni}
            writer.writerow(data)

    csvfile1.close()


if __name__ == '__main__':
    print_features_to_csv('person')


