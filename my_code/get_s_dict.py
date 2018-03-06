import pickle
import os
import csv
from Utils import dictionaries

def get_subj_from_dump(subj_name):
    rf_name = "../results/" + subj_name + "/FINAL/" + subj_name + "_top.dump"
    rp_name = "../results/" + subj_name + "/FINAL/" + subj_name + "_prop.dump"
    if not os.path.exists(rf_name):
        return
    sujects_f = open(rf_name, 'r')
    all_subj = pickle.load(sujects_f)

    sujects_f.close()

    print "the number of {} is:{}".format(subj_name, len(all_subj))
    csvf_name = "../results/" + subj_name + "/" + subj_name + "_subject.csv"
    with open(csvf_name, 'w') as csvfile1:
        fieldnames = ['uri']
        writer = csv.DictWriter(csvfile1, fieldnames=fieldnames)

        writer.writeheader()
        for r in all_subj:
            subj_uri = (r).encode('utf-8')

            data = {'uri' : subj_uri}
            writer.writerow(data)

    csvfile1.close()

    if not os.path.exists(rp_name):
        return
    sujects_p = open(rp_name, 'r')
    all_prop = pickle.load(sujects_p)

    sujects_p.close()

    print "the number of {} is:{}".format(subj_name, len(all_prop))
    csvp_name = "../results/" + subj_name + "/" + subj_name + "_props.csv"
    with open(csvp_name, 'w') as csvfile2:
        fieldnames = ['uri', 'cnter']
        writer = csv.DictWriter(csvfile2, fieldnames=fieldnames)

        writer.writeheader()
        for r, c in all_prop.items():
            subj_uri = (r).encode('utf-8')
            
            data = {'uri' : subj_uri, 'cnter' : c}
            writer.writerow(data)

    csvfile2.close()



if __name__ == '__main__':
    for d in dictionaries:
        for s, suri in d.items():
            get_subj_from_dump(s)

