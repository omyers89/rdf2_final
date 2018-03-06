import pickle
import sys
import os
import time
import csv
import numpy as np
from sklearn import svm
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.metrics import confusion_matrix
from sklearn.neighbors import KNeighborsClassifier
#from feature_miner import *
from new_dist_feature_miner import *
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import ShuffleSplit, train_test_split
from new_dist_feature_miner import NewDistFeatureMiner
from sklearn.ensemble import BaggingRegressor
def create_training_data(th):
    full_path = os.path.realpath(__file__)
    path, filename = os.path.split(full_path)
    full_file_name = os.path.join(path, "../dumps/diff_props_dbp_all.dump")

    r_dict_file = open(full_file_name, 'r')
    cop_dict = pickle.load(r_dict_file)
    r_dict_file.close()

    classified_properies_dict = {}
    for k, v in cop_dict.items():
        if v["tot"] == 0:
            continue
        prob = float(v["diff"]) / v["tot"]
        clsf = 1 if prob > th else 0
        seg = v["tot"]
        classified_properies_dict[k] = {'seg': seg, 'class': clsf, 'features': {}}

    full_path = os.path.realpath(__file__)
    path, filename = os.path.split(full_path)
    dump_name = os.path.join(path, "../dumps/prop_features_dbp.dump")
    r_dict_file = open(dump_name, 'r')
    feature_dict = pickle.load(r_dict_file)
    r_dict_file.close()

    max_seg = 0
    for (p,v) in classified_properies_dict.items():
        if v['seg'] > max_seg:
            max_seg = v['seg']

    # add here weighted features.

    for (prop, features) in feature_dict.items():
        if prop in classified_properies_dict:
            classified_properies_dict[prop]['features'] = features
            t_seg = classified_properies_dict[prop]['seg']
            classified_properies_dict[prop]['seg'] = (float(t_seg)/max_seg) * len(classified_properies_dict)
            if classified_properies_dict[prop]['seg'] < 1:
                classified_properies_dict[prop]['seg'] = 1
                print prop + " -is not so seg!"

    #now we have in classified_properies_dict all props classified and with the features

    #the features:
    x_list = []
    #the lables:
    y_list = []

    for (p, v) in classified_properies_dict.items():
        features = v['features']
        weight = v['seg']
        if 'p_only_one_counter' not in features:
            continue

        x1 = float(features['p_only_one_counter'])
        x2 = float(features['p_multy_objs_same_type_counter'])
        x3 = float(features['p_objs_unique_type_counter'])
        x4 = float(features['p_has_primitive_type'])
        for i in range(int(round(weight))):
            x_list.append([x1, x2, x3, x4])
            y_list.append(int(v['class']))

    training_data = {"data":x_list,"target": y_list}
    # dir_name = "../dumps"
    # dump_name = dir_name + "/" + "person_training_data.dump"
    # data_file = open(dump_name, 'w')
    # pickle.dump(training_data, data_file)
    # data_file.close()

    #now we have here x_list with lists of fetures ordered at same order as the lables
    return training_data



def create_training_data_manual():
    classified_properties_dict = get_classified_prop_dict()


    for r in classified_properties_dict.items():
        print r

    full_path = os.path.realpath(__file__)
    path, filename = os.path.split(full_path)
    dump_name = os.path.join(path,"../dumps/prop_features_dbp.dump")
    r_dict_file = open(dump_name, 'r')
    feature_dict = pickle.load(r_dict_file)
    r_dict_file.close()

    max_seg = 0
    for (p,v) in classified_properties_dict.items():
        if v['seg'] > max_seg:
            max_seg = v['seg']

    # add here weighted features.

    for (prop, features) in feature_dict.items():
        if prop in classified_properties_dict:
            classified_properties_dict[prop]['features'] = features
            t_seg = classified_properties_dict[prop]['seg']
            classified_properties_dict[prop]['seg'] = min ((float(t_seg)/max_seg) * len(classified_properties_dict),max_seg)
            if classified_properties_dict[prop]['seg'] < 1:
                classified_properties_dict[prop]['seg'] = 1
                print prop + " -is not so seg!"

    #now we have in classified_properies_dict all props classified and with the features

    #the features:
    x_list = []
    #the lables:
    y_list = []

    for (p, v) in classified_properties_dict.items():
        features = v['features']
        weight = v['seg']
        if 'p_only_one_counter' not in features:
            continue

        x1 = float(features['p_only_one_counter'])
        x2 = float(features['p_multy_objs_same_type_counter'])
        x3 = float(features['p_objs_unique_type_counter'])
        x4 = float(features['p_has_primitive_type'])

        for i in range(int(round(weight))):
            x_list.append([x1, x2, x3, x4])
            y_list.append(int(v['class']))

    training_data = {"data":x_list,"target": y_list}
    # dir_name = "../dumps"
    # dump_name = dir_name + "/" + "person_training_data.dump"
    # data_file = open(dump_name, 'w')
    # pickle.dump(training_data, data_file)
    # data_file.close()

    #now we have here x_list with lists of fetures ordered at same order as the lables
    return training_data


def get_classified_prop_dict(file_suffix="abo"):
    classified_properies_dict = {}

    full_path = os.path.realpath(__file__)
    path, filename = os.path.split(full_path)
    all_path = os.path.join(path, '../results/CSVS/fetures_class_sum_'+file_suffix+ '.csv')
    with open(all_path, mode='r') as infile:
        reader = csv.reader(infile)
        for row in reader:
            if "tag" in row:
                continue
            #prob = float(row[1])
            clsf = int(row[5])
            if row[0] == "-1" or clsf == -1:
                break
            seg = 1
            classified_properies_dict[row[0]] = {'seg': seg, 'class': clsf, 'features': {}}
    return classified_properies_dict


def get_clasifier_for_data(training_data):
    x_list = training_data["data"]
    y_list = training_data["target"]

    clf_logistic = LogisticRegression()
    clf_logistic.fit(x_list,y_list)
    clf_svm = svm.SVC(probability=True)
    clf_svm.fit(x_list, y_list)

    return clf_logistic, clf_svm



def check_svm(training_data):
    print "*********Checking SVM*************"
    clf_svm = svm.SVC(probability=True)
    data = np.array(training_data["data"])
    target = np.array(training_data["target"])
    cv = ShuffleSplit(n_splits=3, test_size=0.3, random_state=0)
    try:
        scores = cross_val_score(clf_svm, data, target, cv=cv)
    except:
       print "Error in score"
    print "scores: ", scores
    max_avg_score = np.average(scores)
    print "Average Score:", max_avg_score

    #conf matrix:
    x_train, x_test, y_train, y_test = train_test_split(data, target, test_size=0.25, random_state=42)
    clf_svm.fit(x_train,y_train)
    y_pred = clf_svm.predict(x_test)
    cm = confusion_matrix(y_test, y_pred)
    print cm
    print "*********Checking SVM*************"
    return max_avg_score


def check_logistic(training_data):
    print "*********Checking Logistic*************"
    clf_logistic = LogisticRegression()
    data = np.array(training_data["data"])
    target = np.array(training_data["target"])
    cv = ShuffleSplit(n_splits=3, test_size=0.3, random_state=0)
    try:
        scores = cross_val_score(clf_logistic, data, target, cv=cv)
    except:
       print "Error in score"
    print "scores: ", scores
    max_avg_score = np.average(scores)
    print "Average Score:", max_avg_score

    # conf matrix:
    x_train, x_test, y_train, y_test = train_test_split(data, target, test_size=0.25, random_state=42)
    clf_logistic.fit(x_train, y_train)
    y_pred = clf_logistic.predict(x_test)
    cm = confusion_matrix(y_test, y_pred)
    print cm
    print "*********Checking Logistic*************"
    return max_avg_score

def check_linear(training_data):
    print "*********Checking Linear*************"
    clf_linear = LinearRegression()
    data = np.array(training_data["data"])
    target = np.array(training_data["target"])
    cv = ShuffleSplit(n_splits=3, test_size=0.3, random_state=0)
    try:
        scores = cross_val_score(clf_linear, data, target, cv=cv)

    except:
       print "Error in score"
    print "scores: ", scores
    max_avg_score = np.average(scores)
    print "Average Score:", max_avg_score

    # conf matrix:
    x_train, x_test, y_train, y_test = train_test_split(data, target, test_size=0.25, random_state=42)
    clf_linear.fit(x_train, y_train)
    y_pred = clf_linear.predict(x_test)
    cm = confusion_matrix(y_test, [int(round(y)) for y in y_pred])
    print cm

    print "*********Checking Linear*************"
    return max_avg_score



#BaggingRegressor
def check_Bagging(training_data):
    print "*********Checking clf_Bagging*************"
    clf_Bagging= BaggingRegressor()
    data = np.array(training_data["data"])
    target = np.array(training_data["target"])
    cv = ShuffleSplit(n_splits=3, test_size=0.3, random_state=0)
    try:
        scores = cross_val_score(clf_Bagging, data, target, cv=cv)
    except:
       print "Error in score"
    print "scores: ", scores
    max_avg_score = np.average(scores)
    print "Average Score:", max_avg_score

    # conf matrix:
    x_train, x_test, y_train, y_test = train_test_split(data, target, test_size=0.25, random_state=42)
    clf_Bagging.fit(x_train, y_train)
    y_pred = clf_Bagging.predict(x_test)
    cm = confusion_matrix(y_test, [int(round(y)) for y in y_pred])
    print cm

    print "*********Checking clf_Bagging*************"
    return max_avg_score

def get_features_for_new_x(prop_uri,quick, nx):
    FMx = NewDistFeatureMiner(DBPEDIA_URL_UP, 'person', "http://dbpedia.org/ontology/Person")
    features = {"p_count": -1,
                                "p_only_one_counter": -1,
                                            "p_multy_objs_same_type_counter": -1,
                                            "p_objs_unique_type_counter": -1,
                                            "p_has_primitive_type": -1}
    try:
        s_dic_for_prop = FMx.get_top_for_prop(prop_uri)
        features = FMx.get_fetures_for_prop(quick, prop_uri,s_dic_for_prop,nx)
    except Exception as e:
        print "exception in get features for new prop:" , e
    return features

def get_classes_prob_for_new_x(features, clsfirx):
    # CHECKED !
    if 'p_only_one_counter' not in features:
        return
    x1 = float(features['p_only_one_counter'])
    x2 = float(features['p_multy_objs_same_type_counter'])
    x3 = float(features['p_objs_unique_type_counter'])
    x4 = float(features['p_has_primitive_type'])
    x_list=[x1, x2, x3, x4]
    print x_list
    # prediction = clsfir.predict_proba[[x_list]]
    # print 'prob for ' + prop_uri + 'is:' + prediction
    # return prediction
    return clsfirx.predict_proba([x_list])



def get_class_with_prob(prop_uri, quick=False, nx=100, man=True):

    temp_dict = get_classified_prop_dict()
    if prop_uri in temp_dict:
        bool_res = int(temp_dict[prop_uri]['class']) == 1
        return bool_res, 1

    if man:
        training_data = create_training_data_manual()
    else:
        training_data = create_training_data(0.02)
    clf_logistic, svm_clsf = get_clasifier_for_data(training_data)
    real_prob=[0,0]
    bool_result = False
    try:
        # res_prod_list_knn = get_classes_prob_for_new_x(prop_uri, clsfir, FM, quick, nx)
        features = get_features_for_new_x(prop_uri, quick, nx)
        res_prod_list_logistic = get_classes_prob_for_new_x(features, clf_logistic)
        # res_prod_list_knn = get_classes_prob_for_new_x(features, knn_clsf)
        # print "res_prod_list is knn:"
        # print res_prod_list_knn
        print "res_prod_list is svm:"
        print res_prod_list_logistic
        real_prob = res_prod_list_logistic[0]
        bool_result = real_prob[1] > 0.5  # if 1 (prob for temporal greater than 0.5 ) res is true for display
    except Exception as e:
        LOG(" Failed to find probs for p:" + e.message )

    return bool_result, (real_prob[1] if bool_result else real_prob[0])



def print_features_to_csv():
    temp_dict = get_classified_prop_dict()
    diff_dump_name = "../dumps/prop_features_dbp.dump"
    diff_dict_file = open(diff_dump_name, 'r')
    p_filtered_dict = pickle.load(diff_dict_file)
    diff_dict_file.close()
    csvp_name = "../results/CSVS/fetures_class.csv"
    with open(csvp_name, 'w') as csvfile2:
        fieldnames = ['uri', 'p_only_one_counter', 'p_multy_objs_same_type_counter','p_objs_unique_type_counter', 'p_has_primitive_type', 'tag']
        writer = csv.DictWriter(csvfile2, fieldnames=fieldnames)

        writer.writeheader()
        for p, c in p_filtered_dict.items():
            p_uri = (p).encode('utf-8')
            p_only_one_counter = c['p_only_one_counter']
            p_multy_objs_same_type_counter = c['p_multy_objs_same_type_counter']
            p_objs_unique_type_counter = c['p_objs_unique_type_counter']
            p_has_primitive_type= c['p_has_primitive_type']
            if p in temp_dict:
                tag = temp_dict[p]['class']
            else:
                tag = -1
            data = {'uri': p_uri,
                    'p_only_one_counter':p_only_one_counter,
                    'p_multy_objs_same_type_counter':p_multy_objs_same_type_counter,
                    'p_objs_unique_type_counter':p_objs_unique_type_counter,
                    'p_has_primitive_type':p_has_primitive_type,
                    'tag':tag}
            writer.writerow(data)

    csvfile2.close()




if __name__ == "__main__":
    # clsfir = create_training_data('person')
    # FM = NewFeatureMiner(DBPEDIA_URL_UP, 'person', "http://dbpedia.org/ontology/Person")
    # x1_list = get_classes_prob_for_new_x('http://dbpedia.org/ontology/militaryRank',clsfir, FM, False)
    # x11_list = get_classes_prob_for_new_x('http://dbpedia.org/ontology/nominee', clsfir, FM, False)
    # x0_list = get_classes_prob_for_new_x('http://dbpedia.org/ontology/deputy', clsfir, FM, False)
    # x11_list = get_classes_prob_for_new_x('http://dbpedia.org/ontology/governor', clsfir, FM, False)
    # x111_list = get_classes_prob_for_new_x('http://dbpedia.org/ontology/lieutenant', clsfir, FM, False)
    # x1111_list = get_classes_prob_for_new_x('http://dbpedia.org/ontology/relation', clsfir, FM, False)
    # x11111_list = get_classes_prob_for_new_x('http://dbpedia.org/ontology/vicePresident', clsfir, FM, False)
    # #http://dbpedia.org/ontology/vicePresident

    #check_svm()
    #
    #  (b,p ) = get_class_with_prob("http://dbpedia.org/ontology/birthPlace", False, 100)
    #x00_sanity_list = get_class_with_prob('http://dbpedia.org/ontology/birthPlace', False, 75)
    training_data = create_training_data_manual()
    # check_svm(training_data)
    check_logistic(training_data)
    #check_linear(training_data)
    # check_Bagging(training_data)
    # get_class_with_prob("http://dbpedia.org/ontology/birthPlace")
    # get_class_with_prob("http://dbpedia.org/ontology/spouse")
