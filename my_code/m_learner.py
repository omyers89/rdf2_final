import graphviz as graphviz

import Utils
import pickle
import sys
import os
import time
import csv
import numpy as np
from sklearn import svm, tree
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.metrics import confusion_matrix, f1_score
from sklearn.neighbors import KNeighborsClassifier
#from feature_miner import *
from new_dist_feature_miner import *
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import ShuffleSplit, train_test_split
from new_dist_feature_miner import NewDistFeatureMiner
from sklearn.tree import DecisionTreeClassifier
import copy
import os



class learner:
    def __init__(self, featur_list):

        self.feature_list = featur_list #feature list ['time_prep_f', 'obj_sat_f', 'obj_type_f']
        self.training_data_full = self.get_trainig_data_full()
        self.data_set = self.get_data_set()
        self.feature_name_list = self.get_feature_names()
        self.clf_dict = {'svm': svm.SVC(),
                         'logistic': LogisticRegression(),
                         'decision_tree': DecisionTreeClassifier(max_depth=6)}


    def get_trainig_data_full(self):
        full_path = os.path.realpath(__file__)
        path, filename = os.path.split(full_path)
        dump_name = os.path.join(path, "../dumps/training_set.dump")
        r_dict_file = open(dump_name, 'r')
        feature_tag_dict = pickle.load(r_dict_file)
        r_dict_file.close()
        return feature_tag_dict

    def get_feature_names(self):
        tmp_feat_list = []
        for prop ,data in self.training_data_full.items():
            for feat in self.feature_list:
                for f,fv in sorted(data[feat].items()):
                    tmp_feat_list.append(f)
            break
        return tmp_feat_list

    def get_data_set(self):
        x_list = []
        y_list = []

        for prop ,data in self.training_data_full.items():
            y_list.append(data['class'])
            tmp_feat_list = []
            for feat in self.feature_list:
                for f,fv in sorted(data[feat].items()):
                    tmp_feat_list.append(float(fv))
            x_list.append(tmp_feat_list)

        training_data = {"data": x_list, "target": y_list}
        return training_data

    def normalize_columns(self, arr):
        rows, cols = arr.shape
        for col in xrange(cols):
            # warning possible devision by zero
            arr[:, col] /= abs(arr[:, col]).max()

    def test_model(self, normalized = False):
        for clsf_name, clsf in self.clf_dict.items():
            print "*********Checking %s*************" % clsf_name
            data = np.array(self.data_set["data"])
            target = np.array(self.data_set["target"])
            if normalized:
                self.normalize_columns(data)
            cv = ShuffleSplit(n_splits=3, test_size=0.3, random_state=42)
            scores = [0]
            try:
                scores = cross_val_score(clsf, data, target, cv=cv)
            except:
                print "Error in score"
            print "scores: ", scores
            max_avg_score = np.average(scores)
            print "Average Score:", max_avg_score

            # conf matrix:
            x_train, x_test, y_train, y_test = train_test_split(data, target, test_size=0.25, random_state=42)
            clsf.fit(x_train, y_train)
            y_pred = clsf.predict(x_test)
            y_pred_round =[int(round(y)) for y in y_pred]
            cm = confusion_matrix(y_test, y_pred_round)
            f1 = f1_score(y_test, y_pred_round, labels=None, pos_label=1, average="binary", sample_weight=None)
            print "confusion_matrix:"
            print cm
            print "F1 score:"
            print f1

            if clsf_name == 'decision_tree' and False:
                tree.export_graphviz(clsf, out_file='tree.dot', class_names=["Eternal", "Temporal"],
                                     feature_names=self.feature_name_list, impurity=False, filled=True)
                with open("tree.dot") as f:
                    dot_graph = f.read()
                g = graphviz.Source(dot_graph)
                g.render()


            print "*********finished %s*************" % clsf_name


    def init_feat_counters(self):
        res_dict = {0: {}, 1: {}}
        feat_dict = {}
        feat_max_dict = {}
        for prop ,data in self.training_data_full.items():
            for feat in self.feature_list:
                for f,fv in data[feat].items():
                    feat_dict[f] = 0
                    feat_max_dict[f] = 0
            break
        feat_dict['counter'] = 0

        res_dict[0] = copy.deepcopy(feat_dict)
        res_dict[1] = copy.deepcopy(feat_dict)
        return res_dict,feat_max_dict

    def feature_analizer(self):
        feat_counters,feat_max_dict = self.init_feat_counters()
        for prop ,data in self.training_data_full.items():
            cls = data['class']
            for feat in self.feature_list:
                for f,fv in data[feat].items():
                    feat_counters[cls][f] = feat_counters[cls][f] + fv
                    feat_max_dict[f] = max(feat_max_dict[f], fv)
            feat_counters[cls]['counter'] = feat_counters[cls]['counter'] + 1

        for cls ,data in feat_counters.items():
            cntr = feat_counters[cls]['counter']
            for f,fv in data.items():
                if f in feat_max_dict:
                    maxx = feat_max_dict[f]
                    feat_counters[cls][f] = (float(feat_counters[cls][f]) / cntr)/maxx
        print "properties are:", data.keys()
        for cls ,data in feat_counters.items():
            print "cls: ", cls, "data:", data.values()
        self.print_diff_to_csv(feat_counters)

    def print_diff_to_csv(self, dict_to_print):
        csvp_name = "../results/CSVS/feature_analizer.csv"
        with open(csvp_name, 'w') as csvfile2:
            fieldnames = ['class']
            fieldnames_feat = sorted(dict_to_print[0].keys())
            fieldnames.extend(fieldnames_feat)
            writer = csv.DictWriter(csvfile2, fieldnames=fieldnames)
            writer.writeheader()
            data0 = {'class' : 0}
            data0.update(dict_to_print[0])
            writer.writerow(data0)
            data1 = {'class': 1}
            data1.update(dict_to_print[1])
            writer.writerow(data1)
        csvfile2.close()







def test_ml():


    # feature list ['time_prep_f', 'obj_sat_f', 'obj_type_f']
    print "@@@@@@@@@@@@@@@@@@@@@'time_prep_f', 'obj_sat_f', 'obj_type_f'************"
    l_all = learner(['time_prep_f', 'obj_sat_f', 'obj_type_f'])
    print "***************'normalized = False'************"
    l_all.test_model(normalized = False)
    print "***************'normalized = True'************"
    l_all.test_model(normalized = True)

    print "@@@@@@@@@@@@@@@@@@@@@'time_prep_f', 'obj_sat_f'************"

    l_all = learner(['time_prep_f', 'obj_sat_f'])
    print "***************'normalized = False'************"
    l_all.test_model(normalized=False)
    print "***************'normalized = True'************"
    l_all.test_model(normalized=True)
    print "@@@@@@@@@@@@@@@@@@@@@'time_prep_f','obj_type_f'************"

    l_all = learner(['time_prep_f','obj_type_f'])
    print "***************'normalized = False'************"
    l_all.test_model(normalized=False)
    print "***************'normalized = True'************"
    l_all.test_model(normalized=True)
    print "@@@@@@@@@@@@@@@@@@@@@ 'obj_sat_f', 'obj_type_f'************"

    l_all = learner(['obj_sat_f', 'obj_type_f'])
    print "***************'normalized = False'************"
    l_all.test_model(normalized=False)
    print "***************'normalized = True'************"
    l_all.test_model(normalized=True)


if __name__ == "__main__":
    # l_all = learner(['time_prep_f', 'obj_sat_f', 'obj_type_f'])
    # l_all.feature_analizer()
    os.environ["PATH"] += os.pathsep + 'C:/Program Files (x86)/Graphviz-2.38/release/bin/'
    test_ml()