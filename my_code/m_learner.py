import Utils
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
from sklearn.tree import DecisionTreeClassifier



class learner:
    def __init__(self, featur_list):

        self.feature_list = featur_list #feature list ['time_prep_f', 'obj_sat_f', 'obj_type_f']
        self.training_data_full = self.get_trainig_data_full()
        self.data_set = self.get_data_set()
        self.clf_dict = {'svm': svm.SVC(),
                         'logistic': LogisticRegression(),
                         'decision_tree': DecisionTreeClassifier()}


    def get_trainig_data_full(self):
        full_path = os.path.realpath(__file__)
        path, filename = os.path.split(full_path)
        dump_name = os.path.join(path, "../dumps/training_set.dump")
        r_dict_file = open(dump_name, 'r')
        feature_tag_dict = pickle.load(r_dict_file)
        r_dict_file.close()
        return feature_tag_dict

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

    def test_model(self):
        for clsf_name, clsf in self.clf_dict.items():
            print "*********Checking %s*************" % clsf_name
            data = np.array(self.data_set["data"])
            target = np.array(self.data_set["target"])
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
            cm = confusion_matrix(y_test, [int(round(y)) for y in y_pred])
            print cm

            print "*********finished %s*************" % clsf_name


if __name__ == "__main__":
    # feature list ['time_prep_f', 'obj_sat_f', 'obj_type_f']
    l = learner(['obj_sat_f', 'obj_type_f'])
    l.test_model()
