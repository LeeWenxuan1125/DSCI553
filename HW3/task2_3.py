from pyspark import SparkContext, SparkConf
import sys
import time
from math import sqrt
import math
import json
from operator import add
import pandas as pd
import xgboost as xgb
from sklearn import preprocessing
from sklearn.metrics import mean_squared_error
import numpy as np


def transfer(user_id, business_id, rate, business_feature_dic, user_feature_dic, user_var_dic, busi_var_dic):

    busi_feature = [None, None, None]
    user_feature = [None, None, None, None, None ]
    busi_var = [None]
    user_var = [None]
    try:
        busi_feature = business_feature_dic[business_id]
        busi_var = busi_var_dic[business_id]
    except KeyError:
        print("business_id:", business_id)

    try:
        user_feature = user_feature_dic[user_id]
        user_var = user_var_dic[user_id]
    except KeyError:
        print("user_id:", user_id)


    res = [user_id, business_id] + busi_feature + user_feature

    res.append(busi_var)
    res.append(user_var)
    res.append(rate)

    return res

def fillNan(business_feature):
    if None == business_feature[2]:
        business_feature[2] = 0

    return business_feature




if __name__ == '__main__':

    start = time.time()

    sc = SparkContext('local[*]', 'task2')
    sc.setLogLevel("ERROR")

    # folder_path = './data/'
    # test_file = './data/yelp_val.csv'
    # output_file = 'task2_2.csv'

    folder_path = sys.argv[1]
    test_file = sys.argv[2]
    output_file = sys.argv[3]


    train_file = folder_path + "yelp_train.csv"
    business_file = folder_path + "business.json"
    user_file = folder_path + "user.json"
    photo_file = folder_path + "photo.json"
    review_file = folder_path + "review_train.json"

    t = time.time()

    # RDD
    train_rdd = sc.textFile(train_file)
    test_rdd = sc.textFile(test_file)
    # header
    header = train_rdd.first()


    # read training dataset
    pre_train_rdd = train_rdd\
        .filter(lambda x: x != header)\
        .map(lambda x: (x.split(',')[0], x.split(',')[1], float(x.split(',')[2])))

    # read validation dataset
    pre_test_rdd = test_rdd\
        .filter(lambda x: x != header)\
        .map(lambda x: (x.split(',')[0], x.split(',')[1]))


    # business feature: stars, review_count, photo number
    business_rdd = sc.textFile(business_file)\
        .map(lambda x: json.loads(x)).map(lambda x: (x['business_id'], (x['review_count'], x['stars'])))

    # user rate variance and business rate variance
    review_rdd = sc.textFile(review_file).map(lambda x: json.loads(x)).map(lambda x: (x['user_id'], x['business_id'], x['stars']))
    review_rdd.cache()
    user_review = review_rdd.map(lambda x: (x[0], x[2]))
    busi_review = review_rdd.map(lambda x: (x[1], x[2]))

    user_var_dic = user_review.groupByKey().mapValues(list).mapValues(lambda x: np.var(x)).collectAsMap()
    busi_var_dic = busi_review.groupByKey().mapValues(list).mapValues(lambda x: np.var(x)).collectAsMap()

    # user_feature: review_count, average_stars, useful, funny, fans
    user_rdd = sc.textFile(user_file)\
        .map(lambda x: json.loads(x)).map(lambda x: (x['user_id'], [x['review_count'], x['average_stars'], x['useful'], x['funny'], x['fans']]))


    # photo_feature: the number of pictures that the business have
    photo_rdd = sc.textFile(photo_file).map(lambda x: json.loads(x)).map(lambda x: (x['business_id'], 1)).reduceByKey(add)


    # key = business_id, value = business feature
    business_feature_dic = business_rdd.leftOuterJoin(photo_rdd)\
        .mapValues(lambda x: [x[0][0], x[0][1], x[1]]).mapValues(lambda x: fillNan(x)).collectAsMap()

    # key = user_id, value = user feature
    user_feature_dic = user_rdd.collectAsMap()

    training_data_rdd = pre_train_rdd.map(lambda x: transfer(x[0], x[1], x[2],
                                                             business_feature_dic,  user_feature_dic, user_var_dic, busi_var_dic)).filter(lambda x: None not in x)
    training_data = pd.DataFrame(training_data_rdd.collect(),
                                 columns = ['user_id', 'business_id', 'review_count', 'business_ave_stars', 'photo_num', 'review_count', 'user_ave_stars', 'useful', 'funny', 'fans', 'busi_var', 'user_var', 'rate'])


    test_data_rdd = pre_test_rdd.map(lambda x: transfer(x[0], x[1], 0,
                                                        business_feature_dic,  user_feature_dic, user_var_dic, busi_var_dic))
    test_data = pd.DataFrame(test_data_rdd.collect(),
                             columns = ['user_id', 'business_id', 'review_count', 'business_ave_stars', 'photo_num', 'review_count', 'user_ave_stars', 'useful', 'funny', 'fans', 'busi_var', 'user_var', 'prediction'])
    test_data.fillna(0)
   # print(test_data)

    # create an xgboost regression model
    model = xgb.XGBRegressor()

    ## train xgboost regressor
    xg_reg = xgb.XGBRegressor(colsample_bytree = 0.3, learning_rate = 0.1,\
                max_depth = 9, alpha = 0, n_estimators = 70, subsample = 0.5, random_state = 0)

    scaler = preprocessing.StandardScaler()

    train_data_no_label = training_data[['review_count', 'business_ave_stars', 'photo_num', 'review_count', 'user_ave_stars', 'useful', 'funny', 'fans', 'busi_var', 'user_var']]

   # print(train_data_no_label)

    # trained model
    scaler.fit(train_data_no_label)

    # transfer data
    transfer_train_data = scaler.transform(train_data_no_label)

    # fit the model
    model.fit(transfer_train_data, training_data['rate'])
  #  print("training_RMSE:", mean_squared_error(y_true = training_data['rate'].values, y_pred = model.predict(transfer_train_data), squared=False))

    # test
    test_data_no_label = test_data[['review_count', 'business_ave_stars', 'photo_num', 'review_count', 'user_ave_stars', 'useful', 'funny', 'fans','busi_var', 'user_var']]
    transfer_test_data = scaler.transform(test_data_no_label)

 #   pre_res = model.predict(transfer_test_data)


    test_data['prediction'] = model.predict(transfer_test_data)

    print("test_RMSE:", mean_squared_error(y_true = test_data['rate'].values, y_pred = test_data['prediction'], squared=False))

    res = test_data[['user_id', 'business_id', 'prediction']]

    res.to_csv(output_file, index = False)

    end = time.time()

    print("Duration:", end - start)







