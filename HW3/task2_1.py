from pyspark import SparkContext, SparkConf
from typing import List, Dict
import random
import sys
from itertools import combinations
import time
from math import sqrt
import math


def BKDRHash(string):
    """
    BKDR Hash function
    :param string: business id: key(type: string)
    :return: hash code
    """
    seed = 131
    hash = 0
    for ch in string:
        hash = hash * seed + ord(ch)
    return hash & 0x7FFFFFFF


def merge(a, b):
    """
    merge two dictionary
    :param a: dictionary a
    :param b: dictionary b
    :return:
    """
    a.update(b)
    return a

def calculate_similarity(b1, b2, business_dict):

    l1 = business_dict[b1]
    l2 = business_dict[b2]
    co_rater = list(set(l1.keys()).intersection(set(l2.keys())))

    #
    if len(l1) == 0 or len(l2) == 0:
        return 0

    if len(co_rater) < 3:
        return 0

    numerator = 0
    denominato_r1 = 0
    denominato_r2 = 0

    ave_1 = sum(l1.values())/len(l1.values())
    ave_2 = sum(l2.values())/len(l2.values())

    for rater in co_rater:
        numerator = numerator + (l1[rater] - ave_1)*(l2[rater] - ave_2)
        denominato_r1 = denominato_r1 + (l1[rater] - ave_1)**2
        denominato_r2 = denominato_r2 + (l2[rater] - ave_2)**2
    if denominato_r1 == 0 or denominato_r2 == 0:
        return 0
    else:
        smi = numerator/(sqrt(denominato_r1)*sqrt(denominato_r2))
      #  return max(1, smi + 0.15)
       # return max(1, smi + 0.15)
        return smi

def pred_rate(user_id, business_id, business_dict, user_dict, N):
    """
    predict the rate of user_id to business_id
    :param user_id:
    :param business_id:
    :param business_dict:
    :param user_dict:
    :param N: N neigbors
    :return:
    """

    if user_id not in user_dict:
        return sum(business_dict[business_id].values()) / len(business_dict[business_id])

    if business_id not in business_dict:
        return sum(user_dict[user_id].values())/len(user_dict[user_id])

    similarity_res = []

    for business in user_dict[user_id].keys():
        if (business_id != business):
            smi = calculate_similarity(business, business_id, business_dict)
            smi = float(smi*(math.pow(abs(smi), 1.5)))
            similarity_res.append((smi, business))

    similarity_res = sorted(similarity_res, key=lambda x: -x[0])
    similarity_res = list(filter(lambda x: x[0] > 0, similarity_res))

  #  print(len(similarity_res))

    numerate_si = 0
    denominate_si = 0
    # select the most N similar item
  #  print(similarity_res)

    for i in range(min(len(similarity_res), N)):
       # if similarity_res[i][0] > 0.6:
        numerate_si = numerate_si + similarity_res[i][0] * user_dict[user_id][similarity_res[i][1]]
        denominate_si = denominate_si + abs(similarity_res[i][0])

    if denominate_si == 0:
        return sum(user_dict[user_id].values())/len(user_dict[user_id])
    else:
        rate = numerate_si/denominate_si

        return 0.2*rate + 0.4*sum(user_dict[user_id].values())/len(user_dict[user_id]) + 0.4*sum(business_dict[business_id].values())/len(business_dict[business_id])
        # return rate



if __name__ == '__main__':

    train_file = './data/yelp_train.csv'
    test_file = './data/yelp_val.csv'
    output_file = 'task2_1.csv'
    # train_file = sys.argv[1]
    # test_file = sys.argv[2]
    # output_file = sys.argv[3]

    start = time.time()

    configuration = SparkConf().setAppName("task2").setMaster("local[*]")
    sc = SparkContext.getOrCreate(configuration)
    sc.setLogLevel("ERROR")



    train_rdd = sc.textFile(train_file)
    test_rdd = sc.textFile(test_file)

    # header
    header = train_rdd.first()

    pre_train_rdd = train_rdd.filter(lambda x: x != header)
    pre_train_rdd.cache()

    business_dict = pre_train_rdd\
        .map(lambda x: (x.split(',')[1], {x.split(',')[0]: float(x.split(',')[2])}))\
        .reduceByKey(lambda x,y: merge(x, y)).collectAsMap()

    #print(business_dict)

    user_dict = pre_train_rdd\
        .map(lambda x: (x.split(',')[0], {x.split(',')[1]: float(x.split(',')[2])}))\
        .reduceByKey(lambda x,y: merge(x, y)).collectAsMap()


    res = test_rdd.filter(lambda x: x != header).map(lambda x: (x.split(',')[0], x.split(',')[1])) \
        .partitionBy(10, BKDRHash).map(lambda x: (x[0], x[1], pred_rate(x[0], x[1], business_dict, user_dict, 12)))

    ground_truth = test_rdd.filter(lambda x: x != header) \
        .map(lambda x: (x.split(',')[0], x.split(',')[1], float(x.split(',')[2])))

    grou_rdd = ground_truth.map(lambda x: ((x[0], x[1]), x[2]))
    #
    #
    # # for i in range(10,20,2):
    # #     res = test_rdd.filter(lambda x: x!= header).map(lambda x: (x.split(',')[0], x.split(',')[1]))\
    # #         .partitionBy(10, BKDRHash).map(lambda x: (x[0], x[1], pred_rate(x[0], x[1], business_dict, user_dict, i)))
    #
    res_rdd = res.map(lambda x: ((x[0], x[1]), x[2]))
    RMSE = res_rdd.join(grou_rdd).map(lambda x: (1, (x[1][0] - x[1][1])**2)).reduceByKey(lambda x,y: x+y).collect()
    print("{}:{}".format(12, sqrt(RMSE[0][1]/len(res.collect()))))

    with open(output_file, 'w') as f:
        f.write("user_id,business_id,stars")
        for pair in res.collect():
            f.write('\n')
            f.write("{},{},{}".format(pair[0], pair[1], pair[2]))

        f.close()

    end = time.time()
    print("Duration:", end - start)
   # print(res_rdd.join(grou_rdd).take(10))
   #  with open(output_file, 'w') as f:
   #      f.write("user_id,business_id,stars_pred,stars_true")
   #      for pair in res_rdd.join(grou_rdd).collect():
   #          f.write('\n')
   #          f.write("{},{},{},{}".format(pair[0][0], pair[0][1], pair[1][0], pair[1][1]))
   #
   #      f.close()


