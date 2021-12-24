from pyspark import SparkContext, SparkConf
from typing import List, Dict
import random
import sys
from itertools import combinations
import time

# Recall: 0.97   # Accuracy: 0.99

NUM_MINHASH_BUCKETS = 30 # minhash
MAX = 10000000
B = 15
R = 2
NUM_HASH_LSH = 100000

def f(x):
    return x

def map_userid_2_userindex(user_list, user_dic):
    """
    ['vxR_YV0atFxIxfOnF9uHjQ',...]  -> [104,.....]
    :param user_list:
    :param user_dic:
    :return:
    """

    res = []

    for user in user_list:
        res.append(user_dic[user])

    return sorted(res)


def min_ele_2_lists(list1, list2):
    """

    :param list1: list 1
    :param list2: list 2
    :return:
    """
    res = []

    for i,j in zip(list1, list2):
        temp = min(i, j)
        res.append(temp)

    return res

# min_hash
def hash_fun(param_a, param_b, x, param_m):
    """
    f(x) = ((ax + b)) % m
    :param param_a:  a
    :param param_b:  b 
    :param x:  input_x
    :param param_m:  number of baskets 
    :return: 
    """

    return (param_a * x + param_b) % param_m


def create_signature(user_index_list: List[int], bin_length: int, a_list: List[int], b_list: List[int]) -> List[int]:
    """

    :param user_index_list: index of users of each business
    :return: signature
    """

    # initialize the list with maximum number

    res = [sys.maxsize for _ in range(NUM_MINHASH_BUCKETS)]
    for user_index in user_index_list:
        res1 = []
        for a, b in zip(a_list, b_list):
            res1.append(hash_fun(a, b, user_index, bin_length))

        res = min_ele_2_lists(res, res1)


    return res

def create_bands(signature_list, b, r):
    """
    ('a': [2,3,4,5,6...]) --> ('a':[2, 3, 4], 'a':[5, 6,7]......)
    :param signature_list: signature of each business
    :param b: the number of bands
    :param r: the size of each band
    :return:
    """
    count = 0
    band = []
    bands_res = []
    for sig in signature_list:
        count = count + 1
        band.append(sig)
        if count == R:
            bands_res.append(band)
            count = 0
            band = []

    return bands_res

def lsh_hash_fun(band):

    return sum(band)%NUM_HASH_LSH

def Jaccard_similarity(l1, l2):

    l1 = set(l1)
    l2 = set(l2)

    return len(l1.intersection(l2))/len(l1.union(l2))

def calculate_similarity(business_1, business_2, user_busi_adja_rdd_dic, reverse_business_dic):

    l1 = user_busi_adja_rdd_dic[business_1]
    l2 = user_busi_adja_rdd_dic[business_2]

    sim = Jaccard_similarity(l1, l2)

    business_id_1 = min(reverse_business_dic[business_1], reverse_business_dic[business_2])
    business_id_2 = max(reverse_business_dic[business_1], reverse_business_dic[business_2])

    return {"business_id_1": business_id_1, "business_id_2": business_id_2, "similarity": sim}



if __name__ == '__main__':

    start = time.time()

    input_file = "../data/yelp_train.csv"
    output_file = "output_1.csv"

    # input_file = sys.argv[1]
    # output_file = sys.argv[2]


    configuration = SparkConf().setAppName("task1").setMaster("local[*]")
    # configuration.set("spark.driver.memory", "4g")
    # configuration.set("spark.executor.memory", "4g")
    sc = SparkContext.getOrCreate(configuration)
    sc.setLogLevel("ERROR")




    input_rdd = sc.textFile(input_file, 5)
    header = input_rdd.first()


    # review_rdd: (business_id, user_id)
    review_rdd = input_rdd.filter(lambda x:x != header)\
        .map(lambda x: (x.split(',')[1], x.split(',')[0]))

  #  print(review_rdd.take(10))

    # user_list: store all the user id, sort by alphabetic order
    user_list = review_rdd.map(lambda x: x[1]).sortBy(lambda x: x).distinct().collect()

    # business_list: store all the user id, sort by alphabetic order
    business_list = review_rdd.map(lambda x:x[0]).sortBy(lambda x:x).distinct().collect()


    # print(len(user_list))
    # print(len(business_list))

    # hash table for user index
    user_dic = {}
    for index, user in enumerate(user_list):
        # O(1)
        user_dic[user] = index

    # hash table for business index
    business_dic = {}
    reverse_business_dic = {}
    for index, business in enumerate(business_list):
        business_dic[business] = index
        reverse_business_dic[index] = business


    # grounp review by business id
    #
   # print( review_rdd.groupByKey().mapValues(list).mapValues(len).filter(lambda x: x[1] > 50).count())

    # (key = index of business, value = rated user list)
    user_busi_adja_rdd = review_rdd.groupByKey()\
        .mapValues(set)\
        .mapValues(list)\
        .map(lambda x: (business_dic[x[0]], map_userid_2_userindex(x[1], user_dic))).sortByKey().partitionBy(5)


    user_busi_adja_rdd.cache()
    user_busi_adja_rdd_dic = user_busi_adja_rdd.collectAsMap()

    # end = time.time()
    # print("Duration:", end - start)

    # parameters of hash table
    a_list = random.sample(range(len(user_dic)), NUM_MINHASH_BUCKETS)
    b_list = random.sample(range(len(user_dic)), NUM_MINHASH_BUCKETS)

    # min-hash
    minhash_rdd = user_busi_adja_rdd.mapValues(lambda x: create_signature(x, 2*len(user_dic), a_list, b_list))

    # LSH
    LSH_rdd = minhash_rdd\
        .mapValues(lambda x: create_bands(x, B, R))\
        .flatMapValues(f).mapValues(lsh_hash_fun)\
        .map(lambda x: (x[1], x[0]))\
        .groupByKey().mapValues(set)\
        .mapValues(lambda x: list(combinations(x, 2)))\
        .flatMapValues(f).map(lambda x: (min(x[1][0], x[1][1]), max(x[1][0], x[1][1]))).distinct()\
        .map(lambda x: calculate_similarity(x[0], x[1], user_busi_adja_rdd_dic, reverse_business_dic))\
        .filter(lambda x: x['similarity'] >= 0.5).sortBy(lambda x:(x['business_id_1'], x['business_id_2']))

    #
    # print(user_busi_adja_rdd.take(10))
    # print(minhash_rdd.take(10))
    #
    # print(LSH_rdd.take(10))
   # print(LSH_rdd.take(10))
   #  end = time.time()
   #  print("Duration:", end - start)
   # print("Duration:", end - start)

    with open(output_file, 'w') as f:
        f.write("business_id_1, business_id_2, similarity")
        for pair in LSH_rdd.collect():
            f.write('\n')
            f.write("{},{},{}".format(pair['business_id_1'], pair['business_id_2'], pair['similarity']))

        f.close()

    end = time.time()
    print("Duration:", end - start)
    #
