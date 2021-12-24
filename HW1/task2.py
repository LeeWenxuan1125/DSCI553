#_author: Wenxuan Li
#_USC_ID: 8329-9604-25

from pyspark import SparkContext
import json
import sys
from operator import add
import time

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

def DJBHash(string):
    hash = 5381
    for ch in string:
        hash = hash << 5 + ord(ch)

    return (hash & 0x7FFFFFFF)

def top10_business_with_largest_nums(business_rdd):
    """

    :param business_rdd: transformered review data
    :return: The top 10 businesses that had the largest numbers of reviews and the number of reviews they had
    """
    return business_rdd.map(lambda x:(x[0],1)).reduceByKey(add).sortBy(lambda x: (-x[1],x[0])).take(10)

if __name__ == '__main__':

    review_filepath = sys.argv[1]
    output_filepath = sys.argv[2]
    n_partition = sys.argv[3]

    sc = SparkContext.getOrCreate()
    sc.setLogLevel("ERROR")

    # result
    exe_result = {}
    exe_result['default'] = {}
    exe_result['customized'] = {}

    review_rdd = sc.textFile(review_filepath).map(lambda x: json.loads(x)).map(lambda x:(x['business_id'], x['business_id']))

    # using default partition function
    business_rdd = review_rdd
    exe_result['default']['n_partition'] = business_rdd.getNumPartitions()
    exe_result['default']['n_items'] = business_rdd.glom().map(len).collect()
    start = time.time()
    top10_business_with_largest_nums(business_rdd)
    end = time.time()
    exe_result['default']['exe_time'] = end - start

    # using customized partition function
    business_rdd_cus = review_rdd.partitionBy(int(n_partition), BKDRHash)
    exe_result['customized']['n_partition'] = business_rdd_cus.getNumPartitions()
    exe_result['customized']['n_items'] = business_rdd_cus.glom().map(len).collect()

    start = time.time()
    top10_business_with_largest_nums(business_rdd_cus)
    end = time.time()
    exe_result['customized']['exe_time'] = end - start

    print(exe_result)

    out_file = open(output_filepath, 'w')
    json.dump( exe_result, out_file, indent=4)





