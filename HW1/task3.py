#_author: Wenxuan Li
#_USC_ID: 8329-9604-25

from pyspark import SparkContext
import json
import sys
from operator import add
import time

def calculate_ave_star(review_star_rdd):
    """
    :param review_star_rdd: (business_id, star)
    :return: (business_id, (sum, num))
    """
    return review_star_rdd\
        .aggregateByKey((0, 0), lambda U,v: (U[0] + v, U[1] +1), lambda U1, U2:(U1[0] + U2[0], U1[1] + U2[1]))

def get_city_ave_star(review_star_rdd, business_city_rdd):
    """

    :param review_star_rdd: (business_id, star)
    :param business_star_rdd: (business_id, (sum, num))
    :return:
    """

    business_star_rdd = calculate_ave_star(review_star_rdd)

    return business_star_rdd.\
            leftOuterJoin(business_city_rdd)\
            .map(lambda x: (x[1][1], x[1][0]))\
            .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))\
            .mapValues(lambda value: float(value[0] / value[1]))\
            .sortBy(lambda x: (-x[1], x[0])).collect()


def get_execution_time(review_star_rdd, business_city_rdd, load_time):

    """

    :param review_star_rdd: (business_id, star)
    :param business_city_rdd: (business_id, (sum, num))
    :param load_time: the time of loading file
    :return: method1: python  method2:pyspark
    """
    # method 1:
    start = time.time()
    business_star_rdd = calculate_ave_star(review_star_rdd)
    temp = business_star_rdd. \
        leftOuterJoin(business_city_rdd) \
        .map(lambda x: (x[1][1], x[1][0])) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .mapValues(lambda value: float(value[0] / value[1])).collect()
    result_1 = sorted(temp, key = lambda x: (-x[1], x[0]))[:10]
    print("top10 by method1:", result_1)
    end = time.time()
    method_1_time = load_time + end - start

    # method 2:
    start = time.time()
    business_star_rdd = calculate_ave_star(review_star_rdd)
    result_2 = business_star_rdd. \
        leftOuterJoin(business_city_rdd) \
        .map(lambda x: (x[1][1], x[1][0])) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .mapValues(lambda value: float(value[0] / value[1])) \
        .sortBy(lambda x: (-x[1], x[0])).take(10)
    print("top10 by method2:", result_2)
    end = time.time()
    method_2_time = load_time + end - start

    return method_1_time, method_2_time

if __name__ == '__main__':

    review_file = sys.argv[1]
    business_file = sys.argv[2]
    output_filepath_a = sys.argv[3]
    output_filepath_b = sys.argv[4]

    sc = SparkContext.getOrCreate()
    sc.setLogLevel("ERROR")

    start = time.time()
    # It is very important to add partitionBy!!
    review_star_rdd = sc.textFile(review_file).map(lambda x: json.loads(x)).map(lambda x: (x['business_id'], x['stars'])).partitionBy(4)
    business_city_rdd = sc.textFile(business_file).map(lambda x: json.loads(x)).map(lambda x: (x['business_id'], x['city'])).partitionBy(4)
    end = time.time()
    # load file time
    load_time = end - start
    #
    method_1_time, method_2_time = get_execution_time(review_star_rdd, business_city_rdd, load_time)
    print("python time:{}, pyspark time:{}".format(method_1_time, method_2_time))

    exe_result = {}
    exe_result['m1'] = method_1_time
    exe_result['m2'] = method_2_time
    exe_result['reason'] = 'The execution time of method_2 is greatly less than the execution time of method_1, especially in huge dataset. ' \
                           'Because in spark, data is split into multiple partitions and processed in parallel which is faster'

    city_result = get_city_ave_star(review_star_rdd, business_city_rdd)
   # print(city_result)
    with open(output_filepath_a, 'w') as f:
        f.write('city,stars')
        for (city, star) in city_result:
            f.write('\n')
            f.write("{0},{1}".format(city, star))

    out_file = open(output_filepath_b, 'w')
    json.dump(exe_result, out_file, indent=4)










