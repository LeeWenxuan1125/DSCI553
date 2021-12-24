from pyspark import SparkContext
from graphframes import *
import os
from itertools import combinations
from pyspark.sql import SparkSession
import pyspark.sql.functions as fc
import pandas as pd
import sys


os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.8.2-spark3.1-s_2.12 pyspark-shell")


def fun_merge(a, b):
    return set(a).union(set(b))


if __name__ == '__main__':


    filter_threshold = 7
    input_file = "../data/ub_sample_data.csv"
    output_file = "./res/task2.txt"

    # filter_threshold = int(sys.argv[1])
    # input_file = sys.argv[2]
    # output_file = sys.argv[3]

    sc = SparkContext('local[*]', 'task1')
    sc.setLogLevel('ERROR')


    input_RDD = sc.textFile(input_file, 10)
    header = input_RDD.first()

    raw_rdd = input_RDD.filter(lambda x: x != header)


    # key: user   value: list of businesses that this user have rated
    user_bus_dic = raw_rdd.map(lambda x: (x.split(',')[0], x.split(',')[1]))\
        .groupByKey().mapValues(list)\
        .filter(lambda x: len(x[1]) >= filter_threshold)\
        .collectAsMap()

    # potential user list
    user_list = user_bus_dic.keys()

    # potential combination
    candidate_pairs = list(combinations(user_list, 2))

    # edge rdd
    edge_rdd = sc.parallelize(candidate_pairs,10)\
        .map(lambda x: (x, len(set(user_bus_dic[x[0]]).intersection(set(user_bus_dic[x[1]])))))\
        .filter(lambda x: x[1] >= filter_threshold).sortBy(lambda x: x[0])


    user_list_inEdge = list(edge_rdd.map(lambda x:(1, x[0])).reduceByKey(lambda a,b: fun_merge(a, b)).collect()[0][1])


    ## define node, edge
    spark = SparkSession.builder.getOrCreate()

    vertices = spark.createDataFrame([(user,) for user in sorted(user_list_inEdge)], ["id"])
    edges = spark.createDataFrame([tup[0] for tup in edge_rdd.collect()], ["src", "dst"])
    vertices.toPandas().to_csv("vertices.csv")
    edges.toPandas().to_csv("edge.csv")

    g = GraphFrame(vertices, edges)
    res = g.labelPropagation(maxIter=5)

    res = res.groupBy('label').agg(fc.sort_array(fc.collect_list("id")).alias("community"), fc.count("*").alias("size")).orderBy(['size', 'community'])


    output_res = res.toPandas()['community']

    f = open(output_file, "w")
    for community in output_res :
        group = ""
        for id in community:
            group += "'" + str(id) + "', "
        group = group.rstrip().rstrip(',')
        f.write(group)
        f.write("\n")
    f.close()


