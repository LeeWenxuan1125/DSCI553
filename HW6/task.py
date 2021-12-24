from sklearn.cluster import KMeans
import sys
import time
import warnings
from pyspark import SparkContext
import random
import numpy as np
from collections import defaultdict
from itertools import combinations

warnings.filterwarnings(action='ignore')


def elementwiseAdd(list_a, list_b):
    sum_list = []

    for (item1, item2) in zip(list_a, list_b):
        sum_list.append(item1 + item2)

    return sum_list


def Maha_Distance(point, cluster):
    """
    point: new point (array)
    cluster: discard set or compressed set
    """

    centroid = cluster["sum"] / cluster["num"]

    sigma = cluster["sumsq"]/cluster["num"] - (cluster["sum"]/cluster["num"])**2

  #  print("sig1:", (sigma ** (1/2)))
    sigma = sigma ** (1/2)

    z = (point - centroid) / (sigma)

    m_distance = np.dot(z, z) ** (1 / 2)

    return m_distance

def giveLabel(point, DS_dict, CS_dict, thre):
    """
    :param point: feature point
    :param DS_dict:  discard set dictionary
    :param CS_dict:   compressed set dictionary
    :param threshold: 2*root(d)
    :return: [1, -1, -1]  discard set label, compressed set label, retained set
    """
    ds_dis_list = []



    for label in range(len(DS_dict)):

        m_distance = Maha_Distance(np.array(point), DS_dict[label])

        ds_dis_list.append(m_distance)


    min_ds_dis = min(ds_dis_list)
    # if the nearest DS cluster < 2*root(d)
    if min_ds_dis < thre:

        return [ds_dis_list.index(min_ds_dis), -1, -1]
    else:
        if len(CS_dict) > 0: # if the nearest CS cluster < 2*root(d)
            cs_dis_list = []
            cs_dis_dict = {}
            for label in list(CS_dict.keys()):
                m_distance = Maha_Distance(np.array(point), CS_dict[label])
                cs_dis_list.append(m_distance)
                cs_dis_dict[m_distance] = label

            min_cs_dis = min(cs_dis_list)

            if min_cs_dis < thre:
                return [-1, cs_dis_dict[min_cs_dis], -1]
            else:
                return [-1, -1, 1]
        else:
            return [-1, -1, 1]

def update_DS(new_DS_points, DS):
    """
    :param new_DS_points: list of discard points
    :param DS: discard set
    :return: new discard set
    """
    for (key, fea) in new_DS_points:
        DS[key]["points"].append(fea)
        DS[key]["num"] = DS[key]["num"] + 1
        DS[key]["sum"] = DS[key]["sum"] + np.array(fea[2:])
        DS[key]["sumsq"] = DS[key]["sumsq"] + np.multiply(np.array(fea[2:]), np.array(fea[2:]))

def update_CS(new_CS_points, CS):
    """
    :param new_CS_points: list of compression points
    :param CS: compressed sets
    :return: new compressed sets
    """
    for (key, fea) in new_CS_points:

        CS[key]["points"].append(fea)
        CS[key]["num"] = CS[key]["num"] + 1
        CS[key]["sum"] = CS[key]["sum"] + np.array(fea[2:])
        CS[key]["sumsq"] = CS[key]["sumsq"] + np.multiply(np.array(fea[2:]), np.array(fea[2:]))

def Maha_Distance_two_cluster(cluster1, cluster2):
    """
    :param cluster1: cluster 1
    :param cluster2:  cluster 2
    :return:
    """

    centroid_1 = cluster1["sum"] / cluster1["num"]
    centroid_2 = cluster2["sum"] / cluster2["num"]

    sig_1 = cluster1["sumsq"] / cluster1["num"] - (cluster1["sum"] / cluster1["num"])**2
    sig_2 = cluster2["sumsq"] / cluster2["num"]  - (cluster2["sum"] / cluster2["num"])**2

    sig = (sig_2 ** (1/2) + sig_1 ** (1/2))/2

   # print("sig2:", (sig ** (1 / 2)))

    z = (centroid_1 - centroid_2)/sig

    m = np.dot(z, z) ** (1/2)
    return m
    # z_1 = (centroid_1 - centroid_2) / sig_1
    # z_2 = (centroid_1 - centroid_2) / sig_2
    #
    # m_1 = np.dot(z_1, z_1) ** (1 / 2)
    # m_2 = np.dot(z_2, z_2) ** (1 / 2)

    #return min(m_1, m_2)


def getClusterNum(clu):

    sum = 0
    for k, v in clu.items():
        sum = sum + v["num"]

    return sum


if __name__ == '__main__':

    t = time.time()
    # partition number
    partion_num = 30
    sc = SparkContext('local[*]', 'task')
    sc.setLogLevel('ERROR')

    # parameters
    # input_file = '../data/hw6_clustering.txt'
    # n_cluster = 10
    # output_file = '../res/result.txt'
    input_file = sys.argv[1]
    n_cluster = int(sys.argv[2])
    output_file =  sys.argv[3]


    file = open(output_file, 'w')
    file.write("The intermediate results:\n")

    RS_point_list = [] #（feature...)

    input_rdd = sc.textFile(input_file, partion_num)

    # read the data
    data_rdd = input_rdd\
        .map(lambda x: [int(fea) for fea in x.split(',')[:2]] + [float(fea) for fea in x.split(',')[2:]])\
        .cache()

    # the number of dataset
    data_num = data_rdd.count()

    # shuffle data to acquire the random 20% data
    shuffle_data = data_rdd.collect()
    random.shuffle(shuffle_data)

    # Step 1. Load 20% of the data randomly.
    collected_data = shuffle_data[0: int(0.2*data_num)]
    dim = len(collected_data[0]) - 2

    #print("dim:", dim)

    # Step 2. Run K-Means (e.g., from sklearn) with a large K (e.g., 5 times of the number of the input clusters)
    k_means = KMeans(n_clusters = n_cluster * 25).fit([row[2:] for row in collected_data])

    # step 3: move clusters with only one points to RS

    # RS labels with only one point
    RS_labels = sc.parallelize(k_means.labels_, partion_num) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .filter(lambda x: x[1] == 1) \
        .map(lambda x: x[0]).collect()

    # add the index of these points
    for row, label in zip(collected_data, k_means.labels_):
        if label in RS_labels:
            RS_point_list.append(row)

    # Step 4. Run K-Means again to cluster the rest of the data points with K = the number of input clusters.
    collected_data = [row for row in collected_data if row not in RS_point_list]
    k_means = KMeans(n_clusters = n_cluster).fit([row[2:] for row in collected_data])

    #print(len(set(k_means.labels_)))

    clus_num = len(set(k_means.labels_))
    # print(clus_num)

    # key: label; value: node feature
    c = sc.parallelize(collected_data).map(lambda x: (k_means.predict([x[2:]])[0], x)).cache()

    # the number of discard points
    # Step 5
    DS_num = c.count()
    DS = defaultdict(dict)
    # The number of points: N
    # The vector SUM, whose ith component is the sum of the coordinates of the points in the ith dimension
    # The vector SUMSQ: ith component = sum of squares of coordinates in ith dimension
    # key: label, value: feature

    ds_points = c.groupByKey().mapValues(list).collectAsMap()

    N = c.map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b).collectAsMap()

    SUM_dic = c.mapValues(lambda x: x[2:])\
        .reduceByKey(lambda a, b: elementwiseAdd(a,b)).collectAsMap()

    SUMSQ_dic = c.mapValues(lambda x: [fea ** 2 for fea in x[2:]])\
        .reduceByKey(lambda a,b: elementwiseAdd(a, b)).collectAsMap()



    for key in N.keys():
        DS[key]["points"] = ds_points[key]
        DS[key]["num"] = np.array(N[key])
        DS[key]["sum"] = np.array(SUM_dic[key])
        DS[key]["sumsq"] = np.array(SUMSQ_dic[key])



    # Step 6. Run K-Means on the points in the RS with a large K (e.g., 5 times of the number of the input clusters)
    # to generate CS (clusters with more than one points) and RS (clusters with only one point).
    try:
        k_means = KMeans(n_clusters = n_cluster * 5).fit([row[2:] for row in RS_point_list])
    except:
        k_means = KMeans(n_clusters = min(len(RS_point_list), n_cluster)).fit([row[2:] for row in RS_point_list])

    RS_count = 0
    CS_count = 0

    RS_labels = sc.parallelize(k_means.labels_, partion_num) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .filter(lambda x: x[1] == 1) \
        .map(lambda x: x[0]).collect()

    CS = defaultdict(dict) # store the CS information

    RS_tmp = []

    for row, label in zip(RS_point_list, k_means.labels_):

        if label in RS_labels:
            RS_tmp.append(row)
            RS_count += 1
        else:
            CS_count += 1
            if label not in CS:
                CS[label]["points"] = [row]
                CS[label]["num"] = 1
                CS[label]["sum"] = np.array(row[2:])
                CS[label]["sumsq"] = np.multiply(np.array(row[2:]), np.array(row[2:]))
            else:
                CS[label]["points"].append(row)
                CS[label]["num"] = CS[label]["num"] + 1
                CS[label]["sum"] = CS[label]["sum"] + np.array(row[2:])
                CS[label]["sumsq"] = CS[label]["sumsq"] + np.multiply(np.array(row[2:]), np.array(row[2:]))


    # print("minimun key of cs:", min(CS.keys()))
    # print("maximun key of cs:", max(CS.keys()))
    RS_point_list= RS_tmp

    file.write("Round {}: ".format(1) + str(getClusterNum(DS)) + "," + str(len(CS)) + "," + str(getClusterNum(CS)) + "," + str(RS_count) + '\n')
    #print("Round 1: " + str(DS_num) + "," + str(len(CS)) + "," + str(CS_count) + "," + str(RS_count) + '\n')

    threshold = 2 * (dim ** (1 / 2))  # 2 root 𝑑.

    # Step 7 - Step 12
    for round in [2, 3, 4, 5]:
        # Step 7. Load another 20% of the data randomly.
        if round == 5:
            collected_data = shuffle_data[int(0.2*data_num) * 4:]
        else:
            collected_data = shuffle_data[(round - 1) * int(0.2*data_num): round * int(0.2*data_num)]

        # Step 8. For the new points, compare them to each of the DS using the Mahalanobis Distance and assign
        # them to the nearest DSclusters if the distance is <2 root 𝑑.


        # collected_data_rdd = sc.parallelize(collected_data) \
        #     .map(lambda x: x[2:]).cache()  # DS, CS, RS

        collected_data_rdd = sc.parallelize(collected_data)\
            .map(lambda x: (x, giveLabel(x[2:], DS, CS, threshold))).cache() # DS, CS, RS

       # print(collected_data_rdd.take(10))

        new_DS_points = collected_data_rdd.filter(lambda x: x[1][0] != -1).map(lambda x: (x[1][0], x[0])).collect()
        new_CS_points = collected_data_rdd.filter(lambda x: x[1][1] != -1).map(lambda x: (x[1][1], x[0])).collect()
        new_RS_points = collected_data_rdd.filter(lambda x: x[1][2] != -1).map(lambda x: x[0]).collect()

        DS_num = DS_num + len(new_DS_points)



        update_CS(new_CS_points, CS)
        update_DS(new_DS_points, DS)
        RS_point_list = RS_point_list + new_RS_points

        # Step 11. Run K-Means on the RS with a large K
        try:
            k_means = KMeans(n_clusters=n_cluster * 5).fit([row[2:] for row in RS_point_list])
        except:
            k_means = KMeans(n_clusters=min(len(RS_point_list), n_cluster)).fit([row[2:] for row in RS_point_list])

        RS_labels = sc.parallelize(k_means.labels_, partion_num) \
            .map(lambda x: (x, 1)) \
            .reduceByKey(lambda a, b: a + b) \
            .filter(lambda x: x[1] == 1) \
            .map(lambda x: x[0]).collect()

        RS_tmp = []
        RS_count = 0 # only RS count should be 0

        for row, label in zip(RS_point_list, k_means.labels_):
            if label in RS_labels:
                RS_tmp.append(row)
                RS_count += 1
            else:
                label = label + 1000*round
                CS_count += 1
                if  label not in CS:
                    CS[label]["points"] = [row]
                    CS[label]["num"] = 1
                    CS[label]["sum"] = np.array(row[2:])
                    CS[label]["sumsq"] = np.multiply(np.array(row[2:]), np.array(row[2:]))
                else:
                    CS[label]["points"].append(row)
                    CS[label]["num"] = CS[label]["num"] + 1
                    CS[label]["sum"] = CS[label]["sum"] + np.array(row[2:])
                    CS[label]["sumsq"] = CS[label]["sumsq"] + np.multiply(np.array(row[2:]), np.array(row[2:]))

        RS_point_list = RS_tmp

        while True:
            compare_list = list(combinations(list(CS.keys()), 2))
            old_cluster = set(CS.keys())
            merge_list = []
            for (idx1, idx2) in compare_list:

                m_distance = Maha_Distance_two_cluster(CS[idx1], CS[idx2])
                if m_distance < threshold:
                    CS[idx1]["points"] += CS[idx2]["points"]
                    CS[idx1]["num"] += CS[idx2]["num"]
                    CS[idx1]["sum"] += CS[idx2]["sum"]
                    CS[idx1]["sumsq"] += CS[idx2]["sumsq"]
                    del CS[idx2]
                    break
            new_cluster = set(CS.keys())
            # no clusters to merge(no update)
            if new_cluster == old_cluster:
                break

        # last run merge CS clusters with DS clusters that have a Mahalanobis Distance < 2√𝑑.
        if round == 5:

            for cs_idx, value in list(CS.items()):
                dis_list = []
                dis_dic = {}
                for ds_idx in DS.keys():
                    dis = Maha_Distance_two_cluster(CS[cs_idx], DS[ds_idx])
                    dis_list.append(dis)
                    dis_dic[dis] = ds_idx
                min_dis = min(dis_list)
                if min_dis < threshold:

                    ds_merge_idx = dis_dic[min_dis] # get the merge index of discard set
                    # merge the discard set with compressed set
                   # CS_count = CS_count - CS[cs_idx]["num"]
                   # DS_num = DS_num + CS[cs_idx]["num"]
                    DS[ds_merge_idx]["points"] += CS[cs_idx]["points"]
                    DS[ds_merge_idx]["num"] += CS[cs_idx]["num"]
                    DS[ds_merge_idx]["sum"] += CS[cs_idx]["sum"]
                    DS[ds_merge_idx]["sumsq"] += CS[cs_idx]["sumsq"]
                    del CS[cs_idx]

        file.write("Round {}: ".format(round) + str(getClusterNum(DS)) + "," + str(len(CS)) + "," + str(getClusterNum(CS)) + "," + str(RS_count) + '\n')

    file.write("\n")
    file.write("The clustering results:\n")

    cluster_res = {}

    for rs_point in RS_point_list:
        cluster_res[rs_point[0]] = -1

    for key, val in CS.items():
        for cs_point in CS[key]["points"]:
            cluster_res[cs_point[0]] = -1

    for key, val in DS.items():
        for ds_point in DS[key]["points"]:
            cluster_res[ds_point[0]] = key

    clust_res_out = sorted(cluster_res.items(), key = lambda kv: kv[0])

    for (key, val) in clust_res_out:
        file.write(str(key) + "," + str(val) + "\n")

    file.close()
































