from pyspark import SparkContext
import os
from itertools import combinations
import sys
from collections import defaultdict
import time
import copy

def fun_merge(a, b):
    return set(a).union(set(b))


#def flagBFSLoop(node_list, level_node)


def BFS(root, adja_list):

    label_list = [1]*len(adja_list.keys())
    visited_list = [False]*len(adja_list.keys())
    # key: level, value the nodes in this level
    level_node = defaultdict(list)
    # key: level, value: the edge in this level(key: child node, value: father node)
    level_edge = {}

    level_node[0].append(root)
    visited_list[root] = True
    temp_level_node = level_node[0]
    index = 0
   # while False == all(visited_list):
    while True:
        index = index + 1

        level_edge[index] = defaultdict(list)
        # iterate all the nodes in this level
        for node in temp_level_node:
            for child in adja_list[node]:
                if False == visited_list[child]:
                   # isited_list[child] = True # set this child node is visited
                    level_node[index].append(child) # append the new node to this level
                    level_edge[index][child].append(node)

        if 0 == len(list(level_node[index])):
            del level_node[index]
            del level_edge[index]
            break

        level_node[index] = list(set(level_node[index])) # remove duplicate nodes in this level

        temp_level_node = level_node[index]
        for idx in temp_level_node:
            visited_list[idx] = True

        for k1, v1 in level_edge.items():
            for k2, v2 in v1.items():
                label_list[k2] = len(v2)

    return level_node, level_edge, label_list
#
def getBetweeness(node_num, level_node, level_edge, label_list):
    """

    :param level_node: defaultdict(<class 'list'>, {0: [0], 1: [1, 2], 2: [3, 4, 5, 6]})
    :param level_edge: {1: defaultdict(<class 'list'>, {1: [0], 2: [0]}), 2: defaultdict(<class 'list'>, {3: [1], 4: [1], 5: [1, 2], 6: [2]})}
    :param label_list: the label each node by the number of shortest paths that reach it from the root.
    :return: dictionary (key: edge, value: betweens ness)
    """
    node_value = [1]*node_num
    level_num = len(level_node)


    res = {}

    for idx in range(level_num-1, 0, -1):
        for node in level_node[idx]:
            #
            denominator = sum([label_list[i] for i in level_edge[idx][node]])
          #  add_value = node_value[node]/len(level_edge[idx][node])

            for father_node in level_edge[idx][node]:

                add_value = node_value[node]*(label_list[father_node]/denominator)

                node_value[father_node] = node_value[father_node] + add_value

                edgeKey = tuple(sorted((node, father_node)))

                res[edgeKey] = add_value

    resList = []
    for item in res.items():
        resList.append(item)

    return resList


def BFS_getBetweeness(node_num, root, adja_list):
   # print(root)
    level_node, level_edge, label_list = BFS(root, adja_list)
 #   print(level_node, level_edge)
    res = getBetweeness(node_num, level_node, level_edge, label_list)
  #  print(res)
    return res

def edgeTransformIdx(edge, user_to_idx):
    """
    :param edge:  ("string", "decey")
    :param user_to_idx: (3, 4)
    :return:
    """
    return (user_to_idx[edge[0]], user_to_idx[edge[1]])





if __name__ == '__main__':

    # adja_list = {0: [1, 2], 1:[0, 3], 2:[0, 3, 4], 3:[1, 2, 5], 4:[2, 5], 5:[3, 4]}
    # level_node, level_edge, label_list = BFS(0, adja_list)
    # print(level_node)
    # print(level_edge)
    # print(label_list)
    # print(BFS_getBetweeness(6, 0, adja_list))

    start = time.time()

    # filter_threshold = 7
    # input_file = "../data/ub_sample_data.csv"
    # betweenness_output_file = "./res/betweenness.txt"
    # community_output_file_path = "./res/community.txt"

    filter_threshold = int(sys.argv[1])
    input_file = sys.argv[2]
    betweenness_output_file = sys.argv[3]
    community_output_file_path = sys.argv[4]

    sc = SparkContext('local[*]', 'task1')
    sc.setLogLevel('ERROR')

    input_RDD = sc.textFile(input_file, 10)
    header = input_RDD.first()
    raw_rdd = input_RDD.filter(lambda x: x != header)

    # key: user   value: list of businesses that this user have rated
    user_bus_dic = raw_rdd.map(lambda x: (x.split(',')[0], x.split(',')[1])) \
        .groupByKey().mapValues(list) \
        .filter(lambda x: len(x[1]) >= filter_threshold) \
        .collectAsMap()

    # potential user list
    user_list = user_bus_dic.keys()

    # potential combination
    candidate_pairs = list(combinations(user_list, 2))

    # edge rdd
    edge_rdd = sc.parallelize(candidate_pairs, 10) \
        .map(lambda x: (x, len(set(user_bus_dic[x[0]]).intersection(set(user_bus_dic[x[1]]))))) \
        .filter(lambda x: x[1] >= filter_threshold).cache()


    user_list_inEdge = sorted(list(edge_rdd.map(lambda x:(1, x[0])).reduceByKey(lambda a,b: fun_merge(a, b)).collect()[0][1]))

    # give integer label to each user
    user_to_idx = {}
    for index, user in enumerate(user_list_inEdge):
        user_to_idx[user] = index
    idx_to_user = {v: k for k, v in user_to_idx.items()}

    edge_list = edge_rdd.map(lambda x: x[0])\
        .map(lambda x: (min(user_to_idx[x[0]], user_to_idx[x[1]]), max(user_to_idx[x[0]], user_to_idx[x[1]]))).collect()

    user_idx_list_inEdge = list(idx_to_user.keys())

    edge_rdd = edge_rdd.map(lambda x: x[0])\
        .flatMap(lambda x: [(user_to_idx[x[0]], user_to_idx[x[1]]), (user_to_idx[x[1]], user_to_idx[x[0]])])\
        .groupByKey().mapValues(list)

    adja_list = edge_rdd.collectAsMap()

    # the total number of nodes in the social network graph
    node_num = len(adja_list.keys())

  #  print (user_idx_list_inEdge)

    user_idx_list_inEdge_rdd = sc.parallelize(user_idx_list_inEdge, 10)

    betweeness_rdd = user_idx_list_inEdge_rdd\
        .flatMap(lambda x: BFS_getBetweeness(node_num, x, adja_list))\
        .reduceByKey(lambda a,b : a+b).mapValues(lambda x: round(x/2, 5)).sortBy(lambda x: (-x[1], x[0]))\
        .map(lambda x: ((idx_to_user[x[0][0]], idx_to_user[x[0][1]]), x[1]))

    betw_res = betweeness_rdd.collect()

    # write the betweeness result to file
    with open(betweenness_output_file, "w") as f:
        for (pair, val) in betw_res:
            tmp = "('"
            tmp += pair[0]
            tmp += "', '"
            tmp += pair[1]
            tmp += "'),"
            tmp += str(val)

            f.write(tmp)
            f.write("\n")

#    construt the original adjacent matrix and len

    m = len(edge_list)
    A = [[0 for j in user_idx_list_inEdge] for i in user_idx_list_inEdge]
    for i, j in edge_list:
        A[i][j] = 1
        A[j][i] = 1

    #Modularity
    oldQ = 0
    newQ = 0
    old_community = []
    new_community = []
    community_list = []

    while newQ >= oldQ:
        # print(newQ)
        # remove edge
        largest_betweeness = betw_res[0][1]
        for edgeWithValue in betw_res:
            if edgeWithValue[1] == largest_betweeness:
                (i, j) = edgeTransformIdx(edgeWithValue[0], user_to_idx)
                # remove this edge from adjacent list
                adja_list[i].remove(j)
                adja_list[j].remove(i)
                adja_list = dict(filter(lambda ele: len(ele[1]) > 0, adja_list.items()))
            else:
                break

        # get ki and kj, node degree
        node_degree_dict = defaultdict(int)
        for node, v in adja_list.items():
            node_degree_dict[node] = len(v)

        # get new edge betweenness
        betweeness_rdd = user_idx_list_inEdge_rdd \
            .flatMap(lambda x: BFS_getBetweeness(node_num, x, adja_list)) \
            .reduceByKey(lambda a, b: a + b).mapValues(lambda x: round(x / 2, 5)).sortBy(lambda x: (-x[1], x[0])) \
            .map(lambda x: ((idx_to_user[x[0][0]], idx_to_user[x[0][1]]), x[1]))
        betw_res = betweeness_rdd.collect()

        old_community = community_list
        community_list = []
        node_list_tmp = copy.deepcopy(user_idx_list_inEdge)

        while len(node_list_tmp) >= 1:
            root = node_list_tmp[-1]
            level_node, level_edge, label_list = BFS(root, adja_list)

            # get visted nodes
            commmunity = []
            for k,v in level_node.items():
                commmunity = commmunity + v

            commmunity = list(set(commmunity))
            community_list.append(commmunity)
            node_list_tmp = list(set(node_list_tmp).difference(set(commmunity)))


        # calculate Q
        oldQ = newQ
        newQ = 0
        for community in community_list:
            g = 0
            for i in community:
                for j in community:
                        g += A[i][j] - (node_degree_dict[i] * node_degree_dict[j]) / (2 * m)
            newQ = newQ + g
        newQ = newQ/(2*m)
        new_community = community_list

    old_community = [sorted([idx_to_user[item] for item in community]) for community in old_community]
    old_community = sorted(old_community, key=lambda x: (len(x), x))

    f = open(community_output_file_path, "w")
    for community in old_community:
        line = ""
        for node in community:
            line += "'" + str(node) + "', "
        line = line.rstrip().rstrip(',')
        f.write(line)
        f.write("\n")
    f.close()

    end = time.time()
    print("Duration:", end - start)











