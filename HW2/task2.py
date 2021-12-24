# Wenxuan Li
# 09/25/2021

from pyspark import SparkContext, SparkConf, StorageLevel
from itertools import combinations
import collections
import math
from functools import reduce
from operator import add
from operator import add
import time
import sys

BUCKET_NUMBER = 1000000

def hash_func(combination):
    """

    :param combination:  each combination
    :return: the index of hash table
    """
    result = sum(map(lambda x: int(x), list(combination)))
    index = result % BUCKET_NUMBER
    return index

def strip_0(x):
    return (x[0], x[1], x[2].lstrip('"0'))

def f(x):
    return x


def check_bitmap(combination, bitmap):
    """
    check if its hash result in bitmap
    :param combination: each combination
    :param bitmap:corresponding bitmap
    :return:
    """
    index = hash_func(combination)
    return bitmap[index]


def unionAll(fre_item):
    """

    :param fre_item:
    :return:
    """
    result = set()
    for i in fre_item:
       result = result.union(i)

    return result


def generate_fre_combinations(subset_baskets, original_support, total_baskets_num):
    """
    find candidate itemsets in each chunk
    :param subset_baskets: partiton of baskets
    :param original_support: specific support
    :param total_baskets_num: baskets number
    :return: frequent itemsets
    """
    subset_baskets_list = list(subset_baskets)
   # print(subset_baskets_list)
    candidate_fre_item = collections.defaultdict(list) # store all combination result

    new_support = original_support * len(subset_baskets_list)/ total_baskets_num

    print("new support for each chunk:", new_support)

    temp_count_dic = collections.defaultdict(int) # 临时用来计数的

    bitmap_2 = [0 for _ in range(BUCKET_NUMBER)]
    bitmap_3 = [0 for _ in range(BUCKET_NUMBER)]

    for each_basket in subset_baskets_list:
        for item in each_basket:
            temp_count_dic[item] = temp_count_dic[item] + 1

        for pair in combinations(each_basket, 2):
            key = hash_func(pair)
            bitmap_2[key] = (bitmap_2[key] + 1)

        for pair in combinations(each_basket, 3):
            key = hash_func(pair)
            bitmap_3[key] = (bitmap_3[key] + 1)


    filter_dic = {k:v for k,v in temp_count_dic.items() if v >= new_support}

    # check bitmap
    bitmap_2 = list(map(lambda value: True if value >= new_support else False, bitmap_2))
    bitmap_3 = list(map(lambda value: True if value >= new_support else False, bitmap_3))

    # frequent singleton
    fre_singleton = [tuple([x]) for x in list(filter_dic.keys())]

    candidate_fre_item[1] = fre_singleton # sorted in lexicographical order
    fre_num = len(fre_singleton)

  #  print("1：", fre_num)

    index = 2

   # print(fre_singleton)
 #   print("length of single:", len(fre_singleton))
  #  print("fre_num:", fre_num)
    while fre_num > 0 :   # the number of lower layer frequent itemset > 0
        temp_count_dic = collections.defaultdict(int) # new a dictionary

        temp_set = unionAll(candidate_fre_item[index - 1])

        candidate_items = list(combinations(temp_set , index))

        print(index)

        for dummy_i in candidate_items:

            if index == 2:
                if False == check_bitmap(dummy_i, bitmap_2):
                    continue

            if index == 3:
                if False == check_bitmap(dummy_i, bitmap_3):
                    continue


            for each_basket in subset_baskets_list:

                dummy_i = tuple(sorted(dummy_i))

                if set(dummy_i).issubset(set(each_basket)):

                    possible_subset = list(combinations(dummy_i, index - 1))
                    sort_possible_subset = [tuple(sorted(x)) for x in possible_subset]

                    if all(item in candidate_fre_item[index - 1] for item in sort_possible_subset):

                        temp_count_dic[dummy_i] = temp_count_dic[dummy_i] + 1

        # for each_basket in subset_baskets_list: # list
        #
        #     candidate_items = list(combinations(each_basket, index)) #
        #
        #     for dummy_i in candidate_items:
        #        # possible_subset = [x[0] if len(x) == 1 else x for x in list(combinations(dummy_i, index - 1))]  # [('2'，), ('1',)] -> [('2', '1')]
        #         dummy_i = tuple(sorted(dummy_i))
        #         possible_subset = list(combinations(dummy_i, index - 1))
        #
        #         sort_possible_subset = [tuple(sorted(x)) for x in possible_subset]
        #
        #         if all(item in candidate_fre_item[index - 1] for item in sort_possible_subset): # 如果一个set的所有子集在之前的frequent中, 那么计数
        #
        #             temp_count_dic[dummy_i] = temp_count_dic[dummy_i] + 1


        filter_dic = {k: v for k, v in temp_count_dic.items() if v >= new_support}

        fre_items = [tuple(sorted(x)) for x in list(filter_dic.keys())]
        if len(fre_items) != 0:
            candidate_fre_item[index] = fre_items # sorted in lexicographical order

        fre_num = len(fre_items)
        print("{}: {}".format(index, fre_num))
       # print(fre_num)

        index = index + 1

   # print("candidate_fre_item:", candidate_fre_item)

    return reduce(lambda val1, val2: val1 + val2, candidate_fre_item.values())


def count_frequent_itemset(subset_baskets, candidate_pairs):
    """
    count the actual number of those candidate itemsets
    :param subset_baskets: each baskets [only one baskets]
    :param candidate_pairs: all candidate pairs
    :return: (candidate itemset: count)
    """
    temp_counter = collections.defaultdict(int)

    for pairs in candidate_pairs:
      #  print("pair:", pairs)
        if set(pairs).issubset(set(subset_baskets)):
            temp_counter[pairs] += 1

    return [(key, value) for key, value in temp_counter.items()]

def out_format(itemset):
    """
    format all the frequent pairs as output string
    :param itemset: list of tuples of frequent itemset
    :return: string output
    """

    last_num = 1
    str_out = str()

    for item in itemset:

        if  last_num == len(item):
            if 1 == len(item):
                str_out = str_out + '(' + str(item)[1:-2] + '),'
            else:
                str_out = str_out + str(item) + ","
        else:
            str_out = str_out.strip(',')
            str_out += '\n\n'
            str_out = str_out + str(item)+ ","
            last_num += 1

    str_out = str_out.strip(',')

    return str_out

def export_to_file(output_file, candidate_itemsets, frequent_itemsets):
    """

    :param output_file: output txt file
    :param candidate_itemsets:  candidate itemsets
    :param frequent_itemsets:   frequent itemsets
    :return:
    """
    str_result = 'Candidates:\n' + out_format(candidate_itemsets) + '\n\n' \
                 + 'Frequent Itemsets:\n' + out_format(frequent_itemsets)
   # print(str_result)
    with open(output_file, 'w') as f:
        f.write(str_result)
        f.close()


if __name__ == '__main__':


    start = time.time()

    input_file = './data/ta_feng_all_months_merged.csv'
    output_file = 'output_2.csv'
    filter_threshold = 20
    support = 50

    #
    # filter_threshold = int(sys.argv[1])
    # support = int(sys.argv[2])
    # input_file = sys.argv[3]
    # output_file = sys.argv[4]


    configuration = SparkConf().setAppName("test").setMaster("local[*]")
    configuration.set("spark.driver.memory", "4g")
    configuration.set("spark.executor.memory", "4g")
    sc = SparkContext.getOrCreate(configuration)

  #  sc = SparkContext.getOrCreate()

    input_rdd = sc.textFile(input_file, 5)
    head = input_rdd.first()

    pre_rdd = input_rdd.filter(lambda x: x != head)\
        .map(lambda x: [x.split(',')[0].strip('"'), x.split(',')[1].strip('"'), x.split(',')[5].strip('"')])\
        .map(lambda x: strip_0(x)).map(lambda x: (x[0][:-4] + x[0][-2:] + '-' + x[1], x[2]))

    # output the intermdiate csv
    # print(pre_rdd.take(10))
    with open("customer_product.csv", 'w') as f:
        f.write('DATE-CUSTOMER_ID,PRODUCT_ID')
        for (customer, product) in pre_rdd.collect():
            f.write('\n')
            f.write("{0},{1}".format(customer, product))


    basket_rdd = pre_rdd.groupByKey().mapValues(lambda x: list(set(list(x)))).filter(lambda x: len(x[1]) > 20)

    total_baskets_num = basket_rdd.count()

    candidate_itemset = basket_rdd.map(lambda x: x[1]) \
        .mapPartitions(
        lambda partition: generate_fre_combinations(
            subset_baskets=partition,
            original_support=support,
            total_baskets_num=total_baskets_num
        )).distinct().sortBy(lambda x: (len(x), x)).collect()

    frequent_itemset = basket_rdd.map(lambda x: x[1]) \
        .flatMap(lambda rdd: count_frequent_itemset(subset_baskets=rdd, candidate_pairs=candidate_itemset)) \
        .reduceByKey(add).filter(lambda x: x[1] >= support) \
        .map(lambda x: x[0]).sortBy(lambda x: (len(x), x)) \
        .collect()



    end = time.time()
    print("Duration:{}".format(end - start))

    export_to_file(output_file, candidate_itemset, frequent_itemset)


    # with open(output_file, 'w') as f:
    #     f.write('DATE-CUSTOMER_ID,PRODUCT_ID')
    #     for (cus_id, pro_id) in pre_rdd.collect():
    #         f.write('\n')
    #         f.write("{0},{1}".format(cus_id, pro_id))



