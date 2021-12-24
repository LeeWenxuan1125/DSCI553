#_author: Wenxuan Li
#_USC_ID: 8329-9604-25

from pyspark import SparkContext
import json
import os
import sys
from operator import add
import datetime

def count_reviews_number(review_rdd):
    """
    :param review_rdd:  review data
    :return: the total number of reviews
    """
    return review_rdd.count()

def number_reviews_2018(review_rdd):
    """
    :param review_rdd: review data
    :return: The number of reviews in 2018
    """
    return review_rdd.filter(lambda x:
                             datetime.datetime.strptime(x['date'], '%Y-%m-%d %H:%M:%S')
                             .year == 2018).count()

def number_distinct_users(review_rdd):
    """
    :param review_rdd: review_data
    :return: the number of distinct users who wrote reviews
    """
    return review_rdd.map(lambda x:x['user_id']).distinct().count()

def top10_users_wrote_reviews(review_rdd):
    """
    :param review_rdd: review_data
    :return: The top 10 users who wrote the largest numbers of reviews and the number of reviews they wrote
    """
    return review_rdd.map(lambda x:(x['user_id'],1))\
        .reduceByKey(add).sortBy(lambda x: (-x[1], x[0])).take(10)

def distinct_reviewed_business(review_rdd):
    """
    :param review_rdd: review data
    :return: The number of distinct businesses that have been reviewed
    """
    return review_rdd.map(lambda x:x['business_id']).distinct().count()

def top10_business_with_largest_nums(review_rdd):
    """
    :param review_rdd: review data
    :return: The top 10 businesses that had the largest numbers of reviews and the number of reviews they had
    """
    return review_rdd.map(lambda x:(x['business_id'],1)).\
        reduceByKey(add).sortBy(lambda x: (-x[1],x[0])).take(10)


if __name__ == '__main__':

    review_filepath = sys.argv[1]
    output_filepath = sys.argv[2]

    sc = SparkContext.getOrCreate()
    sc.setLogLevel("ERROR")
    review_rdd = sc.textFile(review_filepath).map(lambda row: json.loads(row))

    result_dict = {}
    result_dict['n_review'] = count_reviews_number(review_rdd)
    result_dict['n_review_2018'] = number_reviews_2018(review_rdd)
    result_dict['n_user'] = number_distinct_users(review_rdd)
    result_dict['top10_user'] = top10_users_wrote_reviews(review_rdd)
    result_dict['n_business'] = distinct_reviewed_business(review_rdd)
    result_dict['top10_business'] = top10_business_with_largest_nums(review_rdd)

    print(result_dict)

    out_file = open(output_filepath, 'w')
    json.dump(result_dict, out_file, indent=4)




#_author: Wenxuan Li
#_USC_ID: 8329-9604-25

from pyspark import SparkContext
import json
import os
import sys
from operator import add
import datetime

def count_reviews_number(review_rdd):
    """
    :param review_rdd:  review data
    :return: the total number of reviews
    """
    return review_rdd.count()

def number_reviews_2018(review_rdd):
    """
    :param review_rdd: review data
    :return: The number of reviews in 2018
    """
    return review_rdd.filter(lambda x:
                             datetime.datetime.strptime(x['date'], '%Y-%m-%d %H:%M:%S')
                             .year == 2018).count()

def number_distinct_users(review_rdd):
    """
    :param review_rdd: review_data
    :return: the number of distinct users who wrote reviews
    """
    return review_rdd.map(lambda x:x['user_id']).distinct().count()

def top10_users_wrote_reviews(review_rdd):
    """
    :param review_rdd: review_data
    :return: The top 10 users who wrote the largest numbers of reviews and the number of reviews they wrote
    """
    return review_rdd.map(lambda x:(x['user_id'],1))\
        .reduceByKey(add).sortBy(lambda x: (-x[1], x[0])).take(10)

def distinct_reviewed_business(review_rdd):
    """
    :param review_rdd: review data
    :return: The number of distinct businesses that have been reviewed
    """
    return review_rdd.map(lambda x:x['business_id']).distinct().count()

def top10_business_with_largest_nums(review_rdd):
    """
    :param review_rdd: review data
    :return: The top 10 businesses that had the largest numbers of reviews and the number of reviews they had
    """
    return review_rdd.map(lambda x:(x['business_id'],1)).\
        reduceByKey(add).sortBy(lambda x: (-x[1],x[0])).take(10)


if __name__ == '__main__':

    review_filepath = sys.argv[1]
    output_filepath = sys.argv[2]

    sc = SparkContext.getOrCreate()
    sc.setLogLevel("ERROR")
    review_rdd = sc.textFile(review_filepath).map(lambda row: json.loads(row))

    result_dict = {}
    result_dict['n_review'] = count_reviews_number(review_rdd)
    result_dict['n_review_2018'] = number_reviews_2018(review_rdd)
    result_dict['n_user'] = number_distinct_users(review_rdd)
    result_dict['top10_user'] = top10_users_wrote_reviews(review_rdd)
    result_dict['n_business'] = distinct_reviewed_business(review_rdd)
    result_dict['top10_business'] = top10_business_with_largest_nums(review_rdd)

    print(result_dict)

    out_file = open(output_filepath, 'w')
    json.dump(result_dict, out_file, indent=4)




