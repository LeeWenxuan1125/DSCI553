from blackbox import BlackBox
import random
import sys
import binascii
import time

def hash_paras(num):
    """
    :param num: the number of hash function
    :return: the hash parameters
    """
    a_list = random.sample(range(1, sys.maxsize - 1), num)
    b_list = random.sample(range(1, sys.maxsize - 1), num)
    return a_list, b_list

def myhashs(user):
    """
    :param user: the user string
    :return: the hash value of each hash function
    """
    res = []
    s = int(binascii.hexlify(user.encode("utf8")), 16)
    a_list, b_list = hash_paras(100)
    for (a, b) in zip(a_list, b_list):
        hash_value = ( (a*s + b) % 233333 ) % 1399
        res.append(hash_value)
    return res

def getCount(stream_users):

    zero_value_list = [0]*100
    for user in stream_users:
        res = myhashs(user)
        for idx in range(len(res)):
            trailing_zero_num  =  len(bin(res[idx]).split('1')[-1])
            zero_value_list[idx] = max(zero_value_list[idx], trailing_zero_num)

    # 2^num
    for idx in range(len(zero_value_list)):
        zero_value_list[idx] = 2**zero_value_list[idx]

    # group hash functions to 5 group
    group_num = 10
    group_hash = [[]]*group_num
    count_list = [0]*group_num
    for idx, zero_val in enumerate(zero_value_list):
        group_hash[idx % group_num].append(zero_val)

    for idx, val in enumerate(group_hash):
        count_list[idx] = int(sum(val)/len(val))

    count_list = sorted(count_list)

    if 0 == group_num % 2:
        return int((count_list[int(group_num/2)] + count_list[int(group_num/2) + 1])/2)
    else:
        return count_list[int(group_num/2)]


if __name__ == '__main__':

    start = time.time()

    # input_filename = '../data/users.txt'
    # stream_size = 300
    # num_of_asks = 50
    # output_filename = '../data/task2_output.txt'

    input_filename = sys.argv[1]
    stream_size = int(sys.argv[2])
    num_of_asks = int(sys.argv[3])
    output_filename = sys.argv[4]

    bx = BlackBox()
    estimate_count_list = []
    for dummy_i in range(num_of_asks):
        stream_users = bx.ask(input_filename, stream_size)
        count = getCount(stream_users)
        estimate_count_list.append(count)

    with open(output_filename, 'w') as f:
        f.write("Time,Ground Truth,Estimation\n")
        for idx, count in enumerate(estimate_count_list):
            f.write(str(idx) + "," + str(stream_size) + "," + str(count))
            f.write("\n")

        f.close()

    print("Duration : ", time.time() - start)

