from blackbox import BlackBox
import random
import sys
import binascii
import time

# filter bit array
filter = [0] * 69997
# keep track of previous users
previous_users = set()

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
    a_list, b_list = hash_paras(2)
    for (a, b) in zip(a_list, b_list):
        hash_value = ( (a*s + b) % 2333333 ) % 69997
        res.append(hash_value)
    return res



def getfr(stream_users):
    """
    :param stream_users: users group
    :return: get false positive rate this time
    """
    FP = 0
    N = 0

    for user in stream_users:
        hash_vals = myhashs(user)
        if user not in previous_users:
            flag = False
            N += 1
            for i in hash_vals:
                if filter[i] == 0:
                    flag = True
            if False == flag:
                FP += 1

        for i in hash_vals:
            filter[i] = 1

    return FP/N


if __name__ == '__main__':

    start = time.time()

    random.seed(553)

    # input_filename = '../data/users.txt'
    # stream_size = 100
    # num_of_asks = 50
    # output_filename = '../data/task1_output.txt'

    input_filename = sys.argv[1]
    stream_size = int(sys.argv[2])
    num_of_asks = int(sys.argv[3])
    output_filename = sys.argv[4]

    bx = BlackBox()

    # fpr res
    fpr_list = []

    for dummy_i in range(num_of_asks):
        stream_users = bx.ask(input_filename, stream_size)
        fpr = getfr(stream_users)
        fpr_list.append(fpr)
        previous_users = previous_users.union(set(stream_users))


    with open(output_filename, 'w') as f:
        f.write("Time,FPR\n")
        for idx, fpr in enumerate(fpr_list):
            f.write(str(idx) + "," + str(fpr))
            f.write("\n")

        f.close()

    print("Duration : ", time.time() - start)




