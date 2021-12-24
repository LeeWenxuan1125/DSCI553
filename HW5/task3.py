from blackbox import BlackBox
import sys
import time
import random

if __name__ == '__main__':

    start = time.time()
    #
    #
    input_filename = sys.argv[1]
    stream_size = int(sys.argv[2])
    num_of_asks = int(sys.argv[3])
    output_filename = sys.argv[4]

    # input_filename = '../data/users.txt'
    # stream_size = 100
    # num_of_asks = 30
    # output_filename = '../data/task3_1_output.txt'

    random.seed(553)

    bx = BlackBox()
    count = 0
    sample = []

    with open(output_filename, 'w') as f:
        f.write("seqnum,0_id,20_id,40_id,80_id")
        f.write('\n')
        for i in range(num_of_asks):
            stream_data = bx.ask(input_filename, stream_size)
            for user in stream_data:
                count += 1
                if count <= 100:
                    sample.append(user)
                else:
                    p = random.random()
                    if p > 100 / count:
                        pass
                    elif p <= 100 / count:
                        b = random.randint(0, 99)
                        sample[b] = user
            seqnum = stream_size * (i+1)

            id0 = sample[0]
            id20 = sample[20]
            id40 = sample[40]
            id60 = sample[60]
            id80 = sample[80]
            f.write(str(seqnum) + "," + id0 + "," + id20 + "," + id40 + "," + id60 + "," + id80 + '\n')
    f.close()

    print("Duration : ", time.time() - start)

