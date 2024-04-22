from pyspark import SparkContext, SparkConf
import sys

class proj3:
    def run(self, input1, input2, tau, outputPath):
        conf = SparkConf().setAppName("proj2")
        sc = SparkContext(conf=conf)


        file1 = sc.textFile(input1)
        file2 = sc.textFile(input2)
        # file1.collect()

        # remove ''
        file1 = file1.filter(lambda x: True if len(x) != 0 else False)
        file2 = file2.filter(lambda x: True if len(x) != 0 else False)

        temp1 = file1.map(lambda line: (1, int(line.strip().split(" ", 1)[0]), list(set([int(i) for i in line.strip().split(" ", 1)[1].split(" ")]))))
        temp2 = file2.map(lambda line: (2, int(line.strip().split(" ", 1)[0]), list(set([int(i) for i in line.strip().split(" ", 1)[1].split(" ")]))))
        # temp1.collect()                 # (flag, index, num_list)    only type(int) in num_list

        temp = temp1.union(temp2).cache()           # use cache to mark where the restart is
        # temp.collect()

        # wordcount for num in num_list
        def func1(line):
            temp = []
            for i in line:
                temp.append((i, 1))
            return temp

        wordcount = temp.map(lambda x: x[2]).flatMap(func1).reduceByKey(lambda a, b: a + b)
        # wordcount.collect()

        # ordering and raise a boardcast for wordcount since it is widely used in ordering
        # order_list = sorted(wordcount.collectAsMap().items(),key=lambda x: x[1])
        order_dict = wordcount.collectAsMap()
        bc_order = sc.broadcast(order_dict)
        # order_dict

        # make num_list ordering by wordcount, if n1 and n2 have the same wordcount, sorted by n self.
        tmp = temp.map(lambda x: (x[0], x[1], sorted(x[2], key=lambda y: (bc_order.value[y], y))))
        # tmp.collect()

        # prefix filter
        def func2(line):
            flag, index, num_list = line
            temp = []
            for num in num_list:
                temp.append((num, line))
            return temp

        tmp2 = tmp.flatMap(func2).groupByKey().mapValues(list)
        # tmp2.collect()

        tau = float(tau)
        def func3(line):
            num, temp_list = line
            result = []
            if len(temp_list) > 1:
                for temp1 in temp_list:
                    flag1, index1, list1 = temp1
                    if flag1 == 1:
                        for temp2 in temp_list:
                            flag2, index2, list2 = temp2
                            if flag2 == 2:
                                num1 = len(set(list1).intersection(set(list2)))
                                num2 = len(set(list1).union(list2))
                                num3 = num1 / num2
                                #                         result.append((index1,index2))
                                if num3 >= tau:
                                    result.append((index1, index2, "%.6f" % num3))
            return result

        tmp3 = tmp2.flatMap(func3).distinct().sortBy(lambda x: (x[0], x[1]))
        tmp3.collect()

        # tmp4 = tmp3.map(lambda x:(str((x[0],x[1]))+'\t'+str(float(x[2]))))
        tmp4 = tmp3.map(lambda x: f'({x[0]},{x[1]})\t{str(float(x[2]))}')
        tmp4.collect()

        tmp4.coalesce(1).saveAsTextFile(outputPath)
        sc.stop()

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Wrong inputs")
        sys.exit(-1)
    proj3().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])


