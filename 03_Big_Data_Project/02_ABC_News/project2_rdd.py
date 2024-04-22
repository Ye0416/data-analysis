from pyspark import SparkContext, SparkConf
from operator import add
import math
import re
import sys


class proj2:
    def run(self, inputPath, outputPath, stopwords, k):
        def func1(line):
            (year, l1) = line
            temp = []
            for i in l1:
                temp.append((year, i))
            return temp

        conf = SparkConf().setAppName("proj2")
        sc = SparkContext(conf=conf)

        file = sc.textFile(inputPath)
        stop_file = sc.textFile(stopwords)

        # stop_set = set(stop_file.collect())         # 记录stop_word的set
        stop_list = stop_file.collect()

        temp1 = file.map(lambda line: line.split(","))  # 先分date和term两部分
        temp2 = temp1.map(lambda x: (x[0][:4], set(x[1].split())))  # (year,word_list)
        # temp2 = temp1.map(lambda x: (x[date][:4], set([x[terms]])-stop_set))             # 只取年份，set使每行value集合不重复并求差集
        temp3 = temp2.flatMap(func1).filter(lambda x: True if len(x) != 0 else False)  # 验证不为空
        temp4 = temp3.filter(lambda x: True if x[1] not in stop_list else False)  # 剔除stop_word            （year, word）
        # temp4.collect()

        # -------------------------------- number of year in D -----------------------------------------
        num_of_year = temp4.keys().distinct().count()

        # ----------------- wordcount, the number of headlins containing t in y (TF)---------------------
        pairs = temp4.map(lambda word: (word, 1))
        tf1 = pairs.reduceByKey(add)  # ((year,word),count)
        tf2 = tf1.map(lambda x: (x[0][1], (x[0][0], x[1])))  # (word, (year, total_count))
        # tf2.collect()

        # --------------------------------------- idf -----------------------------------------------
        # The number of years in D is already known
        # how to get the number of years having t
        ty = temp4.map(lambda x: (x[1], x[0])).groupByKey().mapValues(lambda x: len(set(x)))  # (word,count)
        # ty.collect()
        idf = ty.map(lambda x: (x[0], math.log10(num_of_year / x[1])))
        # idf.collect()

        # ----------------------------------- weight + sort ----------------------------------------
        # A.join(B): 要求key相同，Values变为(A.value(),B.value())
        weight1 = tf2.join(idf)  # e.g. 'fails', (('2003', 1), 0.47712125471966244
        # weight1.collect()
        # 需要转换为 key: year ; values: 'word,total_count'
        weight2 = weight1.map(lambda x: (x[1][0][0], x[0], x[1][0][1]*x[1][1])).sortBy(lambda x: (x[0], -x[2], x[1]))
        # weight2.collect()          # e.g. ('2003', 'council', 1.4313637641589874),
        weight3 = weight2.map(lambda x: (x[0], x[1]+','+str(round(x[2], 6)))).groupByKey().mapValues(lambda x: list(x))
        # weight3.collect()            # e.g. (year,['word+count1','word+count2'...])
        # 取前k个
        k = int(k)
        weight4 = weight3.map(lambda x: (x[0], ';'.join(x[1][:k]))).sortBy(lambda x: x[0]).map(lambda x: '\t'.join(x))
        # weight4.collect()

        weight4.coalesce(1).saveAsTextFile(outputPath)
        sc.stop()


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Wrong inputs")
        sys.exit(-1)
    proj2().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])

