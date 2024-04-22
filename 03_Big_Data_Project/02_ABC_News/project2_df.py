from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
import sys
import re
import math
from pyspark.sql.window import Window

class proj2:
    def run(self, inputPath, outputPath, stopwords, k):
        spark = SparkSession.builder.master("local").appName("proj2").getOrCreate()
        file = spark.sparkContext.textFile(inputPath)
        stop_words = spark.sparkContext.textFile(stopwords).collect()
        # stopwords.collect()
        # stopwords

        # fileDF = file.map(lambda line: (line.split(",")[0][:4],' '.join(set(line.split(",")[1].split())))).toDF(['date','wordlist'])
        # fileDF.collect()
        fileDF = file.flatMap(lambda line: [(line.split(",")[0][:4], word) for word in set(line.split(",")[1].split()) if word not in stop_words]).toDF(['date', 'word'])
        # fileDF.collect()

        # ------------------------- number of year ----------------------------
        yearDF = fileDF.select('date').distinct().count()
        # yearDF

        # ----------- wordcount, the number of headlins containing t in y (TF)--------------
        pairDF = fileDF.select('date', 'word', length(fileDF.word.substr(0, 1))).toDF('date1', 'word1', 'count')
        # pairDF.collect()
        countDF = pairDF.groupBy('date1', 'word1').agg(sum('count').alias('totalcount'))
        countDF = countDF.withColumn("word2", col('word1'))
        # countDF.collect()

        # --------------- idf ----------------
        # the number of years having t
        # countDF2 = pairDF.groupby('word1').agg(collect_set('date1').alias('date_list'))
        # countDF2.collect()
        # countDF2 = pairDF.groupBy('word1').agg((yearDF/approx_count_distinct('date1')).alias('ratio'))
        countDF2 = pairDF.groupBy('word1').agg((log(10.0, yearDF / approx_count_distinct('date1'))).alias('ratio'))
        countDF2 = countDF2.withColumn("word3", col('word1'))
        # countDF2.collect()

        # join
        cond = [countDF.word1 == countDF2.word1]
        joinDF = countDF.join(countDF2, on=cond, how='inner')
        # joinDF = joinDF.withColumn("totalcount", joinDF["totalcount"].cast(IntegerType()))
        # joinDF = joinDF.withColumn("ratio", joinDF["ratio"].cast(FloatType()))
        joinDF = joinDF.withColumn("totalcount", col("totalcount").cast("float"))
        joinDF = joinDF.withColumn("ratio", col("ratio").cast("float"))
        joinDF = joinDF.withColumn('weight', joinDF.totalcount * joinDF.ratio)
        joinDF = joinDF.withColumn("weight", col("weight").cast("string"))
        joinDF = joinDF.withColumn("weight", col("weight").cast("float"))
        joinDF = joinDF.withColumn("weight", round("weight", 6))
        # joinDF.collect()
        # joinDF = countDF.join(countDF2,countDF.word1==countDF2.word1,'inner')

        # sort
        finalDF = joinDF.select("date1","word2","weight")
        finalDF = finalDF.withColumn("weight",round("weight", 6))
        finalDF = finalDF.orderBy("date1",-col("weight"),"word2")
        # # finalDF.collect()

        order = Window.partitionBy("date1").orderBy(col("weight").desc(), col("word2").asc())
        final = finalDF.withColumn("ranking", row_number().over(order))
        final = final.filter(col("ranking") <= int(k))
        # final.collect()
        # finalRDD = final.rdd.map(lambda x: x[0] + '\t' + x[1]+','+str(x[2])[: str(x[2]).find('.')+7])
        finalRDD = final.rdd.map(lambda x: (x[0], x[1] + ',' + str(x[2])[: str(x[2]).find('.') + 7])).groupByKey().mapValues(lambda x: list(x)).map(lambda x: x[0] + '\t' + ';'.join(x[1]))
        # finalRDD.collect()

        finalRDD.coalesce(1).saveAsTextFile(outputPath)
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Wrong inputs")
        sys.exit(-1)
    proj2().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
