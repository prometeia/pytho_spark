from pyspark.sql import SparkSession
import urllib.request

spark = SparkSession.builder.getOrCreate()

with urllib.request.urlopen("http://www.gutenberg.org/cache/epub/5200/pg5200.txt") as url:
    s = url.read().decode('utf-8')

text_file = spark.sparkContext.parallelize([s])

counts = text_file.flatMap(lambda line: line.split()) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)

df = counts.toDF(["word", "count"]).orderBy("count", ascending=False)

df.where('count > 7').write.mode('overwrite').csv('hdfs:///tmp/system_test_data/')
