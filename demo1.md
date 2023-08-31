```python
import re
import functools
import numpy as np
from pyspark import SparkContext
sc = SparkContext.getOrCreate();
rdd = sc.textFile("integers_file_1.txt")

delimiters = '#-.;"!^&%'+"@'wad, "

# Sử dụng re.split() để tách chuỗi thành các phần tử số và các cụm dấu giữa hai số
split_pattern = "[" + re.escape(delimiters) + "]+"
numbers_and_clusters = re.split(split_pattern, ''.join(rdd.collect()))
# b = sc.parallelize(numbers_and_clusters,10).glom().collect()
rdd0 = sc.parallelize(numbers_and_clusters);
for n in rdd0.collect():
    rdd0.collect().remove('')
    
rdd1, rdd2,rdd3,rdd4,rdd5,rdd6,rdd7,rdd8,rdd9,rdd10= rdd0.randomSplit([0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1])
        
arr0 = []
arrA= [] 
arrMax= []
arr0.append([rdd1.collect(),rdd2.collect(),rdd3.collect(),rdd4.collect(),rdd5.collect(),rdd6.collect(),rdd7.collect(),rdd8.collect(),rdd9.collect(),rdd10.collect()])
max=0
for index, a in enumerate(arr0):
    if index == 0:
        arrA= a
for b in (arrA):
    arrMax.append(functools.reduce(lambda x, y: x if x > y else y, b))
print (arrMax)
max = functools.reduce(lambda m, n: m if m > n else n, arrMax)
print (max)

sumAll = functools.reduce(lambda x, y: int(x)+int(y),rdd0.collect())
avgAll = sumAll/len(rdd0.collect())
# print(rdd0.collect())
print(avgAll)

```


```python

```
