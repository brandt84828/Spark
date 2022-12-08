# Spark
  ## Spark v.s. Hadoop
|          |                  Hadoop                   |                         Spark                          |
| :------: | :---------------------------------------: | :----------------------------------------------------: |
|   類型   |      基礎平台，包含運算、儲存、調度       |                   純計算工具(分布式)                   |
|   場景   |      巨量數據批次處理(磁盤迭代處理)       | 巨量數據批次處理(in memory、交互計算)、巨量數據流計算  |
|   價格   |            對機器要求低，便宜             |                對memory有要求，相對較貴                |
| Paradigm |   map reduce，API較底層，算法適應性較差   |      RDD組成DAG有向無環圖，API階層較高，方便使用       |
| 儲存結構 | MapReduce中間計算結果在HDFS磁盤上，延遲大 |           RDD中間運算結果在Memory中，延遲小            |
| 運作方式 |      Task以進程方式維護，任務啟動慢       | Task以線程方式維護，任務啟動快，可批量創建提高並行能力 |

儘管Spark相對Hadoop具有較大優勢，但並不能完全替代
* 計算方面，Spark相比MR有巨大的性能優勢，但有許多計算工具仍基於MR架構，ex:Hive
* Spark僅作計算，而Hadoop生態圈不僅有計算(MR)也有儲存(HDFS)和資源管理調度(YARN)，HDFS和YARN仍是許多大數據體系的核心架構

- - - 


! Spark有提供兩種對數據流計算的功能，SparkStreaming & 基於SparkSQL提供的StruredStreaming Module，SparkStreaming較早誕生，是以微批的方式處理，使用上有一定缺陷，要處理數據流以SparkSQL提供的為主。

- - - 

## Spark運行模式
* 本地(單機) : 以獨立的進程透過內部的多個線程模擬整個Spark的運行環境
* Standalone(集群) : Spark的各個角色以獨立的形式存在，並組成Spark集群環境
* Hadoop YARN模式(集群) : Spark的各個角色運行在YARN的容器內部，並組成Spark集群環境
* Kubernetes模式(容器集群) : Spark中的各個角色運行在Kubernetes的容器內部，並組成Spark集群環境
* 雲服務模式(運行在雲平台上)


### Local模式
資源管理
* Master: Local進程本身
* Worker: Local進程本身

任務執行
* Driver: Local進程本身
* Executor: 不存在，沒有獨立的Executor，由Local進程(Driver)內的線程提供計算能力

! Driver也算一種Executor，只不過大多時候會將Executor當作純Worker，較容易區分
</br>
! Local模式只能運行一個Spark程序，多個Spark程序則是多個相互獨立的Local進程執行

### Standalone模式
資源管理
* Master: 整個集群的資源管理
* Worker: 所在Server的資源管理

任務執行
* Driver: 運行在Master內，負責任務的調度管理
* Executor: 運行在Worker內，負責任務的執行

! Standardalone是固定的，集群環境內可以開啟多個任務，並非向是Local模式是相互獨立的

#### 配置
至/spark_home/conf先更改worker配置不同server當作worker--->再配置spark-env.sh(log存放位置等)-->log4j(log種類)--->/sbin啟動history server--->/sbin啟動整個集群start-all.sh

預設的port
* 4040: 運行的Application在運行過程中臨時綁定的Port，用以查看當前任務的狀態，當前程序運行完成後會被註銷，多個程序會順延(4041、4042等)
* 8080: Standalone下，Master角色的WEB Port，用以查看當前Master(集群)的狀態
* 18080: 默認是History server的Port，由於每個程序運行完成後，4040 port就被註銷了，之後想回看某個程序的運行狀態就可以通過history server查看

#### Job/Stage/Task的關係
一個Spark程序會被分成多個子任務(Job)，每一個Job會分成多個Stage運行，每一個Stage內會分出來多個Task(線程)執行任務

#### Standalone HA
Spark Standalone集群是Master-Slaves架構的集群模式，和大多數的Master-Slaves結構一樣，存在著Master單點故障(SPOF)的問題。

HA兩種方案
* 基於文件系統的單點恢復--用於開發或測試環境
* 基於ZooKeeper的Standby Masters --可用於生產環境

HA配置(ZooKeeper)
</br>
1. 確定ZooKeeper已經啟動，至/spark/conf中註解掉spark-env.sh export SPARK_MASTER_HOST，因為採用ZooKeeper後就沒有固定Maseter。
2. 增加SPARK_DAEMON_JAVA_OPTS至spark-env.sh中，告訴Spark recoveryMode=ZooKeeper/Zookeeper的url是什麼/臨時節點路徑。
![StandbyMastersWithZooKeeper](/Picture/StandbyMastersWithZooKeeper.PNG)
3. 將文件複製到每台Server上
4. 啟動集群spark/sbin/start-all.sh(啟動Master & Worker)

! Spark Standardalone的切換不影響正在運行的程序。

### Spark On YARN模式
在Server資源較緊迫的情況下，企業通常都會有Hadoop集群，對於在已有YARN集群的前提下在去額外使用Spark Standardalone集群的話，對資源的利用率就不高，所以多數場景下會將Spark運行在YARN集群上。
Spark on YARN無需部屬Spark集群，只需要一台Server當作Spark的Client端，即可提交任務到YARN集群中運行。

資源管理
* Master: YARN的ResourceManager擔任
* Worker: YARN的NodeManager擔任

任務執行
* Driver: 運行在YARN容器內(Cluster模式)或提交任務的Client端(Client模式)
* Executor: 運行在YARN提供的容器內

! Standardalone是固定的，集群環境內可以開啟多個任務，並非向是Local模式是相互獨立的

#### 配置
1. 確保HADOOP_CONF_DIR和YARN_CONF_DIR內配置文件完成
2. 在spark-env.sh以及環境變量配置加入配置文件的目錄

#### Spark On YARN部屬模式
兩種模式的區別在於Driver運行的位置
1. Cluster模式: Driver運行在YARN容器內部，和ApplicationMaster在同一個容器內
2. Client模式: Driver運行在Client端進程中，比如Driver運行在spark-submit程序的進程中

|                    |         Cluster模式          |            Client模式            |
| :----------------: | :--------------------------: | :------------------------------: |
|   Driver運行位置   |          YARN容器內          |          Client端進程內          |
|      通訊效率      |              高              |         低於Cluster模式          |
|      日誌查看      | 日誌輸出在容器內，查看不方便 | 日誌輸出在客戶端stdout，方便查看 |
| Production env使用 |             推薦             |              不推薦              |
|       穩定性       |             穩定             |       受到Client端進程影響       |


## 開發Spark應用程式
Spark Application程式入口為SparkContext，任何一個應用都需要構建SparkContext對象
* 創建SparkConf對象
  * 設置Spark Application基本資訊，例如AppName、運行的Master等 
* 基於SparkConf對象，創建SparkContext對象
```python
conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)

#同步上傳有用到的依賴文件
conf.set("spark.submit.pyFiles","xx.py")
```
! spark-submit和Code裡面針對Master的設置要注意，以Code為主，Code有的話spark-submit工具的設置就無效了

! 如果多個py files要一起submit的話，參數要用spark.submit.pyFiles把依賴文件也同步上傳上去，單個依賴可以直接帶，多個依賴可以傳.zip壓縮檔。Code可以用conf.set，透過spark-submit工具則帶--py-files參數

! 注意讀取文件的路徑一定要是各個機器都能訪問到的地址，因為是分布式計算

! PySpark運作: Python -> JVM Code -> JVM Driver -> RPC -> 調度JVM Executor -> PySpark中轉 -> Python Executor進程
![PySpark](/Picture/PySpark.jpg)
Driver端由JVM執行，Executor端由JVM做命令轉發，底層由Python解釋器執行工作

分布式Coding的重要特徵
* 在集群上運行是被分布是運行的
* 在Spark中，分任務處理部分由Driver執行(非RDD)
* 任務處理部分由Executor執行(RDD)
* Executor的數量可以很多，所以任務的計算是分布式在運行的
  
### PySpark
* /bin/pyspark: 應用程式，提供一個Python編譯器執行環境來運行Spark任務
* PySpark: Python的類別庫，內含完全的Spark API，可以透過PySpark類別庫編寫Spark應用程式，並將其提交至Spark集群中運行

|        功能        |        PySpark         |           Spark           |
| :----------------: | :--------------------: | :-----------------------: |
|      底層語言      |         Python         |        Scala(JVM)         |
|    上層語言支持    |         Python         |    Python\Java\Scala\R    |
| 集群化\分布式運行  |   不支援，僅支援單機   |           支援            |
|        定位        | Python類別庫(Client端) | 標準框架(Client & Server) |
| 是否可以Daemon運行 |           No           |            Yes            |
|      使用場景      | 本地開發測試Python程式 |    生產環境集群化運行     |

# Spark Core

## RDD
分布式計算需要:
* 分區控制
* Shuffle控制
* 數據儲存、序列化、傳送
* 數據計算API
* 其他
  
### Why RDD
這些功能不能簡單地透過Python內置的本地集合對象(List、dictionary等)去完成。需要有一個統一個數據抽象對象實現分布式計算所需功能。這個抽象對象就是RDD。

### What is RDD(Resilient Distributed Dataset)
彈性分布式數據集: 在記憶體中可以實現集群化計算的高容錯抽象對象，是Spark中最基本的數據抽象，代表一個不可變、可分區、裡面的元素可平行計算的集合。
* Dataset: 一個數據集合，用於存放數據的。
* Distributed: RDD中的數據是分布式儲存的，可用於分布式計算。
* Resilient: RDD中的數據可以儲存在記憶體中或磁盤中。

### RDD的五大特性

#### 特性1: A list of partitions. RDD是有分區的
RDD的分區是RDD數據儲存的最小單位
```python
>>> rdd = sc.parallelize([1,2,3,4,5,6,7,8,9], 3)
>>> rdd.glom().collect()
#可以看到結果被分成3個分區
[[1, 2, 3], [4, 5, 6], [7, 8, 9]]
```
#### 特性2: A function for computing each split. RDD方法會作用到所有分區上
```python
>>> rdd = sc.parallelize([1,2,3,4,5,6,7,8,9], 3).glom().collect()
>>> sc.parallelize([1,2,3,4,5,6,7,8,9], 3).glom().collect()
[[1, 2, 3], [4, 5, 6], [7, 8, 9]]
>>> sc.parallelize([1,2,3,4,5,6,7,8,9], 3).map(lambda x: x*10).glom().collect()
[[10, 20, 30], [40, 50, 60], [70, 80, 90]]
```

#### 特性3: A list of dependencies on other RDDs. RDD之間是有相互依賴關係的
```python
rdd1 = sc.textFile("../t.txt")
rdd2 = rdd1.flatMap(lambda x:x.split(' '))
rdd3 = rdd2.map(lambda x: (x, 1))
rdd4 = rdd3.reduceByKey(lambda a, b: a + b)
```
ex: rdd2產生rdd3，但是rdd2依賴rdd1
textFile -> rdd1 -> rdd2 -> rdd3 -> rdd4

#### 特性4: Optionally, a Partitioner for key-value RDDs. Key-Value型的RDD可以有分區器
* 默認分區器: Hash分區規則，可以手動設置(rdd.partitionBy的方法設置)
* KV型RDD: RDD內儲存的數據是二元元組 ex:("hadoop",3)
* 特性是可選的，並非所有RDD都是KV型
* 二元元組: 只有2個元素的元組

#### 特性5: Optionally, a list of preferred locations to compute each split on. RDD分區規劃會盡量靠近數據所在的Server

在初始RDD(讀取數據的時候)規劃的時候，分區會盡量規劃到儲存數據所在的Server上，因為這樣可以走本地讀取，避免網路讀取
* 本地讀取: Executor所在的Server，同樣是一個DataNode，同時這個DataNode上有它要讀取的數據，所以可以直接讀取硬碟無需走網路傳輸
* 網路讀取: 讀取數據需要經過網路的傳輸才能讀取到
* 特性是可選的，Spark會在確保平行計算能力的前提下，盡量確保本地讀取，但不是100%確保

### RDD的創建
* 透過平行化集合創建(本地對象轉分布式RDD)
```python
data = [1,2,3,4,5,6,7,8,9]
rdd = sc.parallelize(data, numSlices=3)

rdd = sparkContext.parallelize(para1, para2)
# para1 集合對象，list、array等
# para2 分區數
```
* 讀取外部數據
```python
rdd = sc.textFile("../word.txt", 1000)

sparkcontext.textFile(para1, para2)
# para1 必填，文件路徑，支援本地文件、HDFS、S3等
# para2 可選，表示最小分區數量
# 注意: para2話語權不足，spark有自己的判斷，在它允許的範圍內para2有效果，超過則失效

#讀取文件的API，適合讀取一堆小文件，此API偏向於少量分區讀取數據，因為文件數據小分區多會導致shuffle的機率更高，所以盡量少分區讀取數據
sparkcontext.wholeTextFile(para1, para2)
```

### RDD Operator
* 算子(Operator): 分布式集合對象上的API
* 方法/函數: 本地對象的API

RDD Operator分成兩類
+ Transformation: 轉換算子
  + 定義: RDD的算子，返回值能仍是RDD則稱為轉換算子
  + 特性: 這類算子是lazy loading的，如果沒有action算子，Transformation算子是不工作的
+ Action: 動作算子
  + 定義: 返回值不是RDD的就是action算子 

### 常用的Transformation Operator
* map算子

將RDD的數據一條一條處理(處理的邏輯基於map算子中接收的處理函數)，返回新的RDD
```Python
rdd = sc.parallelize([1,2,3,4,5,6], 3)

#定義方法傳入map算子
def add(data):
    return data * 10

print(rdd.map(add).collect())

#定義lambda表達式來寫匿名函數
print(rdd.map(lambda data: data*10).collect())
```

* flatMap算子
  
對RDD先執行map操作，然後進行解除嵌套操作
```python
# 嵌套的list
list =[[1,2,3], [4,5,6], [7,8,9]]
# 解除嵌套
list = [1,2,3,4,5,6,7,8,9]

#Spark操作
rdd = sc.parallelize(["hadoop spark hadoop","spark hadoop hadoop","hadoop flink spark"])
rdd2 = rdd.map(lambda line: line.split(" "))
print(rdd2.collect())
>>>[['hadoop', 'spark', 'hadoop'], ['spark', 'hadoop', 'hadoop'], ['hadoop', 'flink', 'spark']]

#改成flatmap
rdd2 = rdd.flatMap(lambda line: line.split(" "))
print(rdd2.collect())
>>>['hadoop', 'spark', 'hadoop', 'spark', 'hadoop', 'hadoop', 'hadoop', 'flink', 'spark']
```

* reduceByKey算子

針對KV型RDD，自動按照key分組，根據提供的聚合邏輯完成組內數據(Value)的聚合操作
```python
rdd = sc.parallelize([('a',1),('a',1),('b',1),('b',1),('a',1)])

#reduceByKey將相同key的數據聚合相加
print(rdd.reduceByKey(lambda a, b: a + b).collect())
>>>[('a', 3), ('b', 2)]
```

* mapValues算子

針對二元元組RDD，對其內部的二元元組的Value執行map操作
```python
rdd = sc.parallelize([('a',1),('a',11),('a',6),('b',3),('b',5)])
# 透過map
print(rdd.map(lambda x:(x[0], x[1] * 10)).collect())

# 透過mapValues
print(rdd.mapValues(lambda x: x * 10).collect())
```

* groupBy算子

將RDD數據進行分組
```python
rdd = sc.parallelize([('a',1),('a',1),('b',1),('b',1),('b',1)])

# 按照誰分組參數就放誰
result = rdd.groupBy(lambda t: t[0])
print(result.collect())
print(result.map(lambda t:(t[0],list(t[1]))).collect())

>>>[('a', <pyspark.resultiterable.ResultIterable object at 0x7f34500c5580>), ('b', <pyspark.resultiterable.ResultIterable object at 0x7f3450052c70>)]
>>>[('a', [('a', 1), ('a', 1)]), ('b', [('b', 1), ('b', 1), ('b', 1)])]
```

* filter算子
  
過濾想要的數據進行保留
```python
rdd = sc.parallelize([1,2,3,4,5,6])

#參數要放boolean
result = rdd.filter(lambda x: x % 2 == 1)
print(result.collect())
>>>[1, 3, 5]
```

* distinct算子

對RDD數據進行去重複後回傳新的RDD
```python
# para1 去重複分區數量，通常不用寫

rdd = sc.parallelize([1,1,1,2,2,2,3,3,3])
print(rdd.distinct().collect())
>>>[1, 2, 3]

rdd2 = sc.parallelize([('a',1),('a',1),('a',3)])
print(rdd2.distinct().collect())
>>>[('a', 1), ('a', 3)]
```

* union算子

將2個RDD合併成1個RDD後回傳
```python
rdd1 = sc.parallelize([1,1,3,3])
rdd2 = sc.parallelize(["a","b","c"])
rdd3 = rdd1.union(rdd2)
print(rdd3.collect())
>>>[1, 1, 3, 3, 'a', 'b', 'c']
# union算子不會去重複
# RDD類型不同還是可以合併
```

* join算子

對2個RDD執行JOIN操作(可實現SQL inner/outer)
* rdd.join(other_rdd) # inner join
* rdd.leftOuterJoin(other_rdd) # left outer
* rdd.rightOuterJoin(other_rdd) # right outer
```python
# 按照key進行關聯
rdd1 = sc.parallelize([(1001,"User1"),(1002,"User2"),(1003,"User3"),(1004,"User4")])
rdd2 = sc.parallelize([(1001,"RD"),(1002,"BD")])
print(rdd1.join(rdd2).collect())
print(rdd1.leftOuterJoin(rdd2).collect())
print(rdd1.rightOuterJoin(rdd2).collect())

>>>[(1002, ('User2', 'BD')), (1001, ('User1', 'RD'))]
>>>[(1002, ('User2', 'BD')), (1004, ('User4', None)), (1001, ('User1', 'RD')), (1003, ('User3', None))]
>>>[(1002, ('User2', 'BD')), (1001, ('User1', 'RD'))]
```

* intersection算子

求2個RDD的交集，回傳一個新的RDD
```python
rdd1 = sc.parallelize([('a',1),('a',3)])
rdd2 = sc.parallelize([('a',1),('b',3)])
rdd3 = rdd1.intersection(rdd2)
print(rdd3.collect())
>>>[('a', 1)]
```

* glom算子

將RDD數據加上嵌套，嵌套按照分區來進行。比如說RDD數據[1,2,3,4,5]有2個分區，那麼glom後數據會變成[[1,2,3],[4,5]]
```python
#可明顯看到分區的情況
rdd = sc.parallelize([1,2,3,4,5,6,7,8,9],3)
print(rdd.glom().collect())
>>>[[1, 2, 3], [4, 5, 6], [7, 8, 9]]

#解嵌套
print(rdd.glom().flatMap(lambda x:x).collect())
>>>[1, 2, 3, 4, 5, 6, 7, 8, 9]
```

* groupByKey算子

針對KV型RDD自動按照Key分組
```python
rdd = sc.parallelize([('a',1),('a',1),('b',1),('b',1),('b',1)])
rdd2 = rdd.groupByKey()
print(rdd2.map(lambda x: (x[0], list(x[1]))).collect())
>>>[('a', [1, 1]), ('b', [1, 1, 1])]

#要注意原本groupBy會保留key值每筆value，但groupByKey則不會，因groupBy對象不限於KV但groupByKey對象限於KV且按照key分組
```

* sortBy

對RDD數據進行排序，基於指定的排序依據
```python
# func: 告知按照rdd中哪個數據進行排序
# ascending True>升序 False>降序
# numPartitions: 用多少分區排序

#如果要全局有序，排序分區數要設置為1
rdd = sc.parallelize([('c',3),('f',1),('b',11),('c',3),('a',1),('e',5),('a',1),('e',9),('a',1)],3)
print(rdd.sortBy(lambda x:x[1],ascending=True,numPartitions=1).collect())
print(rdd.sortBy(lambda x:x[0],ascending=False,numPartitions=1).collect())

>>>[('f', 1), ('a', 1), ('a', 1), ('a', 1), ('c', 3), ('c', 3), ('e', 5), ('e', 9), ('b', 11)]
>>>[('f', 1), ('e', 5), ('e', 9), ('c', 3), ('c', 3), ('b', 11), ('a', 1), ('a', 1), ('a', 1)]
```

* sortByKey算子

針對KV型RDD按照Key進行排序
```python
# ascending True>升序 False>降序 默認為升序
# numPartitions: 用多少分區排序，如果全局有序設置1
# keyfunc: 在排序前對key進行處理

rdd = sc.parallelize([('c',3),('f',1),('b',11),('c',3),('C',1),('e',5),('a',1),('A',9),('a',1)],3)
print(rdd.sortByKey(ascending=True,numPartitions=1,keyfunc=lambda key: str(key).lower()).collect())
>>>[('a', 1), ('A', 9), ('a', 1), ('b', 11), ('c', 3), ('c', 3), ('C', 1), ('e', 5), ('f', 1)]

#注意: 排序前的keyfunc都轉小寫按小寫排序，但實際上不會改動數據
```

* mapPartitions算子

mapPartition一次傳遞的是一整個分區的數據，作為一個迭代器(一次性list)傳遞
```python
# 相較於map則是一個一個傳送，雖然結果相同但網路傳輸的I/O次數差異很大
rdd = sc.parallelize([1,3,2,4,7,9,6],3)
def process(iter):
    result = list()
    for it in iter:
        result.append(it * 10)
    return result
print(rdd.mapPartitions(process).collect())
>>>[10, 30, 20, 40, 70, 90, 60]
```
* partitionBy

對RDD進行自定義分區操作
```python
#para1: 重新分區後有幾個分區
#para2: 自定義分區規則
rdd = sc.parallelize([('hadoop',1),('spark',1),('hello',1),('flink',1),('hadoop',1),('spark',1)])
def process(k):
    if 'hadoop' == k or 'hello' == k : return 0
    if 'spark' == k: return 1
    return 2
print(rdd.partitionBy(3, process).glom().collect())

>>>[[('hadoop', 1), ('hello', 1), ('hadoop', 1)], [('spark', 1), ('spark', 1)], [('flink', 1)]]
```

* repartition算子

對RDD的分區執行重新分區(僅數量)
```python
#para: 新的分區數量
rdd = sc.parallelize([1,2,3,4,5],3)
print(rdd.repartition(1).getNumPartitions())
>>>1
#注意:對分區的數量操作要慎重
#會影響平行計算(記憶體中迭代的平行管道數量)
#分區如果增加，極大可能導致shuffle
```

* coalesce算子
對RDD重新分區，同repartition(repartition原始碼也是調用ccoalesce只是shuffle=True)
```python
# para1: 新的分區數量
# para2: shuffle=True or False(安全機制)，要是True才能增加分區
rdd = sc.parallelize([1,2,3,4,5],3)
print(rdd.coalesce(1).getNumPartitions())
>>>1
```
### 常用的Action Operator
* countByKey算子

統計key出現的次數(一般適用於KV型RDD)
```python
rdd = sc.textFile("/home/cris/spark/python/lib/PY4J_LICENSE.txt")
rdd2 = rdd.flatMap(lambda x: x.split(" ")).map(lambda x: (x,1))
rdd2.countByKey()
# output是dict不是rdd，要注意是Action算子，回傳值不再是RDD
```

* collect算子
  
將RDD各個分區內的數據統一蒐集到Driver中，形成一個List對象。使用之前要考慮資料量會不會太大造成Driver memory不夠

* reduce算子

對RDD數據集按照傳入的邏輯進行聚合，回傳值與參數要求一致
```python
rdd = sc.parallelize([1,2,3,4,5])
print(rdd.reduce(lambda a, b: a + b))
>>>15
```

* fold算子

和reduce一樣，接收傳入邏輯進行聚合，聚合是帶有初始值的。這個初始值會作用在"分區內聚合"及"分區間聚合"
```python
ex: [[1,2,3],[4,5,6],[7,8,9]]，default=10，logic:a+b
分區123聚合=1+2+3+10=16
分區456聚合=4+5+6+10=25
分區789聚合=7+8+9+10=34
Total聚合=16+25+34+10=85

# code
rdd = sc.parallelize([1,2,3,4,5,6,7,8,9],3)
print(rdd.fold(10,lambda a, b: a + b))
>>>85
```

* first算子

取出RDD的第一個元素
```python
sc.parallelize([1,2,3]).first()
>>>1
```

* take算子

取RDD的前N個元素，組合成list回傳
```python
sc.parallelize([1,2,3,4,5,6,7,8,9]).take(5)
>>>[1,2,3,4,5]
```

* top算子

對RDD數據集進行降序排序，取前N個
```python
sc.parallelize([3,2,3,5,7,9,1,3]).top(3)
>>>[9,7,5]
```

* count算子

計算RDD有多少個數據，回傳int
```python
sc.parallelize([1,1,1,1,1,1]).count()
>>>6
```

* takeSample算子

隨機抽樣RDD的數據
```python
#para1: True表示取同一個數據，False表示不允許同一個數據(位置的重複)
#para2: 抽樣的數量
#para3: 隨機數種子，預設Spark會自動給

rdd = sc.parallelize([1,3,5,3,1,3,2,6,7,8,6],1)
print(rdd.takeSample(False, 5, 1))
>>>[2, 7, 6, 6, 3]
```

* takeOrdered算子

對RDD進行排序取前N個
```python
#para1: 要幾個數據
#para2: 對排序的數據進行更改(不會更改數據本身)，只應用在排序

rdd = sc.parallelize([1,3,2,4,7,9,6],1)
print(rdd.takeOrdered(3))
print(rdd.takeOrdered(3, lambda x: -x))
>>>[1, 2, 3]
>>>[9, 7, 6]
```

* foreach算子

對RDD每一個元素執行提供的邏輯操作(和map相同)，但此方法沒有回傳值，經由executor直接輸出(沒有傳到Driver)，假設executor1有1和3，在executor1就會輸出10和30，executor2有2和4則輸出20和40，不會統一蒐集到Driver輸出10 20 30 40
```python
rdd = sc.parallelize([1,3,2,4,7,9,6],1)
result = rdd.foreach(lambda x: print(x * 10))
print(result)
>>>10 30 20 40 70 90 60
>>>None
```

* saveAsTextFile算子

將RDD數據寫入到文本文件中，與foreach相同，由executor直接執行輸出(沒有傳到Driver)
```python
rdd = sc.parallelize([1,3,2,4,7,9,6],1)
rdd.saveAsTextFile("out1")

#產生文件數量與分區有關
```

* foreachPartition算子

和普通foreach一致，但一次處理的是一整個分區數據
```python

rdd = sc.parallelize([1,3,2,4,7,9,6],3)
def process(iter):
    result = list()
    for it in iter:
        result.append(it * 10)
    print(result)
rdd.foreachPartition(process)

#output
[10, 30]
[20, 40]
[70, 90, 60]
```

#### groupByKey V.S. reduceByKey
功能上的區別:
* groupByKey只有分組功能
* reduceByKey除了有ByKey的分組功能外，還有reduce聚合功能

如果對數據執行分組+聚合，使用這2個算子的性能差別是很大的，shuffle過程中網路I/O的花費差異很大。reduceByKey的性能是遠大於groupByKey+聚合邏輯的，因為groupByKey只能分組，所以執行上是先分組(shuffle)後聚合。

相較之下，使用reduceByKey則是在分組前先預聚合再分組(shuffle)，最後再最終聚合，被shuffle的數據可以極大的減少。


### RDD的數據是過程數據
RDD之間進行相互迭代計算(Transformation的轉換)，當執行後，新的RDD產生代表舊的RDD消失。RDD的數據是過程數據，只有在處理的過程中存在，一但處理完成後就不見了。

!這個特性可以最大化的利用資源，舊的RDD從memory中清理掉給後續的計算挪出空間。

### RDD的快取機制
假設有兩條處理過程
* RDD1->RDD2->RDD3->RDD4->collect
* RDD3->RDD5->RDD6->collect

按照前面所說RDD是過程數據，在上面這種情況下RDD3會被重新建立起來兩次，所以Spark提供緩存的機制防止需要從頭到尾重新計算以增進效能。可以將指定的RDD數據保留在memory or disk上。
```python
rdd3.cache() # 緩存到memory中
rdd3.persist(StorageLevel.MEMORY_ONLY) # 僅memory緩存，與cache相同，cache API實際上也是調用這個方法
rdd3.persist(StorageLevel.MEMORY_ONLY_2)# memory緩存，2個副本
rdd3.persist(StorageLevel.DISK_ONLY)# Disk緩存
rdd3.persist(StorageLevel.DISK_ONLY)# Disk緩存，2個副本
rdd3.persist(StorageLevel.MEMORY_AND_DISK)# 先放memory不夠放disk
rdd3.persist(StorageLevel.OFF_HEAP)# heap memory(system memory)
rdd3.unpersist() # 主動清理緩存的API
```
緩存機制是不安全的，雖然有副本。設計上認為是不安全的，涉及到memory，緩存如果丟失的話就要重新計算重新緩存，所以緩存必須要保留"被緩存RDD的前置血緣關係"，另外一個重點是緩存是"分散儲存"的，每個分區自行將數據儲存在所在Executor的memory和disk上

### RDD的CheckPoint  
將RDD的數據保存起來
* 僅支持儲存在Disk上
* 被設計認為是安全的
* 不保留血緣關係
* CheckPoint儲存RDD數據是集中蒐集各個分區數據進行儲存，而cache則是分散儲存  

Cache與CheckPoint的對比
* CheckPoint不管分區數量多少，風險是一樣的，Cache分區越多風險越高
* CheckPoint支持寫入HDFS，Cache不行，HDFS是高可靠儲存，所以CheckPoint被認為是安全的
* CheckPoint不支持memory(集中存儲涉及到網路I/O)，Cache可以，Cache如果寫在memory中性能比CheckPoint好一些
* ChcekPoint因為設計認為是安全的，所以不保留血緣關係，而Cache因為設計上認為不安全，所以要保留
```python
sc.setCheckpointDir("hdfs://node1:8820/output/backup")
#用的時候直接調用checkpoint算子即可
rdd.checkpoint()
```
 
### Spark Broadcast

Spark中因為算子中的真正邏輯是發送到Executor中去運行的，所以當Executor中需要引用外部變量時，需要使用廣播變量。解決分布式集合RDD和本地集合一起使用的時候，降低記憶體占用以及減少網路IO傳輸，提高性能。

進一步解釋：
* 如果executor端用到了Driver的變量，如果不使用廣播變量在Executor有多少task(分區)就有多少Driver端的變量副本。
* 如果Executor端用到了Driver的變量，如果使用廣播變量在每個Executor中只有一份Driver端的變量副本。
```python


stu_info_list=[(1,"Amy",11),(2,"Bob",13),(3,"Chris",11),(4,"David",11)]
score_info_rdd = sc.parallelize([(1,"國語",90),(2,"數學",91),(3,"英語",92),(2,"英語",96),(2,"國語",91),(1,"數學",94),(3,"數學",95),(1,"英語",91),(3,"國語",91)])

# 未使用廣播變量

def map_func(data):
    id = data[0]
    name = ""
    for stu_info in stu_info_list:
        stu_id = stu_info[0]
        if id == stu_id:
            name = stu_info[1]
    return (name,data[1],data[2])


print(score_info_rdd.map(map_func).collect())

# 使用廣播變量

broadcast = sc.broadcast(stu_info_list)

def map_func(data):
    id = data[0]
    name = ""
    for stu_info in broadcast.value:
        stu_id = stu_info[0]
        if id == stu_id:
            name = stu_info[1]
    return (name,data[1],data[2])

print(score_info_rdd.map(map_func).collect())

# 如果將stu_info_list轉成RDD，就要使用Join算子-->會產生shuffle
# 但如果容量太大可能還是採用轉RDD使用JOIN比較好，不然會在每個Executor上都吃掉大量的memory
```

### 累加器
分布式程式執行時進行全域的累加
```python
#未用累加器(非RDD由Driver執行 count=0)，沒累加變10，分區各自計算
rdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10], 2)

count = 0

def map_func(data):
    global count
    count += 1
    print(count)
    
rdd.map(map_func).collect()
print(count)
>>> 1 1 2 2 3 3 4 4 5 5 
>>> 0 # 因為在driver上 count還是=0

#使用Spark提供的累加器
rdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10], 2)

count = sc.accumulator(0)

def map_func(data):
    global count
    count += 1
    print(count)
    
rdd.map(map_func).collect()
print(count)
>>> 1 2 3 4 5 1 2 3 4 5
>>> 10 #使用sc.accumulator後可成功累加，使用上要注意RDD是過程數據(如果沒有Cache or CheckPoint的話，重新調用後會重新根據鍊條執行持續累加)
```

### DAG
Spark的核心是根據RDD實現的，Spark Scheduler則為Spark核心實現的重要一環，其作用就是任務調度。Spark的任務調度就是如何組織任務去處理RDD中每個分區的數據，根據RDD的依賴關係構建DAG，基於DAG劃分Stage，將每個Stage中的任務分配到指定節點運行。基於Spark的任務調度原理，可以合理規劃資源利用，做到盡可能用最少的資源高效完成任務計算。

DAG(有向無環圖)，有方向沒有形成閉環的一個執行流程圖。
* 有向: 有方向
* 無環: 沒有閉環
![DAG](/Picture/DAG.png)

Job & Action

Action:回傳值不是RDD的算子，它是一個觸發開關，會將action算子之前的一串RDD都串聯起來。

! 一個action會產生一個Job(一個應用程序內的子任務)及一個DAG圖

! 層級關係:一個Application中可以有多個JOB，每一個JOB內含一個DAG，同時每一個JOB都是由一個Action產生的

### DAG的寬窄依賴和階段劃分
#### 寬窄依賴
在SparkRDD前後之間的關係分為:
* 窄依賴: 父RDD的一個分區，全部將數據傳給子RDD的一個分區
* 寬依賴: 父RDD的一個分區，將數據發給子RDD的多個分區，又稱shuffle

#### 階段劃分
對於Spark來說會根據DAG，按照**寬依賴**劃分不同的DAG階段

劃分依據: 從後向前，遇到寬依賴就劃分出一個階段稱為Stage，所以在Stage內部一定是**窄依賴**

### 記憶體迭代計算
基於帶有分區的DAG以及階段劃分，可以得到邏輯上最優的任務分配。假設分區有3分成3個task(皆從RDD1>RDD2>RDD3)，一個task是一個線程執行(純記憶體計算)，三個task就形成三個平行的記憶體計算管道(pipeline)。

### Spark為什麼比MapReduce快
1. 模型上Spark較有優勢(算子多): Spark算子豐富，MapReduce算子較少(Map & Reduce)，所以很難在一套MR中處理負責的任務，很多的複雜任務需要多個MapReduce串聯，而多個MR串連又會通過Disk交換數據。
2. 較多的記憶體計算: Spark可以執行記憶體迭代，算子之間形成DAG，基於依賴劃分階段後，在階段內形成記憶體迭代管道，而MapReduce之間的交互則是透過Disk。

### Spark平行度
在同一時間內，有多少個task同時運行。平行度指的就是平行能力的設置，例如設置平行度為6就是有6個task平行在執行。在有6個task平行的前提下，rdd的分區就被規劃成6個分區。

設置的順序:
1. 程式語言中
```python
conf = SparkConf()
conf.set("spark.default.parallelism","100")
```
2. client提交的參數中
```bash
bin/spark-submit --conf "spark.default.parallelism=100"
```
3. 配置文件中
```python
conf/spark-defaults.conf中設置
spark.default.parallelism 100
```
4. Default(1，但不會全部都以1執行，多數的時候基於讀取文件的分區數量)

#### 集群中平行度規劃
設置為CPU總核心的2~10倍，確保是CPU核心的整數倍即可，最小是2倍，最大一般10倍或更高(適量)都可以。

Why: CPU的一個核心同一時間只能幹一件事情，所以在100個核心的情況下設置100個平行度就能讓CPU 100%出力。這種設置下，如果Task的壓力不平均，某個Task先執行完就會導致某個CPU核心idle。所以將Task(平行度)分配的數量變多，例如800，同一時間只有100個在運行，700個在等待，但是可以確保某個task執行完後續有task補上，不讓CPU idle，最大化利用集群的資源。但如果設置太過量會對於spark調度管理不易。

### Spark任務調度
1. 構建Driver
2. 構建SparkContext(執行環境入口對象)
3. 基於DAG Scheduler(DAG調度器)構建邏輯Task分配
4. 基於Task Scheduler(Task調度器)將邏輯Task分配到各個Executor上執行並監控
5. Worker(Executor)被Task Scheduler管理監控，聽從指令並定期報告進度

! 1~4都屬於Driver的工作 5是Worker的工作

#### Driver內的2個元件
1. DAG Scheduler: 將邏輯的DAG圖進行處理得到邏輯上的Task劃分
2. Task Scheduler: 基於DAG Scheduler的產出規劃邏輯的Task應該在哪些物理的executor運行以及監控它們的運行

! 有幾個服務器就幾個Executor，同個Executor內走記憶體，但若單台服務器有多個Executor則Task之間走的是網路通訊(本地)，雖然遠比TCP快但還是沒有記憶體來得更好

# SparkSQL

## SparkSQL框架的基礎概念
SparkSQL是Spark的一個模組，用於處理海量**結構化**數據(只適用於結構化數據)

SparkSQL優勢:
1. 支持SQL語言、性能強、可以自動優化、API簡單、兼容Hive等等
2. 企業廣泛使用(離線開發、數據倉庫創建、科學計算、數據分析)

SparkSQL特點:
1. 整合性: SQL可以無縫整合在code中，隨時用SQL處理數據
2. 統一數據存取: 透過相同的標準API可以讀取不同資料來源
3. HIVE兼容: 可以使用SparkSQL直接計算並生成Hive數據表
4. 標準化連接: 支持標準化JDBC/ODBC連接，方便和各種資料庫進行互動

### SparkSQL & Hive差異
1. 都是分布式SQL計算引擎，但SparkSQL性能更好
2. SparkSQL是記憶體計算 / Hive是Disk迭代
3. SQL & code混和執行 / 僅能以SQL開發
4. 底層運行SparkRDD / 底層運行MapReduce
5. 不支持元數據管理(但可與Hive整合) / 有元數據管理(MetaStore)
6. 都可以運行在YARN之上

### SparkSQL數據抽象
* Pandas - DataFrame: 二維表數據結構，單機(本地)集合
* SparkCore - RDD: 無標準數據結構，儲存什麼數據都可以，分布式集合(分區)
* SparkSQL - DataFrame: 二維表數據結構，分布式集合(分區)
* SparkSQL For JVM - Dataset:
  * SchemaRDD對象: 已廢棄
  * DataSet對象: 支持泛型，可用於Java、Scala
  * DataFrame對象: 可用於Java、Scala、Python、R
* SparkSQL For Python/R - DataFrame: 以DataFrame作為核心數據結構

### DataFrame 與 RDD 差異
* DataFrame儲存二維表結構數據，RDD儲存任意結構數據
* 皆是有分區的
* 皆是分布式
* 皆彈性

假設有以下數據集
|  id   | name  |  age  |
| :---: | :---: | :---: |
|   1   |  Amy  |  11   |
|   2   |  Bob  |  12   |
|   3   | Chris |  13   |
|   4   | David |  14   |
|   5   |  Eva  |  15   |

DataFrame會按照二維表格儲存(必定)

分區1(Executor1):
|       |       |       |
| :---: | :---: | :---: |
|   1   |  Amy  |  11   |
|   2   |  Bob  |  12   |

分區2(Executor2):
|       |       |       |
| :---: | :---: | :---: |
|   3   | Chris |  13   |
|   4   | David |  14   |
|   5   |  Eva  |  15   |

RDD則會按照數組儲存(可改，不一定)
```python
分區1(Executor1):
[1,Amy,11]
[2,Bob,12]

分區2(Executor2):
[3,Chris,13]
[4,David,14]
[5,Eva,15]
```

### SparkSession
Spark2.0後推出，作為Spark程式中統一的入口對象。

SparkSession對象可以:
* 用於SparkSQL作為入口對象
* 用於SparkCore，可以通過SparkSession對象中獲取到SparkContext
```python
from pyspark.sql import SparkSession

#create SparkSession
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

#get SparkContext by SparkSession
sc = spark.sparkContext

#Hello World
df = spark.read.csv("logstash-2022.04.27.csv",sep=',',header=False)
df2 = df.toDF("time","content")
df2.printSchema() # print table structure
df2.show()

#output
root
 |-- time: string (nullable = true)
 |-- content: string (nullable = true)

+--------------------+--------------------+
|                time|             content|
+--------------------+--------------------+
|2022-04-27T06:59:...|"pattern not matc...|
|2022-04-27T06:59:...|"pattern not matc...|
...

#SQL
df2.createTempView("log")
spark.sql("""SELECT * FROM log LIMIT 3 """).show()

#SQL output
+--------------------+--------------------+
|                time|             content|
+--------------------+--------------------+
|2022-04-27T06:59:...|"pattern not matc...|
|2022-04-27T06:59:...|"pattern not matc...|
|2022-04-27T06:59:...|"pattern not matc...|
+--------------------+--------------------+

#DSL
df2.where("time='2022-04-27T06:59:42.575644409+00:00'").show()

#DSL output
+--------------------+--------------------+
|                time|             content|
+--------------------+--------------------+
|2022-04-27T06:59:...|"pattern not matc...|
+--------------------+--------------------+
```

## SparkSQL DataFrame API開發
在結構層面:
* StructType對象描述整個DataFrame的表結構
* StructField描述一個列的訊息
```python
struct_type = StructType().\ # StructType(整個表結構)
  add("id",IntegerType(),False).\ # StructField(單個欄位訊息)
  add("name",StringType(),False).\ # StructField(單個欄位訊息)
  add("age",IntegerType(),False) # StructField(單個欄位訊息)
```
在數據層面:
* Row對象記錄一行數據
* Column對象記錄一列數據並包含列的訊息

### DataFrame的Code構建 - 基於RDD方式1
DataFrame對象可以從RDD轉換而來，都是分布式數據集
```python
rdd = sc.textFile("people.txt").\
    map(lambda x: x.split(",")).\
    map(lambda x: (x[0],int(x[1])))

#convert to DataFrame
#para1: 被轉換的RDD
#para2: 欄位名稱，透過list方式指定，按照順序提供
df = spark.createDataFrame(rdd, schema=['name','age'])

#輸出表結構
df.printSchema()

#輸出df中的數據
#para1: 輸出筆數，default=20
#para2: 是否截斷，如果數據長度超過20後續的內容是否顯示，True->截斷，False->不截斷
df.show()

#將df對象轉換成臨時視圖表，可供SQL語句查詢
df.createOrReplaceTempView("people")
spark.sql("SELECT * FROM people WHERE age < 30").show()
```

### DataFrame的Code構建 - 基於RDD方式2
透過StructType對象定義DataFrame表結構轉換RDD
```python
# RDD to DataFrame 2 (StructType)
from pyspark.sql.types import StructType, StringType, IntegerType

rdd = sc.textFile("people.txt").\
    map(lambda x: x.split(",")).\
    map(lambda x: (x[0],int(x[1])))

schema = StructType().add("name",StringType(),nullable=True).\
            add("age",IntegerType(),nullable=False)

df = spark.createDataFrame(rdd, schema=schema)

df.printSchema()
df.show()
```

### DataFrame的Code構建 - 基於RDD方式3
使用RDD的toDF方法轉換RDD
```python
# RDD to DataFrame 3 (toDF)
from pyspark.sql.types import StructType, StringType, IntegerType

rdd = sc.textFile("people.txt").\
    map(lambda x: x.split(",")).\
    map(lambda x: (x[0],int(x[1])))


#直接放List給欄位名稱這種方式類型只能靠推斷，對類型不敏感要快速創建使用
df1 = rdd.toDF(["name","age"])
df1.printSchema()
df1.show()

#透過StructType才可以指定Type & Name
schema = StructType().add("name",StringType(),nullable=True).\
            add("age",IntegerType(),nullable=False)
df2 = rdd.toDF(schema)
df2.printSchema()
df2.show()
```

### DataFrame的Code構建 - 基於Pandas的DataFrame
將Pandas的DataFrame對象轉換成分布式SparkSQL的DataFrame對象
```python
# RDD to DataFrame 4 (基於Pandas的DataFrame)
from pyspark.sql.types import StructType, StringType, IntegerType
import pandas as pd

pdf = pd.DataFrame(
    {
        "id":[1,2,3],
        "name": ["Amy","Bob","Chris"],
        "age": [11, 21, 11]
    }
)

df = spark.createDataFrame(pdf)
df.printSchema()
df.show()
```

### DataFrame的Code構建 - 讀取外部數據(Text)
透過SparkSQL的統一API進行數據讀取構建DataFrame
```python
# 統一API讀取
#create SparkSession
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

#get SparkContext by SparkSession
sc = spark.sparkContext

#對於text是直接讀取整個列當作一筆資料，默認的欄位名稱是value類型是string
schema = StructType().add("data",StringType(),nullable=True)
df = spark.read.format("text").schema(schema).load("people.txt")

df.printSchema()
df.show()

root
 |-- data: string (nullable = true) #不設定的話欄位名稱會是value

+-----------+
|       data|
+-----------+
|Michael, 29|
|   Andy, 30|
| Justin, 19|
+-----------+

```
### DataFrame的Code構建 - 讀取外部數據(JSON)
```python
#JSON自帶schema訊息
df = spark.read.format("json").load("people.json")
df.printSchema()
df.show()

#output
root
 |-- age: long (nullable = true)
 |-- name: string (nullable = true)

+----+-------+
| age|   name|
+----+-------+
|null|Michael|
|  30|   Andy|
|  19| Justin|
+----+-------+
```

### DataFrame的Code構建 - 讀取外部數據(CSV)
```python
df = spark.read.format("csv").option("sep",",").option("header",True).option("encoding","utf-8").schema("time STRING,content STRING").load("logstash-2022.04.27.csv")
df.printSchema()
df.show()

#output
root
 |-- time: string (nullable = true)
 |-- content: string (nullable = true)

+--------------------+--------------------+
|                time|             content|
+--------------------+--------------------+
|2022-04-27T06:59:...|"pattern not matc...|
|2022-04-27T06:59:...|"pattern not matc...|
...
```

### DataFrame的Code構建 - 讀取外部數據(parquet)
parquet是Spark中常用的一種列式儲存文件格式，和Hive中的ORC差不多都是列儲存格式。

與普通文本文件的區別:
* parquet內建schema(列名、列類型、是否為空)
* 以列作為儲存格式
* 儲存是序列化儲存在文件的(有壓縮，體積小)
```python
df = spark.read.format("parquet").load("users.parquet")
df.printSchema()
df.show()
```

### DSL風格
DSL稱為領域特定語言，指的就是DataFrame的特有API。DSL風格就是以調用API的方式來處理Data，例如: df.where().limit()
```python
df = spark.read.format("csv").schema("id INT, subject STRING, score INT").load("stu_score.txt")

# get column
id_column = df["id"]
subject_column = df["subject"]

#DSL
df.select(["id","subject"]).show()
df.select("id","subject").show()
df.select(id_column,subject_column).show()

#filter API
df.filter("score < 99").show()
df.filter(df['score'] < 99).show()

#where API
df.where("score < 99").show()
df.where(df['score'] < 99).show()

#groupby API
df.groupBy("subject").count().show()
df.groupBy(df['subject']).count().show()

# 回傳值是一個有分組關係的數據結構(GroupedData)不是DataFrame，調用聚合方法後回傳值還是DataFrame
r = df.groupBy("subject")
print(type(r))
```

### SQL風格
DataFrame可以看成是一個關聯數據表，可以在程式中使用spark.sql()執行SQL語句查詢並回傳DataFrame。如果想使用SQL風格的語法，需要先將DataFrame註冊成表，如下:
```python
df.createTempView("score") # 創建一個tempview
df.createOrReplaceTempView("score") #創建一個tempview，如果存在就取代
df.createGlobalTempView("score") # 創建一個Global tempview

#GlobalTempView: 跨SparkSession對象使用，在一個Application中多個SparkSession中均可調用，查詢前帶上前綴: global_temp
```
```python
df = spark.read.format("csv").schema("id INT, subject STRING, score INT").load("stu_score.txt")

#SQL
df.createTempView("score")
df.createOrReplaceTempView("score2")
df.createGlobalTempView("score3") 

spark.sql("SELECT subject, COUNT(*) AS cnt FROM score GROUP BY subject").show()
spark.sql("SELECT subject, COUNT(*) AS cnt FROM score2 GROUP BY subject").show()
spark.sql("SELECT subject, COUNT(*) AS cnt FROM global_temp.score3 GROUP BY subject").show()
```

### Spark SQL function
提供一系列的計算函數供SparkSQL使用，回傳值多數都是Column對象
```python
# 導入
from pyspark.sql import functions as f
```

```python
rdd = sc.textFile("words.txt").flatMap(lambda x: x.split(" ")).map(lambda x: [x])
df = rdd.toDF(["word"])

#SQL
df.createOrReplaceTempView("words")
spark.sql("SELECT word, count(*) AS cnt FROM words GROUP BY word ORDER BY cnt DESC").show()

#DSL
from pyspark.sql import functions as F
df = spark.read.format("text").load("words.txt")

#withColumn 對column操作，有更新成新列保留沒有就取代掉舊的
df2 = df.withColumn("value", F.explode(F.split(df['value']," ")))
df2.groupBy("value").count().withColumnRenamed("value","word").withColumnRenamed("count","cnt").orderBy("cnt",ascending=False).show()
```

### Extra functions
資料來源(http://files.grouplens.org/datasets/movielens/)，使用ml-100k中的u.data
```python
schema = StructType().add("user_id",StringType(),nullable=False).\
    add("movie_id",IntegerType(),nullable=False).\
    add("rank",IntegerType(),nullable=False).\
    add("ts",StringType(),nullable=False)
df = spark.read.format("csv").option("sep","\t").option("header",False).option("encoding","utf-8").schema(schema).load("/home/cris/ml-100k/u.data")

# 用戶平均分
df.groupBy("user_id").avg("rank").withColumnRenamed("avg(rank)","avg_rank").withColumn("avg_rank",F.round("avg_rank",2)).orderBy("avg_rank",ascending=False).show()

# 電影平均分
df.createOrReplaceTempView("movie")
spark.sql("SELECT movie_id, ROUND(AVG(rank),2) AS avg_rank FROM movie GROUP BY movie_id ORDER BY avg_rank DESC").show()

# 查詢大於平均分數電影的評分數量
print("大於平均分數電影的評分數量:", df.where(df['rank'] > df.select(F.avg(df['rank'])).first()['avg(rank)']).count())

# 查詢高分電影(分數>3)評分次數最多的用戶，他的平均評分
# 找到此人 first>get first row
user_id = df.where("rank > 3").groupBy("user_id").count().withColumnRenamed("count","cnt").orderBy("cnt",ascending=False).limit(1).first()['user_id']

# 計算這個人的平均評分
df.filter(df['user_id'] == user_id).select(F.round(F.avg("rank"),2)).show()

# 查詢每個用戶的平均評分,最高評分,最低評分
df.groupBy("user_id").\
    agg(
        F.round(F.avg("rank"),2).alias("avg_rank"),
        F.min("rank").alias("min_rank"),
        F.max("rank").alias("max_rank")
    ).show()

# 查詢評分超過100次的電影 平均評分排名TOP10的
df.groupBy("movie_id").\
    agg(
        F.count("movie_id").alias("cnt"),
        F.round(F.avg("rank"),2).alias("avg_rank")
    ).where("cnt > 100").\
    orderBy("avg_rank",ascending=False).\
    limit(10).\
    show()

# agg: GroupedData對象的API，作用是可以在裡面寫多個聚合
# alias: Column對象的API，針對單一個欄位名稱改名
# withColumnRenamed: DataFrame對象的API，可以對DF中的欄位進行改名，一次可以改1~多
# orderBy: DataFrame對象的API，進行排序，para1:被排序的欄位，para2: 升序True 降序False
# first: DataFrame對象的API，取出DF的first row，回傳值是ROW對象
# Row對象: 就是一個數組，可以透過row['欄位名']取出欄位的具體數值
```

### SparkSQL Shuffle 分區數
在執行程式時，查看WEB UI監控頁面發現某個Stage(agg or join)有200個Task，也就是說RDD有200個分區。主要原因是在SparkSQL中當Job產生Shuffle時，默認的分區數(spark.sql.shuffle.partitions)為200。

實際執行設置可以設置在:
1. 配置文件: conf/spark-defaults.conf中的spark.sql.shuffle.partitions 100
2. client端提交參數: bin/spark-submit --conf "spark.sql.shuffle.partitions=100"
3. 在Code中可以設置
```python
spark = SparkSession.builder.\
    appName("create df").\
    master("local[*]").\
    config("spark.sql.shuffle.partitions","2").\
    getOrCreate()
```
! 集群模式下200個默認算比較適合，但如果在Local下運行，200多個分區在調度上會造成額外的損耗

! 與RDD平行度的設置是相互獨立的

### SparkSQL數據清洗API
```python
df = spark.read.format("csv").option("sep",";").option("header",True).load("people.csv")

#去重複(對全部欄位)
df.dropDuplicates().show()

#針對特定欄位去重複
df.dropDuplicates(['age','job']).show()

#缺失值處理，有n/a值整列刪除
df.dropna().show()
df.dropna(thresh=3).show()
df.dropna(thresh=2,subset=['name','age']).show()

#將缺失值填充
df.fillna("loss").show()
df.fillna("N/A", subset=['job']).show()

#設定字典對所有欄位加入填充規則
df.fillna({"name":"未知姓名","age":1,"job":"worker"}).show()
```

### SparkSQL DataFrame數據輸出
透過統一API輸出DataFrame數據
```python
df.write.mode().format().option(K, V).save(PATH)
# mode: append、overwrite、ignore、error(重複就報異常)
# format: text、csv、json、parquet、orc、avro、jdbc
# text只支援單列輸出
# option設置屬性: ex .option("sep", ",")
# save: 輸出路徑: 本地文件和HDFS
```
```python
schema = StructType().add("user_id",StringType(),nullable=False).\
    add("movie_id",IntegerType(),nullable=False).\
    add("rank",IntegerType(),nullable=False).\
    add("ts",StringType(),nullable=False)
df = spark.read.format("csv").option("sep","\t").option("header",False).option("encoding","utf-8").schema(schema).load("/home/cris/ml-100k/u.data")

# write text
df.select(F.concat_ws("---","user_id","movie_id","rank","ts")).write.mode("overwrite").format("text").save("/home/cris/Documents/text")

# csv
df.write.mode("overwrite").format("csv").option("sep", ";").option("header", True).save("/home/cris/Documents/csv")

# json
df.write.mode("overwrite").format("json").save("/home/cris/Documents/json")

# parquet
df.write.mode("overwrite").format("parquet").save("/home/cris/Documents/parquet")
```
#### DataFrame For JDBC
要先有mysql的驅動jar包，否則會報錯"No suitable Driver"
```python
# save，jdbc輸出會自動創建表，根據DataFrame的資訊
df.write.mode("overwrite").\
    format("jdbc").\
    option("url", "jdbc:mysql://node1:3306/bigdata?useSSL=false&useUnicode=true").\
    option("dbtable", "movie_data").\
    option("user", "root").\
    option("password", "123456").\
    save()

# load
spark.read.format("jdbc").\
    option("url", "jdbc:mysql://node1:3306/bigdata?useSSL=false&useUnicode=true").\
    option("dbtable", "movie_data").\
    option("user", "root").\
    option("password", "123456").\
    load()
```

### SparkSQL 函數定義(UDF) - Python
SparkSQL支持UDF和UDAF，但在Python目前只能定義UDF。

UDF定義方式:
* sparksession.udf.register: 註冊的UDF可以用於DSL和SQL，回傳值用於DSL風格，傳參內給的名字用於SQL風格
* pyspark.sql.functions.udf: 僅能用於DSL風格

```python
rdd = sc.parallelize([1,2,3,4,5,6,7]).map(lambda x:[x])
df = rdd.toDF(["num"])

def num_ride_10(num):
    return num * 10

#1 sparksession.udf.register() DSL&SQL均可使用
#para1: 創建的udf名稱，僅可以用於SQL
#para2: 處理邏輯，是一個單獨的方法
#para3: udf的回傳值類型，真實的回傳值一定要和聲明的一致
#回傳值對象僅可以用於DSL風格
udf2 = spark.udf.register("udf1", num_ride_10, IntegerType())

#SQL中使用 selectExpr:以SQL表達式執行
df.selectExpr("udf1(num)").show()

#DSL中使用，注意欄位名稱還是被註冊的udf1
df.select(udf2(df['num'])).show()

#2 pyspark.sql.functions.udf，僅能用於DSL風格，名稱為函數名稱
udf3 = F.udf(num_ride_10, IntegerType())
df.select(udf3(df['num'])).show()
```

回傳值為Array
```python
rdd = sc.parallelize([["hadoop spark flink"],["hadoop flink java"]])
df = rdd.toDF(["line"])

def split_line(data):
    return data.split(" ") #回傳值是Array

#法1
udf2 = spark.udf.register("udf1", split_line, ArrayType(StringType()))
#DSL風格
df.select(udf2(df['line'])).show()
#SQL風格
df.createOrReplaceTempView("lines")
spark.sql("SELECT udf1(line) FROM lines").show(truncate=False)

#法2
udf3 = F.udf(split_line, ArrayType(StringType()))
df.select(udf3(df['line'])).show()
```

回傳值為字典
```python
rdd = sc.parallelize([[1],[2],[3]])
df = rdd.toDF(["num"])

def process(data):
    return {"num": data, "letters": string.ascii_letters[data]}

#udf回傳值是字典的話要用StructType來實作

udf1 = spark.udf.register("udf1", process, StructType().add("num", IntegerType(), nullable=True).add("letters", StringType(), nullable=True))
df.selectExpr("udf1(num)").show(truncate=False)
df.select(udf1(df['num'])).show(truncate=False)
```

### Python UDAF的實現
```python
rdd = sc.parallelize([1 , 2, 3, 4, 5], 3)
df = rdd.map(lambda x: [x]).toDF(['num'])

# 折衷方式達成UDAF 利用RDD算子達到聚合的操作
# 從DataFrame進行repartition回傳是row對象-->因為是從DataFrame去重新分區
single_partition_rdd = df.rdd.repartition(1)

def process(iter):
    sum = 0
    for row in iter:
        sum += row['num']
    return [sum]  # 這裡要回傳list，因為mapPartition方法支持的是list對象

print(single_partition_rdd.mapPartitions(process).collect())
```

### SparkSQL窗口函數
顯示聚集前的數據又顯示聚集後的數據，在最後添加聚合函數的結果
```python
rdd = sc.parallelize([(1,"國語",90),(2,"數學",91),(3,"英語",92),(2,"英語",96),(2,"國語",91),(1,"數學",94),(3,"數學",95),(1,"英語",91),(3,"國語",91)])
schema = StructType().add("name", StringType()).add("class", StringType()).add("score", IntegerType())
df = rdd.toDF(schema)

df.createOrReplaceTempView("stu")

# 聚合窗口函數
spark.sql("SELECT *, AVG(score) OVER() AS avg_score  FROM stu").show()

# 排序窗口函數
spark.sql("""
    SELECT *, ROW_NUMBER() OVER(ORDER BY score DESC) AS row_number_rank, 
    DENSE_RANK() OVER(PARTITION BY class ORDER BY score DESC) AS dense_rank,
    RANK() OVER(ORDER BY score) AS rank
    FROM stu
    """).show()

# NTILE分組窗口函數
spark.sql("""
    SELECT *, NTILE(6) OVER(ORDER BY score DESC) FROM stu
    """).show()
```

## SparkSQL的運行流程

### SparkSQL的自動優化
RDD的執行會完全按照開發者的程式，執行效率會受到code的好壞影響。而SparkSQL會對完成的code執行**自動優化**，以提升程式執行效率，避免受到code好壞的影響。

為什麼SparkSQL可以被自動優化而RDD不行?

主要原因差在結構，RDD內含的數據類型不限格式和結構，而DataFrame則一定是二維表結構，所以可以被針對。

! SparkSQL的自動優化依賴於Catalyst優化器。

### Catalyst優化器
流程: API --> Catalyst --> RDD --> Cluster
1. Spark透過一些API接收SQL
2. 收到SQL後交給Catalyst，Catalyst負責解析SQL、生成執行計畫等
3. Catalyst輸出RDD的執行計畫
4. 交由集群運行

Catalyst內部工作流程:
1. 解析SQL並且生成AST(抽象語法樹)
2. 在AST中加入元數據的訊息，主要是為了一些優化，方便Spark本身對語法做標記
3. 對加入元數據的AST輸入優化器進行優化
   * 斷言下推(Predicate Pushdown): 將Filter這種可以減少數據集的操作下推，放在下層的位置，這樣可以減少操作時候的數據量。
   * 列值裁剪(Column Pruning): 在斷言下推後執行裁剪，例如people表之上只用到了id欄位，所以可以把其他欄位裁剪掉，這樣可以減少處理的數據量，進而優化處理速度。
4. 上面過程生成的AST還沒辦法最終運行，這個AST稱為邏輯計畫，結束後需要生成物理計畫，進而生成RDD來執行
   * 可以使用queryExecution方法查看邏輯執行計畫，使用explain方法查看物理執行計畫

簡單說明:
* 斷言下推: 將邏輯判斷提前到前面，以減少shuffle階段的數據量
* 列值裁剪: 將加載的列進行裁剪，減少被處理數據的寬度

更簡單說:
* 提前執行where
* 提前規劃select的數量

### SparkSQL執行流程
1. 提交SparkSQL code
2. Catalyst優化
3. Driver執行環境入口建立(SparkSession)
4. DAG調度器規劃邏輯任務
5. Task調度區分配邏輯任務到具體Executor上工作並監控管理任務
6. Worker工作
   

## SparkSQL & Hive的集成
Spark提供執行引擎，Hive的MetaStore提供元數據管理功能

### 配置
前提:MetaStore存在並開基以及Spark要知道MetaStroe在哪裡(IP & Port)

1. 在Spark的conf目錄中創建hive-site.xml
2. 將mysql的驅動jar包放入spark的jars目錄(因為要連接元數據可能會用到)
3. 確保Hive配置了MetaStore相關的服務，檢查Hive配置目錄內的hive-site.xml
```python
spark = SparkSession
  .builder()
  .appName("Spark Hive Example")
  .config("spark.sql.warehouse.dir", warehouseLocation)
  .enableHiveSupport()
  .getOrCreate()
```

## 分布式SQL執行引擎

### 概念
Spark中有一個服務叫做ThriftServer服務，可以啟動並監聽port 10000。這個服務對外提供功能，可以用資料庫工具或是code連接，直接寫SQL即可操作Spark。

### 配置
1. 確保已經配置好spark on hive
2. 啟動ThriftServer
```bash
$SPARK_HOME/sbin/start-thriftserver.sh \
--hiveconf hive.server2.thrift.port=10000 \
--hiveconf hive.server2.thrift.bind.host=node1 \
--master local[*]
```
```python
from pyhive import hive

conn = hive.Connection(host="node1", port=10000, username="hadoop")

#獲取游標對象
cursor = conn.cursor()

cursor.execute("SELECT * FROM student")

#透過fetchall獲得回傳值
result = cursor.fetchall()
print(result)
```

## Spark特性

### Spark Shuffle
在Shuffle過程中，提供數據的稱為Map端(Shuffle Write)，接收數據的稱為Reduce端(Shuffle Read)。在Spark兩個階段中，在前一個階段產生一批的MAP提供數據，下一階段產生一批Reduce接收數據。

Shuffle管理器:
* HashShuffleManager
  * 可優化，透過在記憶體中先將不同task進行shuffle，在後續網路I/O次數也會跟著減少。(不同Task透過共享Buffer緩衝區減少緩衝區寫入至磁盤的數量，提高性能)
* SortShuffleManager
  * 普通運行機制: 經過分批排序後，通過記憶體緩衝區依依寫成磁盤文件，接著合併磁盤文件。下游的task參考索引文件去合併後的磁盤文件拉取數據，對比hashshuffle減少很多磁盤文件，節省網路IO的花費
  * bypass運行機制: 觸發條件1.shuffle map task數量小於spark.shffle.sort.bypassMergeThreshold 2.不是聚合類的shuffle算子(如reduceByKey)

! bypass跟普通模式主要差在1.不會進行排序，節省性能花費 2.磁盤寫入機制不同

### 3.0新特性

#### Adaptive Query Execution 自適應查詢(SparkSQL)
在運行時對查詢執行計畫進行優化，允許Planner在運行時執行可選計畫，這些可選計畫會基於運行時數據統計進行優化，進而提高性能。

三個自適應優化:
* 動態合併Shuffle Partitions: 可以動態調整shuffle分區的數量。使用者可以在開始時設置較多的shuffle分區數，AQE會在運行時將相鄰的小分區合併為較大的分區。
* 動態調整Join策略: 可以在一定程度上避免由於缺少統計資訊或錯誤估計大小導致執行計畫性能不佳的情況。在運行上會將sort merge join轉換成broadcast hash join，進而提升性能。
* 動態優化傾斜Join(Skew joins): skew joins可能導致負載不平衡且嚴重降低性能。在AQE從shuffle文件統計資訊中檢測到任何傾斜後，它可以將傾斜的分區分割成更小的分區，並將它們與另一側的相應分區連接起來。這種優化可以平行化處理傾斜，獲得更好的整體性能。

```bash
# 開啟AQE方式
set spark.sql.adaptive.enabled = true;
```

### Dynamic Partition Pruning 動態分區裁剪(SparkSQL)
當優化器在編譯時無法辨識可跳過的分區時，可以用動態分區裁剪，基於運行時推斷的訊息進一步執行分區裁剪。這在星型模型中很常見，星型模型是由一個或多個引用了任意數量的維度表的事實表組成。在這種連接操作中，可以透過識別維度表過濾後的分區才裁剪從事實表中讀取的分區。

! 舉例來說有些join操作會同時動用到多個表，但實際完成後的才是真正的表(事實表)，但動態分區裁剪可以先把filter提前應用在join前的表(維度表)去減少數據量以提升性能。

### PySpark & Koalas(可擇一使用)
很多開發人員在數據結構和分析方面使用pandas API，但僅限於單節點。Databricks開發的Koalas是基於Apache Spark的pandas API實現，能在分布式環境中更高效處理大數據。
```python
import pandas as pd
import numpy as np
import databricks.koalas as ks

dates = pd.date_range('20130101', periods=6)
pdf = pd.DataFrame(np.random.randn(6,4), index=dates, columns=list('ABCD'))
kdf = ks.from_pandas(pdf) #底層跑spark的分布式計算
type(kdf)
sdf = spark.createDataFrame(pdf)
kdf = sdf.to_koalas()
kdf
ks.DataFrame({'A':['foo', 'bar'], 'B': [1, 2]})
kdf.head()
kdf.index
kdf.columns
kdf.to_numpy()
kdf.describe()
kdf.T
pdf1 = pdf.reindex(index=dates[0:4], columns=list(pdf.columns)+['E'])
pdf1.loc[dates[0]:dates[1], 'E'] = 1
pdf1
kdf1 = ks.from_pandas(pdf1)
kdf1.dropna(how='any')
kdf1.fillna(value=5)
kdf.groupby('A').sum()
kdf.to_csv('foo.csv')
ks.read_csv('foo.csv').head(10)
kdf.to_parquet('bar.parquet')
ks.read_parquet('bar.parquet')
kdf.to_spark_io('zoo.orc', format="orc") #調用spark io
ks.read_spark_io('zoo.orc', format="orc").head(10)
```

