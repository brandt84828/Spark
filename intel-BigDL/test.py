import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import lit
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from bigdl.dllib.nncontext import *
from bigdl.dllib.keras.layers import *
from bigdl.dllib.keras.models import Sequential
from bigdl.dllib.nnframes import *
from bigdl.dllib.nn.criterion import *
from py4j.java_gateway import java_import

#java_import(spark._sc._jvm, "org.apache.spark.sql.api.python.*")


ZooContext.log_output = True # (this will display terminal's stdout and stderr in the Jupyter notebook).

sparkConf = init_spark_conf().setAppName("NNClassifer").setMaster('local[32]')
sc = init_nncontext(sparkConf)
#sc = init_nncontext()
spark = SparkSession(sc)

path = "/home/cris/intel/pima-indians-diabetes.data.csv"
df = spark.read.csv(path, sep=',', inferSchema=True).toDF("num_times_pregrant", "plasma_glucose", "blood_pressure", "skin_fold_thickness", "2-hour_insulin", "body_mass_index", "diabetes_pedigree_function", "age", "class")
df.show(5)

vecAssembler = VectorAssembler(outputCol="features")
vecAssembler.setInputCols(["num_times_pregrant", "plasma_glucose", "blood_pressure", "skin_fold_thickness", "2-hour_insulin", "body_mass_index", "diabetes_pedigree_function", "age"])
train_df = vecAssembler.transform(df)

changedTypedf = train_df.withColumn("label", train_df["class"].cast(DoubleType())+lit(1))\
    .select("features", "label")
(trainingDF, validationDF) = changedTypedf.randomSplit([0.9, 0.1])

x1 = Input(shape=(8,))
dense1 = Dense(12, activation='relu')(x1)
dense2 = Dense(8, activation='relu')(dense1)
dense3 = Dense(2)(dense2)
model = Model(x1, dense3)

classifier = NNClassifier(model, CrossEntropyCriterion(), [8]) \
    .setOptimMethod(Adam()) \
    .setBatchSize(56) \
    .setMaxEpoch(100)

nnModel = classifier.fit(trainingDF)

predictionDF = nnModel.transform(validationDF).cache()
predictionDF.sample(False, 0.1).show()

evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictionDF)

print("Accuracy = %g " % (accuracy))

