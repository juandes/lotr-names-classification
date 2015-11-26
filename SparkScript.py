from pyspark import SparkConf, SparkContext
from pyspark.ml import Pipeline
from pyspark.ml import Transformer
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import NGram
from pyspark.ml.feature import IDF
from pyspark.ml.feature import HashingTF
from pyspark.ml.classification import NaiveBayes
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.mllib.linalg import Vectors

conf = SparkConf().setMaster("spark://Juande.local:7077").setAppName(
    "LOTR Names classification")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

# Import both the train and test dataset and register them as tables
imported_data = sqlContext.read.format('com.databricks.spark.csv').options(
    header='true') \
    .load('/Users/Juande/Development/lotr-names-classification/characters_no_surnames.csv')

# Map the race to a number
race_to_number = {'Man' : 0.0, 'Ainur' : 1.0, 'Elf': 2.0, 'Hobbit': 3.0, 'Dwarf': 4.0}

# Build a new rdd made of a row that has the name of the character, the name as a list of the characters, the race of
# the character and the length of the name (this might be used later)
data_rdd = imported_data.map(lambda row: Row(complete_name = row.name, name = list(row.name.lower()), race = race_to_number[row.race], length = len(row.name)))
df = sqlContext.createDataFrame(data_rdd)


# Pipeline consisting of three stages: NGrams, HashingTF and IDF
ngram = NGram(n=2, inputCol="name", outputCol="nGrams")
hashingTF = HashingTF(numFeatures=500, inputCol="nGrams", outputCol="TF")
idf = IDF(inputCol="TF", outputCol="idf")
pipeline = Pipeline(stages = [ngram, hashingTF, idf])

# Fit the pipeline 
pipelined_data = pipeline.fit(df)
transformed_data = pipelined_data.transform(df)
training_set, test_set = transformed_data.randomSplit([0.8, 0.2], seed = 10) #10

# Create the model, train and predict
nb = NaiveBayes(smoothing=3.0, modelType="multinomial", featuresCol = 'idf', labelCol = 'race')
model = nb.fit(training_set)
predictions = model.transform(test_set)

# Calculate the accuracy
result = predictions.select('complete_name', 'name','race','prediction')
accuracy = 1.0 * result.filter(result.race == result.prediction).count() / result.count()
print (accuracy)
result.show()

# BONUS
#df.groupBy('race').agg({'length': 'avg'}).show()