from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql import SparkSession


spark = SparkSession\
    .builder\
    .appName("MovieRecommendation")\
    .getOrCreate()

ratings = spark.table("amazon_movie_reviews")

# ALS requires Numeric values for user id and item id.
# We need to convert from string to float
#
# Without `dropna()`, `StringIndexer` will fail with NullPointerException
ratings = ratings.select("user_id", "product_id", "score", "reviewed_at").dropna()

from pyspark.ml.feature import StringIndexer
user_indexer = StringIndexer(inputCol="user_id", outputCol="user_id_index")
item_indexer = StringIndexer(inputCol="product_id", outputCol="product_id_index")

indexed = user_indexer.fit(ratings).transform(ratings)
indexed = item_indexer.fit(indexed).transform(indexed)

# Join metadata for looking movie title
meta = spark.table("amazon_meta").select("product_id", "title")
indexed = indexed.join(meta, "product_id")

# Split data into train and test data set
(training, test) = indexed. \
  select("user_id_index", "product_id_index", "score", "reviewed_at", "title"). \
  randomSplit([0.6, 0.4], seed=0)

# Train and evaluate with ALS

from pyspark.ml.tuning import TrainValidationSplit, ParamGridBuilder
als = ALS(maxIter=5, userCol="user_id_index", itemCol="product_id_index", ratingCol="score")

param_grid = ParamGridBuilder().addGrid(
  als.regParam,
  [0.01, 0.1, 1.0]
).build()

evaluator = RegressionEvaluator(metricName="rmse", labelCol="score",
                               predictionCol="prediction")

tvs = TrainValidationSplit(
  estimator=als,
  estimatorParamMaps=param_grid,
  evaluator=evaluator,
)

model = tvs.fit(training)

predictions = model.transform(test)
predictions = predictions.fillna(0, subset=['prediction'])
rmse = evaluator.evaluate(predictions)

print("RMSE: " + str(rmse))

# Show standard deviation for compalison of RMSE
from pyspark.sql.functions import col, stddev
test.select([stddev('score')]).show()

print(model.bestModel._java_obj.parent().getRegParam())

spark.stop()