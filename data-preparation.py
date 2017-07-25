# Amazon moview reviews: Data preparation and simple plotting

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, IntegerType
from pyspark.sql.functions import split, from_unixtime

spark = SparkSession\
    .builder\
    .appName("MovieDataPreparation")\
    .getOrCreate()

## Original schema of json
# schema_json = StructType([
#                    StructField("product/productId", StringType(), nullable=False),
#                    StructField("review/helpfulness", StringType(), nullable=False),
#                    StructField("review/profileName", StringType(), nullable=False),
#                    StructField("review/score", FloatType(), nullable=False),
#                    StructField("review/summary", StringType(), nullable=False),
#                    StructField("review/text", StringType(), nullable=False),
#                    StructField("review/time", LongType(), nullable=False),
#                    StructField("review/userId", StringType(), nullable=False)
#                    ])

df = spark.read.json("movies/movies.json")

split_col = split(df['review/helpfulness'], '/')
df = df.withColumn('helpfulness_agreed', split_col.getItem(0).cast("int"))
df = df.withColumn('helpfulness_reviewed', split_col.getItem(1).cast("int"))
df = df.withColumn('score', df['review/score'].cast("float"))
df = df.withColumn('reviewed_at', from_unixtime(df['review/time']))

df2 = df.selectExpr("`product/productId` as product_id",
              "`review/profileName` as profile_name",
              "`review/summary` as summary",
              "`review/text` as text",
              "`review/userId` as user_id",
              "score",
              "helpfulness_agreed",
              "helpfulness_reviewed",
              "reviewed_at"
              )
df2.show()


df2.write.saveAsTable('amazon_movie_reviews', format="parquet", mode='overwrite')

df2.groupBy("score").count().show()
pd_df = df2.groupBy("score", "helpfulness_agreed").count().toPandas()

### Relationship beween score and helpfulness_agreed
import seaborn as sns
sns.jointplot(x="score", y="helpfulness_agreed", kind='kde',data=pd_df)

spark.stop()