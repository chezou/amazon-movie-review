# Amazon moview reviews: Data preparation and simple plotting

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, IntegerType
from pyspark.sql.functions import split, from_unixtime

spark = SparkSession\
    .builder\
    .appName("MovieDataPreparation")\
    .getOrCreate()

## Original schema of json for movies.json
# ```
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
# ```

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

## Original schema of json for meta.json
# ```
# schema_json = StructType([
#                    StructField("ASIN", StringType(), nullable=False),
#                    StructField("categories", ArrayType(StringType(), True), nullable=True),
#                    StructField("group", StringType(), nullable=True),
#                    StructField("reviews", ArrayType(), nullable=True),
#                    StructField("salesrank", LongType(), nullable=True),
#                    StructField("similar_items", ArrayType(LongType(), True), nullable=True),
#                    StructField("title", StringType(), nullable=True),
#                    ])
# ```


df_meta = spark.read.json("movies/meta.json")
df_meta = df_meta.withColumn('id', df_meta['id'].cast("long"))
df_meta = df_meta.withColumn('salesrank', df_meta['salesrank'].cast("long"))

df3 = df_meta.selectExpr("ASIN as product_id",
                         "categories",
                         "group",
                         "reviews",
                         "salesrank as sales_rank",
                         "similar_items",
                         "title"
                        )

df3.show()

df3.write.saveAsTable('amazon_meta', format="parquet", mode="overwrite")

spark.stop()
