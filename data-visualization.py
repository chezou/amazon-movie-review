from pyspark.sql import SparkSession
from pyspark.sql.functions import year

spark = SparkSession\
    .builder\
    .appName("MovieDataVisualization")\
    .getOrCreate()

df = spark.table("amazon_movie_reviews")
df.dropna().groupBy("score").count().sort("score").show()
pd_df = df.groupBy("score", "helpfulness_agreed").count().toPandas()

### Frequency of `helpfulness_agreed`
pd_df.dropna().groupby(['helpfulness_agreed'])[['count']].sum().plot(logy=True)

### Frequency of `score`
pd_df.dropna().groupby(['score'])[['count']].sum().plot()

### Relationship beween score and helpfulness_agreed
import seaborn as sns
sns.jointplot(x="score", y="helpfulness_agreed", kind='kde',data=pd_df)

### Summarize reviewed year and score

pdf = df.dropna().select("product_id","score", year('reviewed_at').alias('year')).groupBy("score", "year").count().toPandas()
pdf.plot.scatter(x='year', y='score', s=pdf['count']*0.002)

#sns.heatmap(pdf.pivot('year','score', 'count'))
pdf.pivot('year','score', 'count').plot(kind='bar', stacked=True)


spark.stop()