## Recommendation with Factorization Machines
# This code is runned with 16GB RAM with Python2
#
# Factorization Machines is extended model of Matrix Factorization.
# For more detail, you can see [this article](https://getstream.io/blog/factorization-recommendation-systems/).
# fastFM is one implementation which can be called with Python.

from pyspark.sql import SparkSession

spark = SparkSession\
    .builder\
    .appName("MovieRecommendation")\
    .config("spark.executor.memory", "2g")\
    .config("spark.driver.memory", "4g")\
    .getOrCreate()

ratings = spark.table("amazon_movie_reviews")

ratings.show()
ratings = ratings.select("user_id", "product_id", "score", "helpfulness_agreed", "helpfulness_reviewed", "reviewed_at").dropna()

meta = spark.table("amazon_meta")
meta.show()

meta = meta.select("product_id", "sales_rank", "group")
joined_ratings = ratings.join(meta, "product_id")
joined_ratings.show()

pd_ratings = joined_ratings.toPandas()
pd_ratings['sales_rank'] = pd_ratings['sales_rank'].fillna(value=-1)
pd_ratings['group'] = pd_ratings['group'].fillna(value="Nothing")

from sklearn.feature_extraction import DictVectorizer

v = DictVectorizer()

X = v.fit_transform(list(pd_ratings[["user_id", "product_id", "score"]].drop('score', axis=1).T.to_dict().values()))
y = pd_ratings['score'].tolist()

from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.4, random_state=42)

from sklearn.preprocessing import StandardScaler
import numpy as np


## Plot with iter
# How many times should we iterate?

n_iter = 300
seed = 123
step_size = 1
rank = 4

from fastFM import mcmc
from sklearn.metrics import mean_squared_error

y_train = np.array(y_train)
fm = mcmc.FMRegression(n_iter=0, rank=rank, random_state=seed)
# Allocates and initalizes the model and hyper parameter.
fm.fit_predict(X_train, y_train, X_test)

rmse_test = []
hyper_param = np.zeros((n_iter -1, 3 + 2 * rank), dtype=np.float64)

for nr, i in enumerate(range(1, n_iter)):
  if nr % 10 == 0:
    print(nr, i)
  fm.random_state = i * seed
  y_pred = fm.fit_predict(X_train, y_train, X_test, n_more_iter=step_size)
  rmse_test.append(np.sqrt(mean_squared_error(y_pred, y_test)))
  hyper_param[nr, :] = fm.hyper_param_

values = np.arange(1, n_iter)
x = values * step_size
burn_in = 5
x = x[burn_in:]

from matplotlib import pyplot as plt

def plot_iter(x, rmse_test, hyper_param, burn_in):
  fig, axes = plt.subplots(nrows=2, ncols=2, sharex=True, figsize=(15, 8))

  axes[0, 0].plot(x, rmse_test[burn_in:], label='test rmse', color="r")
  axes[0, 0].legend()
  axes[0, 1].plot(x, hyper_param[burn_in:,0], label='alpha', color="b")
  axes[0, 1].legend()
  axes[1, 0].plot(x, hyper_param[burn_in:,1], label='lambda_w', color="g")
  axes[1, 0].legend()
  axes[1, 1].plot(x, hyper_param[burn_in:,3], label='mu_w', color="g")
  axes[1, 1].legend()

plot_iter(x, rmse_test, hyper_param, burn_in)

# Minimum RMSE
print(np.argmin(rmse_test), np.min(rmse_test))



## Plot with Rank
# Rank is the key hyper parameter for tuning
rmse_test = []
n_iter = 100
ranks = [4, 8, 16]

for rank in ranks:
  print(rank)
  fm = mcmc.FMRegression(n_iter=n_iter, rank=rank, random_state=seed)
  # Allocates and initalizes the model and hyper parameter.
  fm.fit_predict(X_train, y_train, X_test)

  y_pred = fm.fit_predict(X_train, y_train, X_test)
  rmse = np.sqrt(mean_squared_error(y_pred, y_test))
  rmse_test.append(rmse)
  print('rank:{}\trmse:{:.3f}'.format(rank, rmse))

    
def plot_ranks(ranks, rmse_test):
  plt.plot(ranks, rmse_test, label='test rmse', color="r")
  plt.legend()


plot_ranks(ranks, rmse_test)  
print("min rmse: {:.3f}".format(np.min(rmse_test)))

pd_ratings['review_year'] = pd_ratings['reviewed_at'].str.split('-').str.get(0)

## Evaluate with multiple column set
# Factorization machines can use feature set rather than only user id and item id.
# In this examination, we will find which is the better feature set.

candidate_columns = [
  ["user_id", "product_id", "score"],
  ["user_id", "product_id", "score", "helpfulness_agreed"],
  ["user_id", "product_id", "score", "helpfulness_agreed", "helpfulness_reviewed"],
  ["user_id", "product_id", "score", "sales_rank", "group"],
  ["user_id", "product_id", "score", "review_year"],
]

rmse_test = []

rank = 4

for column in candidate_columns:
  print(column)
  filtered_lens = pd_ratings[column].dropna()
  v = DictVectorizer()
  X_more_feature = v.fit_transform(list(filtered_lens.drop('score', axis=1).T.to_dict().values()))
  y_more_feature = filtered_lens['score'].tolist()

  X_mf_train, X_mf_test, y_mf_train, y_mf_test = train_test_split(X_more_feature, y_more_feature, test_size=0.4, random_state=42)

  scaler = StandardScaler()
  y_mf_train_norm = scaler.fit_transform(np.array(y_mf_train).reshape(-1, 1)).ravel()

  fm = mcmc.FMRegression(n_iter=n_iter, rank=rank, random_state=seed)
  # Allocates and initalizes the model and hyper parameter.
  fm.fit_predict(X_mf_train, y_mf_train_norm, X_mf_test)

  y_pred = fm.fit_predict(X_mf_train, y_mf_train_norm, X_mf_test)
  rmse_test.append(np.sqrt(mean_squared_error(scaler.inverse_transform(y_pred.reshape(-1, 1)), y_mf_test)))

print(rmse_test)
# Add Spark ALS RMSE    
rmse_test.append(2.90756035416)

## Plot the RMSEs for each features

def plot_columns(rmse_test, labels, min_max=None):
  ind = np.arange(len(rmse_test))
  bar = plt.bar(ind, height=rmse_test)
  plt.xticks(ind, labels, rotation='vertical')
  if min_max:
    plt.ylim(min_max)


labels = ('Base', 'B+ h_agreed', 'B+ h_agreed & h_reviewed', 'B+ sales_rank & group', 'B+ review_year', 'Spark ALS')
plot_columns(rmse_test, labels)
plot_columns(rmse_test, labels, (0.8, 3.0))
plot_columns(rmse_test, labels, (0.8, 1.0))

spark.stop()