import pandas
import tweepy
import sqlite3 as db
from sys import exit
from re import split
from os import path as path

twitter_token = "AAAAAAAAAAAAAAAAAAAAANNBiAEAAAAArtzWVGRSyjxDsVV321J4UjnyH0s%3DT6O492KBj5bDql1NuPeZ7p2tkbaNHA91Bm8f0IiLEDgWipalJ3"

if not path.exists("AppleStore.csv"):
  exit(
    "AppleStore.csv not found, please put the input file on the same folder as this software."
  )

data = pandas.read_csv("AppleStore.csv")

sorted_data = data[(data["prime_genre"] == 'Book') |
                   (data["prime_genre"] == 'Music')].nlargest(
                     10, 'rating_count_tot')

result = []

for index, item in enumerate(sorted_data.track_name):
  name = item
  item = split("–|-", item)
  if item[0][-1:] == " ":
    item = item[0][:-1]
  else:
    item = item[0]
  twitter = tweepy.Client(bearer_token=twitter_token)
  search = twitter.search_recent_tweets(item, max_results=100)
  dict = {
    "id": sorted_data.id.iloc[index],
    "track_name": name,
    "size_bytes": sorted_data.size_bytes.iloc[index],
    "prime_genre": sorted_data.prime_genre.iloc[index],
    "n_citacoes": search.meta["result_count"]
  }
  result.append(dict)
result_df = pandas.DataFrame(result)
result_df.to_csv("Result.csv", sep=',', index=False)
result_df.to_json('Result.json', orient="records")
con = db.connect('Result.sqlite')
result_df.to_sql(name='Result', con=con)
con.close()
