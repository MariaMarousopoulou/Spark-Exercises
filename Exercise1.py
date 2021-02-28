from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField, IntegerType


spark = SparkSession.builder.master("local[1]").appName('Exercise1').getOrCreate()

# Create schema for headers
schema = StructType([StructField("user", IntegerType(), nullable=True),
                     StructField("book", StringType(), nullable=True),
                     StructField("rating", IntegerType(), nullable=True)])

# Read csv
# df = spark.read.csv("test.csv",header=False,schema=schema, sep=';')
df = spark.read.csv('BX-Book-Ratings.csv', header=False, schema=schema, sep=';')
df.registerTempTable('BOOKS')


# 1. The ten most popular books (have been rated most times)
popular_top_ten = spark.sql('SELECT book, COUNT(book) FROM BOOKS GROUP BY book ORDER BY COUNT(book) DESC LIMIT 10')
popular_top_ten.show()


# 2. The ten books with the highest average rating
rating_top_ten = spark.sql('SELECT book, AVG(rating) FROM BOOKS GROUP BY book ORDER BY AVG(rating) DESC LIMIT 10')
rating_top_ten.show()


# 3. The average rating of all books
average_rating = spark.sql('Select AVG(rating) FROM BOOKS')
average_rating.show()


# 4. The ten users who have rated most books
users_top_ten = spark.sql('SELECT user, COUNT(book) FROM BOOKS GROUP BY user ORDER BY COUNT(book) DESC LIMIT 10')
users_top_ten.show()