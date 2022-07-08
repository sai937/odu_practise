from os import truncate
from pyspark.sql import SparkSession
from pyspark.sql.functions import split,explode,countDistinct,col,ntile
from pyspark.sql.types import StructType,StructField, StringType
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import NGram


# for memory settings 
conf = pyspark.SparkConf().setAll([('spark.executor.memory', '16g'), ('spark.executor.cores', '4'), ('spark.cores.max', '4'), ('spark.driver.memory','16g')])
sc.stop()
sc = pyspark.SparkContext(conf=conf)

# create spark session 
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("PySpark Read JSON") \
    .getOrCreate()

#QUESTION 1
''' In yelp_academic_dataset_business.json file, it contains information about business, such as
which state the business is located in, average stars achieved by the business. What is the average stars
achieved by all businesses in each state?'''

#question 1 average stars
# read json file from hdfs

df = spark.read.json("yelp_academic_dataset_business.json")
df.groupBy('state').avg('stars').show(truncate=False)  # query for average stars achieved by the business
#Output:
        +-----+------------------+                                                      
        |state|avg(stars)        |
        +-----+------------------+
        |AZ   |3.6469828791342533|
        |SC   |3.46875           |
        |OR   |2.5               |
        |VA   |1.5               |
        |QC   |3.607739665787159 |
        |BC   |1.5               |
        |MI   |3.5               |
        |NV   |3.6547820079828064|
        |WI   |3.5738461538461537|
        |CA   |3.847826086956522 |
        |NE   |2.75              |
        |CT   |4.5               |
        |NC   |3.490812677272167 |
        |VT   |4.0               |
        |MO   |2.5               |
        |IL   |3.3918387413962634|
        |MB   |4.0               |
        |HPL  |4.5               |
        |WA   |2.8               |
        |AL   |3.8333333333333335|
        +-----+------------------+
# No of Rows : 37
#calcuale standard deviation 
df1 = df.groupBy('state').agg({'stars':'stddev'}).show() # What is the standard deviation of stars achieved by all businesses in each state? Definition of standard deviation
#Output:
        +-----+------------------+
        |state|     stddev(stars)|
        +-----+------------------+
        |   AZ|1.0548812640546474|
        |   SC|1.0351592769769198|
        |   OR|               NaN|
        |   VA|               NaN|
        |   QC|0.8911358185781871|
        |   BC|               0.0|
        |   MI|               0.0|
        |   NV| 1.028051111773983|
        |   WI|0.9939052839552023|
        |   CA|1.1622351370673631|
        |   NE|1.7677669529663689|
        |   CT|0.7071067811865476|
        |   NC|1.0302099880152582|
        |   VT|1.4142135623730951|
        |   MO|               NaN|
        |   IL|1.0209812037206174|
        |   MB|               NaN|
        |  HPL|               NaN|
        |   WA|1.4404860290887933|
        |   AL|1.2583057392117916|
        +-----+------------------+
# No of Rows : 37

# question 2.1
# Please create a table/dataframe which contains users and his/her friends. Each rows
# contains one user friend pair

user_df = spark.read.json("yelp_academic_dataset_user.json")
user_df1=user_df.withColumn("friend",split("friends",",")).select('user_id','friend','average_stars')
fin_df = user_df1.select(user_df1.user_id,explode(user_df1.friend).alias('friend'))
fin_df.show(truncate=False)

#Output:
        +----------------------+-----------------------+
        |user_id               |friend                 |
        +----------------------+-----------------------+
        |ntlvfPzc8eglqvk92iDIAw|oeMvJh94PiGQnx_6GlndPQ |
        |ntlvfPzc8eglqvk92iDIAw| wm1z1PaJKvHgSDRKfwhfDg|
        |ntlvfPzc8eglqvk92iDIAw| IkRib6Xs91PPW7pon7VVig|
        |ntlvfPzc8eglqvk92iDIAw| A8Aq8f0-XvLBcyMk2GJdJQ|
        |ntlvfPzc8eglqvk92iDIAw| eEZM1kogR7eL4GOBZyPvBA|
        |ntlvfPzc8eglqvk92iDIAw| e1o1LN7ez5ckCpQeAab4iw|
        |ntlvfPzc8eglqvk92iDIAw| _HrJVzFaRFUhPva8cwBjpQ|
        |ntlvfPzc8eglqvk92iDIAw| pZeGZGzX-ROT_D5lam5uNg|
        |ntlvfPzc8eglqvk92iDIAw| 0S6EI51ej5J7dgYz3-O0lA|
        |ntlvfPzc8eglqvk92iDIAw| woDt8raW-AorxQM_tIE2eA|
        |ntlvfPzc8eglqvk92iDIAw| hWUnSE5gKXNe7bDc8uAG9A|
        |ntlvfPzc8eglqvk92iDIAw| c_3LDSO2RHwZ94_Q6j_O7w|
        |ntlvfPzc8eglqvk92iDIAw| -uv1wDiaplY6eXXS0VwQiA|
        |ntlvfPzc8eglqvk92iDIAw| QFjqxXn3acDC7hckFGUKMg|
        |ntlvfPzc8eglqvk92iDIAw| ErOqapICmHPTN8YobZIcfQ|
        |ntlvfPzc8eglqvk92iDIAw| mJLRvqLOKhqEdkgt9iEaCQ|
        |ntlvfPzc8eglqvk92iDIAw| VKX7jlScJSA-ja5hYRw12Q|
        |ntlvfPzc8eglqvk92iDIAw| ijIC9w5PRcj3dWVlanjZeg|
        |ntlvfPzc8eglqvk92iDIAw| CIZGlEw-Bp0rmkP8M6yQ9Q|
        |ntlvfPzc8eglqvk92iDIAw| OC6fT5WZ8EU7tEVJ3bzPBQ|
        +----------------------+-----------------------+
# No of Rows : 445836

#question 2.2
# Based on the table created in the 1st step, please create a table/dataframe which contains
# users, his/her friends, ratings given by the users, ratings given by their friends. Each row contains one
# user friend pair and their ratings. Please discard user friend pairs that friend's average_stars is not
# available. Output table/dataframe will be like:
df = spark.read.json("yelp_academic_dataset_user.json")
data=df.select('user_id','friends','average_stars')

filter_data=data.select('user_id',split(col("friends"),",").alias("friends"),'average_stars')
user_entries=filter_data.select(filter_data.user_id,explode(filter_data.friends),filter_data.average_stars)
new_data=user_entries.withColumnRenamed("col","friend")
friend_average_stars=new_data.groupBy("friend").avg( "average_stars")
friend_average_stars_data=friend_average_stars.withColumnRenamed("avg(average_stars)","friend_average_stars")
friends_data = new_data.join(friend_average_stars_data, ["friend"])
result=friends_data.select('user_id','friend','average_stars','friend_average_stars')
result.show()
#Output
        +----------------------+-----------------------+-------------+--------------------+
        |user_id               |friend                 |average_stars|friend_average_stars|
        +----------------------+-----------------------+-------------+--------------------+
        |ntlvfPzc8eglqvk92iDIAw|oeMvJh94PiGQnx_6GlndPQ |3.57         |3.57                |
        |ntlvfPzc8eglqvk92iDIAw| wm1z1PaJKvHgSDRKfwhfDg|3.57         |3.57                |
        |ntlvfPzc8eglqvk92iDIAw| IkRib6Xs91PPW7pon7VVig|3.57         |3.866666666666667   |
        |ntlvfPzc8eglqvk92iDIAw| A8Aq8f0-XvLBcyMk2GJdJQ|3.57         |3.57                |
        |ntlvfPzc8eglqvk92iDIAw| eEZM1kogR7eL4GOBZyPvBA|3.57         |3.8985714285714286  |
        |ntlvfPzc8eglqvk92iDIAw| e1o1LN7ez5ckCpQeAab4iw|3.57         |3.57                |
        |ntlvfPzc8eglqvk92iDIAw| _HrJVzFaRFUhPva8cwBjpQ|3.57         |3.57                |
        |ntlvfPzc8eglqvk92iDIAw| pZeGZGzX-ROT_D5lam5uNg|3.57         |3.715               |
        |ntlvfPzc8eglqvk92iDIAw| 0S6EI51ej5J7dgYz3-O0lA|3.57         |3.8255000000000003  |
        |ntlvfPzc8eglqvk92iDIAw| woDt8raW-AorxQM_tIE2eA|3.57         |3.57                |
        |ntlvfPzc8eglqvk92iDIAw| hWUnSE5gKXNe7bDc8uAG9A|3.57         |3.7293548387096775  |
        |ntlvfPzc8eglqvk92iDIAw| c_3LDSO2RHwZ94_Q6j_O7w|3.57         |3.53                |
        |ntlvfPzc8eglqvk92iDIAw| -uv1wDiaplY6eXXS0VwQiA|3.57         |3.682142857142857   |
        |ntlvfPzc8eglqvk92iDIAw| QFjqxXn3acDC7hckFGUKMg|3.57         |3.8214634146341466  |
        |ntlvfPzc8eglqvk92iDIAw| ErOqapICmHPTN8YobZIcfQ|3.57         |3.823333333333333   |
        |ntlvfPzc8eglqvk92iDIAw| mJLRvqLOKhqEdkgt9iEaCQ|3.57         |3.7399999999999998  |
        |ntlvfPzc8eglqvk92iDIAw| VKX7jlScJSA-ja5hYRw12Q|3.57         |3.65                |
        |ntlvfPzc8eglqvk92iDIAw| ijIC9w5PRcj3dWVlanjZeg|3.57         |3.5300000000000002  |
        |ntlvfPzc8eglqvk92iDIAw| CIZGlEw-Bp0rmkP8M6yQ9Q|3.57         |3.7734693877551018  |
        |ntlvfPzc8eglqvk92iDIAw| OC6fT5WZ8EU7tEVJ3bzPBQ|3.57         |3.7424999999999997  |
        +----------------------+-----------------------+-------------+--------------------+
# No of rows : 445836

#question 2.3
# Based on the table/dataframe in the 2nd step, please create a table/dataframe which
# contains users, his/her average stars, average stars of his/her friends's average stars. Please do not
# consider the case that his/her friends' average stars is not available.

#first drop the records if friend_average_Stars are null
result_fin=result.na.drop(subset=["friend_average_stars"])  
res_df = result_fin.select(result.user_id,result.average_stars.alias('user_stars'),result.friend_average_stars.alias('friend_stars'))
res_df.show()
#Output:
        +----------------------+----------+------------------+                          
        |user_id               |user_stars|friend_stars      |
        +----------------------+----------+------------------+
        |ntlvfPzc8eglqvk92iDIAw|3.57      |3.57              |
        |ntlvfPzc8eglqvk92iDIAw|3.57      |3.57              |
        |ntlvfPzc8eglqvk92iDIAw|3.57      |3.866666666666667 |
        |ntlvfPzc8eglqvk92iDIAw|3.57      |3.57              |
        |ntlvfPzc8eglqvk92iDIAw|3.57      |3.8985714285714286|
        |ntlvfPzc8eglqvk92iDIAw|3.57      |3.57              |
        |ntlvfPzc8eglqvk92iDIAw|3.57      |3.57              |
        |ntlvfPzc8eglqvk92iDIAw|3.57      |3.715             |
        |ntlvfPzc8eglqvk92iDIAw|3.57      |3.8255000000000003|
        |ntlvfPzc8eglqvk92iDIAw|3.57      |3.57              |
        |ntlvfPzc8eglqvk92iDIAw|3.57      |3.7293548387096775|
        |ntlvfPzc8eglqvk92iDIAw|3.57      |3.53              |
        |ntlvfPzc8eglqvk92iDIAw|3.57      |3.682142857142857 |
        |ntlvfPzc8eglqvk92iDIAw|3.57      |3.8214634146341466|
        |ntlvfPzc8eglqvk92iDIAw|3.57      |3.823333333333333 |
        |ntlvfPzc8eglqvk92iDIAw|3.57      |3.7399999999999998|
        |ntlvfPzc8eglqvk92iDIAw|3.57      |3.65              |
        |ntlvfPzc8eglqvk92iDIAw|3.57      |3.5300000000000002|
        |ntlvfPzc8eglqvk92iDIAw|3.57      |3.7734693877551018|
        |ntlvfPzc8eglqvk92iDIAw|3.57      |3.7424999999999997|
        +----------------------+----------+------------------+
# No of Rows : 1000

#QUESTION 2.4
# Finally, we can compute the similarity in terms of average stars given by users and their
# friends. We will use UDF function corr which returns the Pearson coefficient of correlation of a pair
# of a numeric columns in the group. Note, pearson correlation only considers linear correlation. For
# details of corr function

#to calculate corr we can directly use pyspark.df.stat.corr
df_corr = res_df.stat.corr("user_stars","friend_stars")
df.corr.show()
#Output:
    correlation value based on stars given by user and their friends is "0.806540544533278"

# No of Rows :  1(since it is a correlation value)

#QUESTION 3.1
# We want to know for each business, how many unique users lef their tips regards to the
# business.

tips_df = spark.read.json("yelp_academic_dataset_tip.json")
t_df = tips_df.groupBy(["business_id","user_id"]).count().alias("counts")
t_df.selectExpr("business_id","count as num_user_tipped").show(truncate=False)
#OUTPUT:
        +----------------------+---------------+                                        
        |business_id           |num_user_tipped|
        +----------------------+---------------+
        |vflFNEBMH1GSSjvZx3ItdA|1              |
        |eU_713ec6fTGNO4BegRaww|3              |
        |AedKwfr0VdNj7Xmx70GW6g|1              |
        |1O0JT3O8TCULYhu-AIziwg|1              |
        |fHM09_y3QX3n4a_bIFbk_w|5              |
        |9Q1ZtzTPFWG4fJiFSko5Xg|1              |
        |K0j_Znzin0jShXVnpvW86A|1              |
        |idS8mJPE5axaMoLgOy8EOw|1              |
        |ffjnm1Es9fuPF1dnWEcWkA|1              |
        |ikaff3b56-CXzXADJ6uY6w|1              |
        |K-uQkfSUTwu5LIwPB4b_vg|1              |
        |vVlMA3fAXrf7pqpwBleE_w|1              |
        |SrIlbndBJKTJcWNF-fLP7Q|1              |
        |FiG8PzWKRYehtPPcPtOStw|1              |
        |KN0gPRzDvA6uVYims2KA0w|2              |
        |XlUgRG9zJsqBKpPztvlpYQ|2              |
        |iSQwKRrLYYi-dI2PpJmJJg|1              |
        |WqjHEdAftag4ixm09POepQ|1              |
        |1ZnVfS-qP19upP_fwOhZsA|1              |
        |GC5wcui5uejRZTi8OCNPkQ|1              |
        +----------------------+---------------+

# No of Rows : 132700

# QUESTION 3.2
# We want to know for each user, how many unique business he/she has provided tip for.
# The table/dataframe should include user_id, number of unique businesses.
t = tips_df.groupBy("user_id").agg(countDistinct("business_id"))
t.show(truncate=False)

# Ouput
        +----------------------+---------------------------+                            
        |user_id               |num_of_unique_businesses   |
        +----------------------+---------------------------+
        |w_aSpcXhoJkgSnsrakNGUg|3                          |
        |fBdD69y-O15tDxy2yXZBUA|12                         |
        |Bb9519xc5RbTOzOGFQ5N_A|5                          |
        |p4rmmIwWI9lu6g3jcpRyDA|1                          |
        |CeryGeSFYDBA0eUJ-ALjEw|81                         |
        |u_JNhtJqjGZcEIxq6xriOw|1                          |
        |GyVQmBDD63ARsr6op0_Rig|20                         |
        |EnkN58MPWLAydD1Q8d1LZQ|58                         |
        |citPXWuVjLyboxWvgsZ2Wg|59                         |
        |7rPv4UTFuB5yGXZBgxs4Kg|1                          |
        |2LCF5RGdZd6fi_u5SMvwnQ|1                          |
        |g9od9es0LnvMY3U1q7-UYw|4                          |
        |U5SyLOejvB1_wLRq_nN2gA|19                         |
        |moLVZ7mR5eIGgRJiwk05sw|26                         |
        |0PVXNfSOV_5Qf6PPks3-NQ|1                          |
        |qEECNP8LGdXcGML-3YIHZQ|35                         |
        |DSQNYvzwZqMsMYHjC0YV-A|9                          |
        |MWZPj5e6K0etFxMTjF5v6Q|3                          |
        |h3Iydnd3iXDD_cAln6HhlQ|42                         |
        |RO78oDy7vbEcOJU8anY5MQ|38                         |
        +----------------------+---------------------------+

# No of Rows : 365869

# QUESTION 4.1
# Utilizing checkin data, please create a new table/dataframe computing how many dates the
# business was checked in at. The table/dataframe should return business_id, number of dates
# (number of checkins).

check_in = spark.read.json("yelp_academic_dataset_checkin.json")
check_df1=check_in.withColumn("date",split("date",","))
check_res = check_df1.select(check_df1.business_id,explode(check_df1.date))
check_fin=check_res.groupBy('business_id').count().withColumnRenamed("count","num_checkin")

# Output
    +--------------------+-----------+                                              
    |         business_id|num_checkin|
    +--------------------+-----------+
    |--9e1ONYQuAa-CB_R...|       2942|
    |-6c_bJblLXUwoWfmY...|          7|
    |-ElDqujEn1u64ynQr...|          2|
    |-Gh9a15ijNii-8rnp...|          1|
    |-I06hkMFrX0KBqu61...|         39|
    |-Qb6U1MotJfpt6OU_...|          1|
    |-TGwtTiieh_JLml5_...|         53|
    |-VAsjhmAbKF3Pb_-8...|         35|
    |-WGjtt88-6zBiIUiK...|          1|
    |-cxD1NimFldATDUsN...|        100|
    |-m8v19CXrGEYr3PAL...|         24|
    |-qL-wX-UuMaHDBA2X...|        115|
    |-r8SvItXXG6_T3mP5...|         19|
    |069TWjwxctY-3X_Ti...|        514|
    |0859wfd1BQHG46Zpw...|        837|
    |09OYbFNrS1n8u5gE6...|         43|
    |09p3b5BCSz2FPvgR-...|          5|
    |0DwMrcy7_X_C_mP8_...|          8|
    |0FWYa5RT_gQOwW3CR...|         10|
    |0XF5tnQeSZO8BG6OE...|         25|
    +--------------------+-----------+

# No of Rows : 175187

# QUESTION  4.2
# Based on the column num_checkin of the table created in the 1st step, we want to
# compute 10th, 20th, 30th, 40th, 50th, 60th, 70th, 80th, 90th percentile. Please use one line's code to
# compute these percentiles.

percentile=check_fin.agg(F.expr("percentile(num_checkin,0.1)").alias("10"),\
F.expr("percentile(num_checkin,0.2)").alias("20"),F.expr("percentile(num_checkin,0.3)").alias("30"),\
F.expr("percentile(num_checkin,0.4)").alias("40"),F.expr("percentile(num_checkin,0.5)").alias("50"),F.expr("percentile(num_checkin,0.6)").alias("60"),\
F.expr("percentile(num_checkin,0.7)").alias("70"),F.expr("percentile(num_checkin,0.8)").alias("80"),F.expr("percentile(num_checkin,0.9)").alias("90"))
percentile.show()

# Output :
    +---+---+---+----+----+----+----+----+-----+                                    
    | 10| 20| 30|  40|  50|  60|  70|  80|   90|
    +---+---+---+----+----+----+----+----+-----+
    |2.0|3.0|6.0|10.0|16.0|27.0|47.0|90.0|224.0|
    +---+---+---+----+----+----+----+----+-----+

# QUESTION 4.3
# Based on the table/dataframe created at step 1, assign a bucket
# number to each business_id based on num_checkin. num_checkin denotes number of dates the
# business was checked in at. Small value of num_check will correspond to small bucket number. The
# result table/dataframe should contain business_id, num_checkin, bucket number. Please use 10 as
# number of bucket

windowSpec = Window.partitionBy("business_id").orderBy("num_checkin")
check_fin.withColumn("bucket_number",ntile(10).over(windowSpec)).show(truncate=False) #multiply with 10

# Output :

+----------------------+-----------+-------------+                              
|business_id           |num_checkin|bucket_number|
+----------------------+-----------+-------------+
|02d6s2cnDKLllR4f3ykPug|303        |1            |
|1Gg8nehKawegUHIfNihgAw|303        |1            |
|1_WfF9pNoNeWe233CeqE1Q|303        |1            |
|3uNVtgao3ACpD8beMvQVbg|303        |1            |
|7v91woy8IpLrqXsRvxj_vw|303        |2            |
|7xHbnGo4MXFkBm6gYSu_xg|303        |2            |
|Avf10pTql_GsSZ122iP3AA|303        |2            |

# No of Rows : 175187

#question 5
#Ngram 
ng_df = spark.read.json("yelp_academic_dataset_review.json")
#convert text column into array
ng_df1 = ng_df.select(split(col("text"),",").alias("NameArray")).drop("text")

ngram = NGram(n=5)
ngram.setInputCol("NameArray")
ngram.setOutputCol("ngrams")
ngramDataFrame = ngram.transform(ng_df1)
ngramDataFrame.select("ngrams").show(truncate=False)