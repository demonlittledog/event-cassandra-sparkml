from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline

# create the machine-learning model
def create_ml_model(spark):
    # load the train data-set
    df = spark.sql("select * from events.train_data").withColumnRenamed("user_interested", "label")
    # make all fields Double
    df = df.select([col(c).cast(DoubleType()) for c in df.columns])

    # get the feature columns
    feature_columns = list(filter(lambda x: x != 'label' and x != 'event_id' and x != 'user_id', df.columns))
    # transformer
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

    # the random forest classifier 
    rf = RandomForestClassifier(impurity='gini', maxDepth=9, numTrees=30, featureSubsetStrategy='auto', seed=21)
    # the pipeline with 2 stages
    pipeline = Pipeline(stages=[assembler, rf])
    # build the model
    model = pipeline.fit(df)
	# save the model
    model.write().overwrite().save(sys.argv[1])

# write a data-frame to target mysql-table
def writeCopy(df, tbl):
    df.write.format('jdbc') \
      .option('driver', 'com.mysql.jdbc.Driver') \
      .option('url', sys.argv[2]).option('dbtable', tbl) \
      .option('user', sys.argv[3]).option('password', sys.argv[4]) \
      .mode('overwrite') \
      .save()

# set up bi model by copying data from hive to mysql
def setup_bi_model(spark):
    ### load event-attendee
    dfEventAttendees = spark.sql("select event_id, user_id, attend_type from events.event_attendee")
    # write
    writeCopy(dfEventAttendees, 'event_attendee')

    ### load event attendee count
    dfEventAttendeeCount = spark.sql("select event_id, attend_type, attend_count from events.event_attendee_count")
    # write
    writeCopy(dfEventAttendeeCount, 'event_attendee_count')

    ### load event cities
    dfEventCities = spark.sql("select city, level from events.event_cities")
    # write
    writeCopy(dfEventCities, 'event_cities')

    ### load event countries
    dfEventCountries = spark.sql("select country, level from events.event_countries")
    # write
    writeCopy(dfEventCountries, 'event_countries')

    ### load events
    dfEvents = spark.sql("select event_id, start_time, city, state, zip, country, latitude, longitude, user_id from events.events")
    # write
    writeCopy(dfEvents, 'events')

    ### load friend attend summary
    dfFriendAttendSummary = spark.sql("select user_id, event_id, invited_friends_count, attended_friends_count, not_attended_friends_count, maybe_attended_friends_count from events.friend_attend_summary")
    # write
    writeCopy(dfFriendAttendSummary, 'friend_attend_summary')

    ### load locale
    dfLocale = spark.sql("select locale_id, locale from events.locale")
    # write
    writeCopy(dfLocale, 'locale')

    ### load train
    dfTrain = spark.sql("select user_id, event_id, invited, time_stamp, interested from events.train")
    # write
    writeCopy(dfTrain, 'train')

    ### load user attend event count
    dfUserAttendEventCount = spark.sql("select user_id, invited_count, attended_count, not_attended_count, maybe_attended_count from events.user_attend_event_count")
    # write
    writeCopy(dfUserAttendEventCount, 'user_attend_event_count')

    ### load user_event_count
    dfUserEventCount = spark.sql("select user_id, event_count from events.user_event_count")
    # write
    writeCopy(dfUserEventCount, 'user_event_count')

    ### load user_friend
    dfUserFriend = spark.sql("select user_id, friend_id from events.user_friend")
    # write
    writeCopy(dfUserFriend, 'user_friend')

    ### load user_friend_count
    dfUserFriendCount = spark.sql("select user_id, friend_count from events.user_friend_count")
    # write
    writeCopy(dfUserFriendCount, 'user_friend_count')

    ### load users
    dfUsers = spark.sql("select user_id, birth_year, gender, locale, location, time_zone, joined_at from events.users")
    # write
    writeCopy(dfUsers, 'users')



# check if the model-dir is specified
import sys
#if len(sys.argv) < 5:
#    raise ValueError('The model-dir, mysql (jdbc-url, user-name, password) must be specified.')

# create the spark session
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
try:
    # build the recommendation model
    create_ml_model(spark)

    # set up the bi model by transferring data from hive to mysql
    setup_bi_model(spark)
except Exception as e:
    print(e)
# stop the spark session
spark.stop()

