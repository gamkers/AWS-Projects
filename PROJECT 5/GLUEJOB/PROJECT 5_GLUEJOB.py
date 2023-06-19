PROJECT 5 :

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, when, round, desc, lit, floor, expr, countDistinct, row_number, min
from pyspark.sql import functions as F
import pyspark.sql.functions as f

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

 

# Script generated for node Amazon S3
AmazonS3_node1686309256402 = glueContext.create_dynamic_frame.from_catalog(
    database="aws-group-6",
    table_name="ipl_ball_by_ball_2008_2022_csv",
    transformation_ctx="AmazonS3_node1686309256402",
)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="aws-group-6",
    table_name="ipl_matches_2008_2022_csv",
    transformation_ctx="S3bucket_node1",
)
# Script generated for node Change Schema
ChangeSchema_node1686309271115 = ApplyMapping.apply(
    frame=AmazonS3_node1686309256402,
    mappings=[
        ("id", "long", "id", "long"),
        ("innings", "long", "innings", "long"),
        ("overs", "long", "overs", "long"),
        ("ballnumber", "long", "ballnumber", "long"),
        ("batter", "string", "batter", "string"),
        ("bowler", "string", "bowler", "string"),
        ("non-striker", "string", "non-striker", "string"),
        ("extra_type", "string", "extra_type", "string"),
        ("batsman_run", "long", "batsman_run", "long"),
        ("extras_run", "long", "extras_run", "long"),
        ("total_run", "long", "total_run", "long"),
        ("non_boundary", "long", "non_boundary", "long"),
        ("iswicketdelivery", "long", "iswicketdelivery", "long"),
        ("player_out", "string", "player_out", "string"),
        ("kind", "string", "kind", "string"),
        ("fielders_involved", "string", "fielders_involved", "string"),
        ("battingteam", "string", "battingteam", "string"),
    ],
    transformation_ctx="ChangeSchema_node1686309271115",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("id", "long", "id", "long"),
        ("city", "string", "city", "string"),
        ("date", "string", "date", "string"),
        ("season", "string", "season", "string"),
        ("matchnumber", "string", "matchnumber", "string"),
        ("team1", "string", "team1", "string"),
        ("team2", "string", "team2", "string"),
        ("venue", "string", "venue", "string"),
        ("tosswinner", "string", "tosswinner", "string"),
        ("tossdecision", "string", "tossdecision", "string"),
        ("superover", "string", "superover", "string"),
        ("winningteam", "string", "winningteam", "string"),
        ("wonby", "string", "wonby", "string"),
        ("margin", "string", "margin", "string"),
        ("method", "string", "method", "string"),
        ("player_of_match", "string", "player_of_match", "string"),
        ("team1players", "string", "team1players", "string"),
        ("team2players", "string", "team2players", "string"),
        ("umpire1", "string", "umpire1", "string"),
        ("umpire2", "string", "umpire2", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)
matches_df = ChangeSchema_node1686309271115.toDF()
ball_by_ball_df = ApplyMapping_node2.toDF()
joined_df = matches_df.join(ball_by_ball_df, on='ID')
filtered_df = joined_df.select('Season', 'bowler', 'overs', 'isWicketDelivery', 'total_run', 'Date', 'Kind', 'extra_type', 'BattingTeam', 'Venue')
filtered_df = filtered_df.withColumn("isWicketDelivery", F.when(F.col("kind") == "run out", 0).otherwise(F.col("isWicketDelivery")))
filtered_df = filtered_df.withColumn("total_run", F.when(F.col("extra_type") == "byes", 0).otherwise(F.col("total_run")))
filtered_df = filtered_df.withColumn("total_run", F.when(F.col("extra_type") == "legbyes", 0).otherwise(F.col("total_run")))
filtered_df = filtered_df.withColumn("extra_balls", when((col("extra_type") == "wides") | (col("extra_type") == "noballs") | (col("extra_type") == "penalty"), 1).otherwise(0))
filtered_df = filtered_df.withColumn("Dots", when((col("total_run") == 0), 1).otherwise(0))
#filtered_df = filtered_df.withColumn("Dots", when((col("total_run") == 0) & (col("extra_type") == "NA"), 1).otherwise(0))
grouped_df = filtered_df.groupBy("Season", "Date", "bowler", "overs", 'BattingTeam', 'Venue').agg(count(col("overs")).alias("Balls"), \
                                                                                     sum(col("total_run")).cast("integer").alias("Runs"), \
                                                                                     sum(col("isWicketDelivery")).cast("integer").alias("Wkts"), \
                                                                                     sum(col("extra_balls")).cast("integer").alias("extra_balls"), \
                                                                                     sum(col("Dots")).cast("integer").alias("Dots"))

 

grouped_df = grouped_df.withColumn("Maid", when((col("Runs") == 0), 1).otherwise(0))
grouped_df = grouped_df.groupBy("Season", "Date", "bowler", 'BattingTeam', 'Venue').agg(sum(col("Balls")).alias("Balls"),\
                                                                           sum(col("Runs")).alias("Runs") , \
                                                                           sum(col("Wkts")).alias("Wkts") ,\
                                                                           sum(col("extra_balls")).alias("extra_balls"), \
                                                                           sum(col("Dots")).alias("Dots"), \
                                                                           sum(col("Maid")).alias("Maid"))

 

grouped_df = grouped_df.withColumn("Balls", col("Balls") - col("extra_balls"))
grouped_df = grouped_df.groupBy("Season","bowler", "Balls", "Runs","Wkts", 'BattingTeam', 'Venue', 'Date', 'Dots', 'Maid').agg(sum((floor(col("Balls") / 6) + (expr("Balls") % 6) / 10)).alias("Ov"))
grouped_df = grouped_df.withColumnRenamed("bowler", "Player") \
                                     .withColumnRenamed("Date", "Match Date") \
                                     .withColumnRenamed("BattingTeam", "Against")

 

grouped_df = grouped_df.withColumn("SR", round(col("Balls") / col("Wkts"),2))
result_df = grouped_df.select("Season","Player","Ov","Runs","Wkts","Maid","Dots","SR","Against","Venue","Match Date")
result_df = result_df.fillna(0)
result_df = result_df.orderBy(desc("Dots"), "Runs")
# Define the window specification to add a row number within each partition
window_spec = Window.partitionBy("Season").orderBy(desc("Dots"), "Runs")
# Add a new column starting with 1 within each partition
result_df = result_df.withColumn("POS", row_number().over(window_spec))
# Select the desired columns for the final result
result_df = result_df.select(
    "POS","Season","Player","Ov","Runs","Wkts","Maid","Dots","SR","Against","Venue","Match Date"
)
# Define the output path for storing the partitioned data
output_path = "s3://groupno6/aws-project-5/result 1/"
# Write the partitioned data to the output path as a single combined file in each partition folder
result_df.coalesce(1).write.partitionBy("Season").json(output_path, mode="overwrite")

# Global path variables
match_file = "s3://group-11/project-5/data/cricket_csv/IPL_Matches_2008_2022.csv"
ball_file = "s3://group-11/project-5/data/cricket_csv/IPL_Ball_by_Ball_2008_2022.csv"
output_path = "s3://groupno6/aws-project-5/result 2/"

# Preparing DataFrames
match = spark.read.format("csv").option("header", "true").load(match_file).withColumnRenamed("ID", "MATCH_ID")
ball = spark.read.format("csv").option("header", "true").load(ball_file)

match_ball = ball.join(f.broadcast(match), ball.ID == match.MATCH_ID, "inner")
match_ball.createOrReplaceTempView("ALL")

query = """
    WITH DATA AS (
        SELECT BATTER, ID, COUNT(DISTINCT ID) AS MATCHES, COUNT(DISTINCT INNINGS) AS INNINGS,
        COUNT(DISTINCT INNINGS) - SUM(
            CASE
                WHEN BATTER = PLAYER_OUT THEN 1
                ELSE 0
            END
        ) AS NO,
        SUM(
            CASE
                WHEN EXTRA_TYPE IN ("noballs", "NA", "byes", "legbyes", "penalty") THEN 1
                ELSE 0
            END
        ) AS BF,
        SUM(
            CASE
                WHEN BATSMAN_RUN = 4 THEN 1
                ELSE 0
            END
        ) AS FOUR,
        SUM(
            CASE
                WHEN BATSMAN_RUN = 6 THEN 1
                ELSE 0
            END
        ) AS SIX,
        CAST(SUM(BATSMAN_RUN) AS INT) AS RUNS,
        FLOOR(SUM(BATSMAN_RUN) / 100) AS CS,
        FLOOR(SUM(BATSMAN_RUN) / 50) AS FS, SEASON
        FROM ALL
        GROUP BY BATTER, ID, SEASON
    )
    SELECT ROW_NUMBER() OVER (ORDER BY SUM(RUNS) DESC) AS P, BATTER as Player, SUM(MATCHES) AS Mat, SUM(INNINGS) AS Inns, SUM(NO) AS NO,
    SUM(RUNS) AS Runs, MAX(RUNS) AS HS, ROUND((SUM(RUNS) / (SUM(INNINGS) - SUM(NO))), 2) AS AVG,
    SUM(BF) BF, CAST((SUM(RUNS) / SUM(BF)) * 100 AS DECIMAL(10, 2)) AS SR,
    SUM(CS) AS CS, SUM(FS) AS FS, SUM(FOUR) AS FOUR, SUM(SIX) AS SIX, SEASON
    FROM DATA
    GROUP BY BATTER, SEASON
"""

ans = spark.sql(query).coalesce(1).dropna()
window_spec = Window.partitionBy("SEASON").orderBy(f.desc("Runs"))

# Add a new column starting with 1 within each partition
ans = ans.withColumn("POS", f.row_number().over(window_spec))

# Add asterisk (*) to the HS column
ans = ans.withColumn("HS", f.concat(f.col("HS"), f.lit("*")))

# Select the desired columns for the final result
ans = ans.select("POS", "Player", "Mat", "Inns", "NO", "Runs", "HS", "Avg", "BF", "SR", "CS", "FS", "FOUR", "SIX", "SEASON")

ans.write.format("csv").option("header", "true").mode("overwrite").partitionBy("SEASON").save(output_path + "requriment_2")

S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="aws-group-6",
    table_name="ipl_ball_by_ball_2008_2022_csv",
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1686216371280 = glueContext.create_dynamic_frame.from_catalog(
    database="aws-group-6",
    table_name="ipl_matches_2008_2022_csv",
    transformation_ctx="AmazonS3_node1686216371280",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("id", "long", "id", "long"),
        ("innings", "long", "innings", "long"),
        ("overs", "long", "overs", "long"),
        ("ballnumber", "long", "ballnumber", "long"),
        ("batter", "string", "batter", "string"),
        ("bowler", "string", "bowler", "string"),
        ("non-striker", "string", "non-striker", "string"),
        ("extra_type", "string", "extra_type", "string"),
        ("batsman_run", "long", "batsman_run", "long"),
        ("extras_run", "long", "extras_run", "long"),
        ("total_run", "long", "total_run", "long"),
        ("non_boundary", "long", "non_boundary", "long"),
        ("iswicketdelivery", "long", "iswicketdelivery", "long"),
        ("player_out", "string", "player_out", "string"),
        ("kind", "string", "kind", "string"),
        ("fielders_involved", "string", "fielders_involved", "string"),
        ("battingteam", "string", "battingteam", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Change Schema
ChangeSchema_node1686216462725 = ApplyMapping.apply(
    frame=AmazonS3_node1686216371280,
    mappings=[
        ("id", "long", "id", "long"),
        ("city", "string", "city", "string"),
        ("date", "string", "date", "string"),
        ("season", "string", "season", "string"),
        ("matchnumber", "string", "matchnumber", "string"),
        ("team1", "string", "team1", "string"),
        ("team2", "string", "team2", "string"),
        ("venue", "string", "venue", "string"),
        ("tosswinner", "string", "tosswinner", "string"),
        ("tossdecision", "string", "tossdecision", "string"),
        ("superover", "string", "superover", "string"),
        ("winningteam", "string", "winningteam", "string"),
        ("wonby", "string", "wonby", "string"),
        ("margin", "string", "margin", "string"),
        ("method", "string", "method", "string"),
        ("player_of_match", "string", "player_of_match", "string"),
        ("team1players", "string", "team1players", "string"),
        ("team2players", "string", "team2players", "string"),
        ("umpire1", "string", "umpire1", "string"),
        ("umpire2", "string", "umpire2", "string"),
    ],
    transformation_ctx="ChangeSchema_node1686216462725",
)

matches_df = ChangeSchema_node1686216462725.toDF()
ball_by_ball_df = ApplyMapping_node2.toDF()

joined_df = matches_df.join(ball_by_ball_df, on='ID')

filtered_df = joined_df.select('Season', 'bowler', 'overs', 'isWicketDelivery', 'total_run', 'Date', 'Kind', 'extra_type', 'BattingTeam', 'Venue')

filtered_df = filtered_df.withColumn("isWicketDelivery", F.when(F.col("kind") == "run out", 0).otherwise(F.col("isWicketDelivery")))

filtered_df = filtered_df.withColumn("total_run", F.when(F.col("extra_type") == "byes", 0).otherwise(F.col("total_run")))
filtered_df = filtered_df.withColumn("total_run", F.when(F.col("extra_type") == "legbyes", 0).otherwise(F.col("total_run")))

filtered_df = filtered_df.withColumn("extra_balls", when((col("extra_type") == "wides") | (col("extra_type") == "noballs") | (col("extra_type") == "penalty"), 1).otherwise(0))

filtered_df = filtered_df.withColumn("Dots", when((col("total_run") == 0), 1).otherwise(0))
#filtered_df = filtered_df.withColumn("Dots", when((col("total_run") == 0) & (col("extra_type") == "NA"), 1).otherwise(0))

grouped_df = filtered_df.groupBy("Season", "Date", "bowler", "overs", 'BattingTeam', 'Venue').agg(count(col("overs")).alias("Balls"), \
                                                                                     sum(col("total_run")).cast("integer").alias("Runs"), \
                                                                                     sum(col("isWicketDelivery")).cast("integer").alias("Wkts"), \
                                                                                     sum(col("extra_balls")).cast("integer").alias("extra_balls"), \
                                                                                     sum(col("Dots")).cast("integer").alias("Dots"))

grouped_df = grouped_df.withColumn("Maid", when((col("Runs") == 0), 1).otherwise(0))

grouped_df = grouped_df.groupBy("Season", "Date", "bowler", 'BattingTeam', 'Venue').agg(sum(col("Balls")).alias("Balls"),\
                                                                           sum(col("Runs")).alias("Runs") , \
                                                                           sum(col("Wkts")).alias("Wkts") ,\
                                                                           sum(col("extra_balls")).alias("extra_balls"), \
                                                                           sum(col("Dots")).alias("Dots"), \
                                                                           sum(col("Maid")).alias("Maid"))

grouped_df = grouped_df.withColumn("Balls", col("Balls") - col("extra_balls"))

grouped_df = grouped_df.groupBy("Season","bowler", "Balls", "Runs","Wkts", 'BattingTeam', 'Venue', 'Date', 'Dots', 'Maid').agg(sum((floor(col("Balls") / 6) + (expr("Balls") % 6) / 10)).alias("Ov"))

grouped_df = grouped_df.withColumnRenamed("bowler", "Player") \
                                     .withColumnRenamed("Date", "Match Date") \
                                     .withColumnRenamed("BattingTeam", "Against")

grouped_df = grouped_df.withColumn("SR", round(col("Balls") / col("Wkts"),2))
grouped_df = grouped_df.withColumn("Econ", round(((col("Runs") / col("Balls"))*6), 2))

result_df = grouped_df.select("Season","Player","Ov","Runs","Wkts","Dots","Econ","SR","Against","Venue","Match Date")
result_df = result_df.filter(result_df.Ov >= 2)

result_df = result_df.fillna(0)

result_df = result_df.orderBy("Econ")

# Define the window specification to add a row number within each partition
window_spec = Window.partitionBy("Season").orderBy("Econ")

# Add a new column starting with 1 within each partition
result_df = result_df.withColumn("POS", row_number().over(window_spec))

# Select the desired columns for the final result
result_df = result_df.select(
    "POS","Season","Player","Ov","Runs","Wkts","Dots","Econ","SR","Against","Venue","Match Date"
)

# Define the output path for storing the partitioned data
output_path = "s3://groupno6/aws-project-5/result 3/"

# Write the partitioned data to the output path as a single combined file in each partition folder
result_df.coalesce(1).write.partitionBy("Season").json(output_path, mode="overwrite")
# Script generated for node Amazon S3
AmazonS3_node1686309256402 = glueContext.create_dynamic_frame.from_catalog(
    database="aws-group-6",
    table_name="ipl_ball_by_ball_2008_2022_csv",
    transformation_ctx="AmazonS3_node1686309256402",
)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="",
    table_name="ipl_matches_2008_2022_csv",
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Change Schema
ChangeSchema_node1686309271115 = ApplyMapping.apply(
    frame=AmazonS3_node1686309256402,
    mappings=[
        ("id", "long", "id", "long"),
        ("innings", "long", "innings", "long"),
        ("overs", "long", "overs", "long"),
        ("ballnumber", "long", "ballnumber", "long"),
        ("batter", "string", "batter", "string"),
        ("bowler", "string", "bowler", "string"),
        ("non-striker", "string", "non-striker", "string"),
        ("extra_type", "string", "extra_type", "string"),
        ("batsman_run", "long", "batsman_run", "long"),
        ("extras_run", "long", "extras_run", "long"),
        ("total_run", "long", "total_run", "long"),
        ("non_boundary", "long", "non_boundary", "long"),
        ("iswicketdelivery", "long", "iswicketdelivery", "long"),
        ("player_out", "string", "player_out", "string"),
        ("kind", "string", "kind", "string"),
        ("fielders_involved", "string", "fielders_involved", "string"),
        ("battingteam", "string", "battingteam", "string"),
    ],
    transformation_ctx="ChangeSchema_node1686309271115",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("id", "long", "id", "long"),
        ("city", "string", "city", "string"),
        ("date", "string", "date", "string"),
        ("season", "string", "season", "string"),
        ("matchnumber", "string", "matchnumber", "string"),
        ("team1", "string", "team1", "string"),
        ("team2", "string", "team2", "string"),
        ("venue", "string", "venue", "string"),
        ("tosswinner", "string", "tosswinner", "string"),
        ("tossdecision", "string", "tossdecision", "string"),
        ("superover", "string", "superover", "string"),
        ("winningteam", "string", "winningteam", "string"),
        ("wonby", "string", "wonby", "string"),
        ("margin", "string", "margin", "string"),
        ("method", "string", "method", "string"),
        ("player_of_match", "string", "player_of_match", "string"),
        ("team1players", "string", "team1players", "string"),
        ("team2players", "string", "team2players", "string"),
        ("umpire1", "string", "umpire1", "string"),
        ("umpire2", "string", "umpire2", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

matches_df = ChangeSchema_node1686309271115.toDF()
ball_by_ball_df = ApplyMapping_node2.toDF()

joined_df = matches_df.join(ball_by_ball_df, on='ID')

filtered_df = joined_df.select('Season', 'bowler', 'overs', 'isWicketDelivery', 'total_run', 'Date', 'Kind', 'extra_type', 'BattingTeam', 'Venue')

filtered_df = filtered_df.withColumn("isWicketDelivery", F.when(F.col("kind") == "run out", 0).otherwise(F.col("isWicketDelivery")))

filtered_df = filtered_df.withColumn("total_run", F.when(F.col("extra_type") == "byes", 0).otherwise(F.col("total_run")))
filtered_df = filtered_df.withColumn("total_run", F.when(F.col("extra_type") == "legbyes", 0).otherwise(F.col("total_run")))

filtered_df = filtered_df.withColumn("extra_balls", when((col("extra_type") == "wides") | (col("extra_type") == "noballs") | (col("extra_type") == "penalty"), 1).otherwise(0))

grouped_df = filtered_df.groupBy("Season", "Date", "bowler", "overs", 'BattingTeam', 'Venue').agg(count(col("overs")).alias("Balls"), \
                                                                                     sum(col("total_run")).cast("integer").alias("Runs"), \
                                                                                     sum(col("isWicketDelivery")).cast("integer").alias("Wkts"), \
                                                                                     sum(col("extra_balls")).cast("integer").alias("extra_balls"))


grouped_df = grouped_df.groupBy("Season", "Date", "bowler", 'BattingTeam', 'Venue').agg(sum(col("Balls")).alias("Balls"),\
                                                                           sum(col("Runs")).alias("Runs") , \
                                                                           sum(col("Wkts")).alias("Wkts") ,\
                                                                           sum(col("extra_balls")).alias("extra_balls"))

grouped_df = grouped_df.withColumn("Balls", col("Balls") - col("extra_balls"))

grouped_df = grouped_df.groupBy("Season","bowler", "Balls", "Runs","Wkts", 'BattingTeam', 'Venue', 'Date').agg(sum((floor(col("Balls") / 6) + (expr("Balls") % 6) / 10)).alias("Ov"))

grouped_df = grouped_df.withColumnRenamed("bowler", "Player") \
                                     .withColumnRenamed("Date", "Match Date") \
                                     .withColumnRenamed("BattingTeam", "Against")

grouped_df = grouped_df.withColumn("SR", round(col("Balls") / col("Wkts"),2))
grouped_df = grouped_df.filter(grouped_df.SR > 0)

result_df = grouped_df.select("Season","Player","Ov","Runs","Wkts","SR","Against","Venue","Match Date")

result_df = result_df.fillna(0)

result_df = result_df.orderBy("SR",desc("Wkts"))

# Define the window specification to add a row number within each partition
window_spec = Window.partitionBy("Season").orderBy("SR",desc("Wkts"))

# Add a new column starting with 1 within each partition
result_df = result_df.withColumn("POS", row_number().over(window_spec))

# Select the desired columns for the final result
result_df = result_df.select(
    "POS","Season","Player","Ov","Runs","Wkts","SR","Against","Venue","Match Date"
)

# Define the output path for storing the partitioned data
output_path = "s3://groupno6/aws-project-5/result 4/"

# Write the partitioned data to the output path as a single combined file in each partition folder
result_df.coalesce(1).write.partitionBy("Season").json(output_path, mode="overwrite")

job.commit()

 

