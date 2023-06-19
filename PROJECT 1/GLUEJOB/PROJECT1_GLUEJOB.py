import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, sum, count, when, round, desc
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *

spark = SparkSession.builder.appName("AWS Project 1").getOrCreate()
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Global path variables
match_file = "s3://groupno6/aws-project-1/data/IPL_Matches_2008_2022.csv"
ball_file = "s3://groupno6/aws-project-1/data/IPL_Ball_by_Ball_2008_2022.csv"

output_path = "s3://groupno6/aws-project-1/result1/"

# Preparing DataFrames
match = spark.read.format("csv").option("header", "true").load(match_file).withColumnRenamed("ID", "MATCH_ID")
ball = spark.read.format("csv").option("header", "true").load(ball_file)
match_ball = ball.join(f.broadcast(match), ball.ID == match.MATCH_ID, "inner")
match_ball.createOrReplaceTempView("ALL")

query = """
SELECT ROW_NUMBER() OVER(ORDER BY ROUND(100*(SUM(BATSMAN_RUN)/COUNT(BATSMAN_RUN)), 2) ASC) AS POS,
    BATTER AS Player,
    SUM(
        CASE
            WHEN EXTRA_TYPE IN ("wides", "byes", "legbyes") THEN 0
            ELSE BATSMAN_RUN
        END
    ) AS Runs,
    SUM(
        CASE
            WHEN EXTRA_TYPE IN ("NA") THEN 1
            ELSE 0
        END
    ) AS BF,
    SUM(
        CASE
            WHEN BATSMAN_RUN = 4 AND NON_BOUNDARY = 0 THEN 1
            ELSE 0
        END
    ) AS `4s`,
    SUM(
        CASE
            WHEN BATSMAN_RUN = 6 AND NON_BOUNDARY = 0 THEN 1
            ELSE 0
        END
    ) AS `6s`,
    CASE
        WHEN BATTINGTEAM != TEAM1 THEN TEAM1
        WHEN BATTINGTEAM != TEAM2 THEN TEAM2
    END AS Against,
    Venue,
    Date AS MatchDate,
    SEASON
FROM ALL
GROUP BY ID, BATTINGTEAM, BATTER, TEAM1, TEAM2, Venue, Date, SEASON
HAVING SUM(BATSMAN_RUN) >= 100
ORDER BY POS, Player, Runs, BF, `4s`, `6s`, Against, Venue, MatchDate, SEASON
"""

ans = spark.sql(query).coalesce(1).dropna()

window_spec = Window.partitionBy("SEASON").orderBy(f.asc("Runs"))
ans = ans.withColumn("POS", f.row_number().over(window_spec))
ans = ans.select("POS", "Player", "Runs", "BF", "4s", "6s", "Against", "Venue", "MatchDate", "SEASON")

ans.write.format("csv").option("header", "true").mode("overwrite").partitionBy("SEASON").save(output_path + "requirement_1")

# Script generated for node S3 bucket
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

joined_df = matches_df.join(ball_by_ball_df, on='id')

joined_df = joined_df.filter(joined_df.extra_type != 'wides')

filtered_df = joined_df.select('season', 'batter', 'batsman_run', 'battingteam', 'team1', 'team2', 'venue', 'date')

grouped_df = filtered_df.groupBy("season", "batter", "battingteam", "team1", "team2", "venue", "date") \
                       .agg(sum(col("batsman_run")).alias("Runs"),
                            count("*").alias("BF"),
                            sum(when(col("batsman_run") == 4, 1).otherwise(0)).alias("4s"),
                            sum(when(col("batsman_run") == 6, 1).otherwise(0)).alias("6s")) \
                       .withColumn("Runs", col("Runs").cast("integer"))

grouped_df = grouped_df.withColumn("SR", round((col("Runs") / col("BF") * 100), 2))

grouped_df_with_opponent = grouped_df.withColumnRenamed("batter", "Player") \
                                     .withColumnRenamed("date", "Match Date") \
                                     .withColumn("Against",
                                                 when(col("battingteam") == col("team1"), col("team2"))
                                                 .otherwise(col("team1"))) \
                                     .drop("battingteam", "team1", "team2")



selected_df = grouped_df_with_opponent.select("Player","Runs", "BF", "SR", "4s", "6s", "Against", "venue", "Match Date","season")
ordered_df = selected_df.orderBy(desc("4s"))
window_spec = Window.partitionBy("season").orderBy(desc("4s"))
# Add a new column starting with 1 within each partition
ordered_df = ordered_df.withColumn("POS", F.row_number().over(window_spec))
ordered_df=ordered_df.select("POS","Player","Runs", "BF", "SR", "4s", "6s", "Against", "venue", "Match Date","season")


result_dynamic_frame = DynamicFrame.fromDF(ordered_df, glueContext, "result_dynamic_frame").coalesce(1)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=result_dynamic_frame,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://groupno6/aws-project-1/result2/",
        "partitionKeys": ['season'],
    },
    transformation_ctx="S3bucket_node3",
)
# Global path variables
match_file="s3://groupno6/aws-project-1/data/IPL_Matches_2008_2022.csv"
ball_file="s3://groupno6/aws-project-1/data/IPL_Ball_by_Ball_2008_2022.csv"
output_path="s3://groupno6/aws-project-1/result3/"

# Prepairing DataFrames
match=spark.read.format("csv").option("header", "true").load(match_file).withColumnRenamed("ID", "MATCH_ID")
ball=spark.read.format("csv").option("header", "true").load(ball_file)

match_ball=ball.join(f.broadcast(match), ball.ID==match.MATCH_ID, "inner")
match_ball.createOrReplaceTempView("ALL")
query="""WITH DATA AS(
    SELECT BATTER, ID, COUNT(DISTINCT ID) AS MATCHES, COUNT(DISTINCT INNINGS) AS INNINGS,
    COUNT(DISTINCT INNINGS) - SUM(
        CASE
            WHEN BATTER=PLAYER_OUT THEN 1
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
    FLOOR(SUM(BATSMAN_RUN)/100) AS CS,
    FLOOR(SUM(BATSMAN_RUN)/50) AS FS, SEASON
    FROM ALL
    GROUP BY BATTER, ID, SEASON
)

SELECT ROW_NUMBER() OVER(ORDER BY SUM(RUNS) DESC) AS P, BATTER as Player, SUM(MATCHES) AS Mat, SUM(INNINGS) AS Inns, SUM(NO) AS NO,
SUM(RUNS) AS Runs, MAX(RUNS) AS HS, ROUND((SUM(RUNS)/(SUM(INNINGS)-SUM(NO))), 2) AS AVG,
SUM(BF) BF, CAST((SUM(RUNS)/SUM(BF))*100 AS DECIMAL(10, 2)) AS SR,
SUM(CS) AS CS, SUM(FS) AS FS, SUM(FOUR) AS FOUR, SUM(SIX) AS SIX, SEASON
FROM DATA
GROUP BY BATTER, SEASON
"""
ans=spark.sql(query).coalesce(1).dropna()
window_spec = Window.partitionBy("SEASON").orderBy(desc("Runs"))
# Add a new column starting with 1 within each partition
ans = ans.withColumn("POS", f.row_number().over(window_spec))

# Select the desired columns for the final result
ans = ans.select(
    "POS","Player","Mat","Inns", "NO","Runs","HS","Avg","BF", "SR","CS","FS","FOUR","SIX","SEASON")
ans.write.format("csv").option("header", "true").mode("overwrite").partitionBy("SEASON").save(output_path+"requriment_3")
match_file="s3://groupno6/aws-project-1/data/IPL_Matches_2008_2022.csv"
ball_file="s3://groupno6/aws-project-1/data/IPL_Ball_by_Ball_2008_2022.csv"
output_path="s3://groupno6/aws-project-1/result4/"

# Prepairing DataFrames
match=spark.read.format("csv").option("header", "true").load(match_file).withColumnRenamed("ID", "MATCH_ID")
ball=spark.read.format("csv").option("header", "true").load(ball_file)

match_ball=ball.join(f.broadcast(match), ball.ID==match.MATCH_ID, "inner")
match_ball = match_ball.filter(match_ball.extra_type != 'byes')
match_ball = match_ball.filter(match_ball.extra_type != 'legbyes')
match_ball = match_ball.filter(match_ball.extra_type != 'penality')
match_ball.createOrReplaceTempView("ALL")

query="""
WITH DATA AS(
    SELECT BOWLER, ID, COUNT(DISTINCT ID) AS MATCHES, COUNT(DISTINCT INNINGS) AS INNINGS,
    sum(case when kind in ("caught","caught and bowled","stumped","bowled","lbw") then 1
    ELSE 0
    END) AS WICKETS, 
    sum(TOTAL_RUN) AS RUNS,
    COUNT(DISTINCT OVERS) AS OVERS,
    CASE
        WHEN CAST(SUM(ISWICKETDELIVERY) AS INT)=4 THEN 1
        ELSE 0
    END AS 4H,
    CASE
        WHEN CAST(SUM(ISWICKETDELIVERY) AS INT)>4 THEN 1
        ELSE 0
    END AS 5H, SEASON
    FROM ALL
    GROUP BY BOWLER, ID, SEASON
)

SELECT BOWLER, SUM(MATCHES) AS MATCHES, SUM(INNINGS) AS INNINGS, SUM(WICKETS) AS WICKETS,
SUM(RUNS) AS RUNS, SUM(OVERS) AS OVERS, ROUND((SUM(RUNS)/SUM(OVERS)), 2) AS ECON,
SUM(4H) AS 4W, SUM(5H) AS 5W, SEASON
FROM DATA
GROUP BY BOWLER, SEASON
"""
ans=spark.sql(query).coalesce(1).dropna()
# Define the window specification to add a row number within each partition
window_spec = Window.partitionBy("Season").orderBy(desc("WICKETS"))
# Add a new column starting with 1 within each partition
ans = ans.withColumn("POS", f.row_number().over(window_spec))

# Select the desired columns for the final result
ans = ans.select(
    "POS","BOWLER","MATCHES","INNINGS","WICKETS", "RUNS","OVERS","ECON","4W","5W", "SEASON")

ans.write.format("csv").option("header", "true").mode("overwrite").partitionBy("Season").save(output_path+"requriment_4")
# Global path variables
match_file="s3://groupno6/aws-project-1/data/IPL_Matches_2008_2022.csv"
ball_file="s3://groupno6/aws-project-1/data/IPL_Ball_by_Ball_2008_2022.csv"
output_path="s3://groupno6/aws-project-1/result5/"

# Prepairing DataFrames
match=spark.read.format("csv").option("header", "true").load(match_file).withColumnRenamed("ID", "MATCH_ID")
ball=spark.read.format("csv").option("header", "true").load(ball_file)

match_ball=ball.join(f.broadcast(match), ball.ID==match.MATCH_ID, "inner")
match_ball.createOrReplaceTempView("ALL")
query="""
SELECT ROW_NUMBER() OVER(ORDER BY SUM(
    CASE
        WHEN EXTRA_TYPE = 'NA' THEN CAST(TOTAL_RUN AS INTEGER) 
        ELSE 0
    END
) DESC, SUM(
    CASE
        WHEN ISWICKETDELIVERY = 1 THEN 1
        ELSE 0
    END
) DESC) AS POS,
BOWLER AS PLAYER, COUNT(DISTINCT OVERS) AS OVERS,
SUM(
    CASE
        WHEN EXTRA_TYPE = 'NA' THEN CAST(TOTAL_RUN AS INTEGER) 
        ELSE 0
    END
) AS RUNS,
SUM(
    CASE
        WHEN ISWICKETDELIVERY = 1 THEN 1
        ELSE 0
    END
) AS WICKETS,
CAST(
    COALESCE(
        SUM(
        CASE
            WHEN EXTRA_TYPE = 'NA' THEN 1 
            ELSE 0
        END)/ SUM(
            CASE
                WHEN ISWICKETDELIVERY = 1 THEN 1
                ELSE 0
            END), 0
    )
AS INTEGER)AS SR,
CASE
        WHEN BattingTeam != TEAM1 THEN TEAM2
        WHEN BattingTeam != TEAM2 THEN TEAM1
    END AS Against, VENUE, CAST(DATE AS DATE), SEASON
FROM ALL
GROUP BY ID, BOWLER, BATTINGTEAM, TEAM1, TEAM2, VENUE, DATE, SEASON
"""

ans = spark.sql(query).coalesce(1).dropna()
window_spec = Window.partitionBy("SEASON").orderBy(f.desc("Runs"))
ans = ans.withColumn("POS", f.row_number().over(window_spec))
ans = ans.select(
    "POS","PLAYER","OVERS","Runs","WICKETS","SR","Against","VENUE","DATE","SEASON"
)
ans.write.format("csv").option("header", "true").mode("overwrite").partitionBy("SEASON").save(output_path + "requirement_5")

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://groupno6/aws-project-1/data/IPL_Ball_by_Ball_2008_2022.csv"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

 

# Script generated for node Amazon S3
S3bucket_node2 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://groupno6/aws-project-1/data/IPL_Matches_2008_2022.csv"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1686505926468",
)

 

df1 = S3bucket_node1.toDF()
df2 = S3bucket_node2.toDF()

 

# Join the ball data with match data
df_merged = df1.join(df2, on="ID")
df_merged = df_merged.filter(df_merged.extra_type != 'wides')
df_merged.createOrReplaceTempView("df_merged")
df1.createOrReplaceTempView("ball")
df2.createOrReplaceTempView("match")

 

out_df = spark.sql("""
    WITH result_df AS (
        SELECT
            df_merged.*,
            ROW_NUMBER() OVER (PARTITION BY df_merged.Season, df_merged.Date, df_merged.batter ORDER BY df_merged.batter) AS result_df_ballnumber,
            SUM(df_merged.batsman_run) OVER (PARTITION BY df_merged.Season, df_merged.Date, df_merged.batter ORDER BY df_merged.batter ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_runs
        FROM df_merged
    ),
    cumulative_df AS (
        SELECT
            *,
            CASE WHEN batsman_run = 4 THEN 1 ELSE 0 END AS `4s`,
            CASE WHEN batsman_run = 6 THEN 1 ELSE 0 END AS `6s`,
            CASE WHEN cumulative_runs >= 50 THEN result_df_ballnumber ELSE NULL END AS BF,
            CASE WHEN BattingTeam = Team1 THEN Team2 ELSE Team1 END AS Against
        FROM result_df
    ),
    grouped_df AS (
        SELECT
            Season,
            Date AS `Match Date`,
            batter AS Player,
            Against,
            Venue,
            MIN(BF) AS BF,
            SUM(`4s`) AS `4s`,
            SUM(`6s`) AS `6s`,
            SUM(batsman_run) AS Runs
        FROM cumulative_df
        GROUP BY Season, Date, batter, Against, Venue
        HAVING BF IS NOT NULL
    ),
    ordered_df AS (
        SELECT
            ROW_NUMBER() OVER (PARTITION BY Season ORDER BY BF) AS POS,
            Season,
            Player,
            Runs,
            BF,
            `4s`,
            `6s`,
            Against,
            Venue,
            `Match Date`
        FROM grouped_df
    )
    SELECT
        POS,
             CASE
        WHEN Season = '2020/21' THEN 2020
        WHEN Season = '2007/08' THEN 2008
        WHEN Season = '2009/10' THEN 2010
        ELSE CAST(Season AS INT)
    END AS Season,
        Player,
        Runs,
        BF,
        `4s`,
        `6s`,
        Against,
        Venue,
        date_format(date(`Match Date`), 'dd-MMM-yy') AS MatchDate
    FROM ordered_df
""")

 

out_df = out_df.repartition("Season").coalesce(1)

 

# Convert the dataframe to a dynamic frame
output_dynamic_frame = DynamicFrame.fromDF(out_df, glueContext, "output_dynamic_frame")

 

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=output_dynamic_frame,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://groupno6/aws-project-1/result6/",
        "partitionKeys": ["Season"],
    },
    transformation_ctx="S3bucket_node3",
)
# Script generated for node S3 bucket
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

joined_df = matches_df.join(ball_by_ball_df, on='id')

joined_df = joined_df.filter(joined_df.extra_type != 'wides')

filtered_df = joined_df.select('season', 'batter', 'batsman_run', 'battingteam', 'team1', 'team2', 'venue', 'date')

grouped_df = filtered_df.groupBy("season", "batter", "battingteam", "team1", "team2", "venue", "date") \
                       .agg(sum(col("batsman_run")).alias("Runs"),
                            count("*").alias("BF"),
                            sum(when(col("batsman_run") == 4, 1).otherwise(0)).alias("4s"),
                            sum(when(col("batsman_run") == 6, 1).otherwise(0)).alias("6s")) \
                       .withColumn("Runs", col("Runs").cast("integer"))

grouped_df = grouped_df.withColumn("SR", round((col("Runs") / col("BF") * 100), 2))

grouped_df_with_opponent = grouped_df.withColumnRenamed("batter", "Player") \
                                     .withColumnRenamed("date", "Match Date") \
                                     .withColumn("Against",
                                                 when(col("battingteam") == col("team1"), col("team2"))
                                                 .otherwise(col("team1"))) \
                                     .drop("battingteam", "team1", "team2")



selected_df = grouped_df_with_opponent.select("Player","Runs", "BF", "SR", "4s", "6s", "Against", "venue", "Match Date","season")
ordered_df = selected_df.orderBy(desc("6s"))
window_spec = Window.partitionBy("season").orderBy(desc("6s"))
# Add a new column starting with 1 within each partition
ordered_df = ordered_df.withColumn("POS", F.row_number().over(window_spec))
ordered_df=ordered_df.select("POS","Player","Runs", "BF", "SR", "4s", "6s", "Against", "venue", "Match Date","season")


result_dynamic_frame = DynamicFrame.fromDF(ordered_df, glueContext, "result_dynamic_frame").coalesce(1)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=result_dynamic_frame,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://groupno6/aws-project-1/result7/",
        "partitionKeys": ['season'],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()