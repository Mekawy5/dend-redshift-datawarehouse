  

# Project 3:  Data Warehouse

---

  

## Summary

  

This project is an implementation for ETL pipeline written in python for collecting and transforming Sparkify data from JSON logs on s3 and load them in a start schema analytical database on Redshift to help Sparkify's analytics team to run customized queries for song play analysis for answering business questions based on the data they have.

  

---

  
  

## Data Modeling  

Using the star schema optimized for queries on song play analysis. This includes the following tables:

![ER](https://user-images.githubusercontent.com/20432520/104038432-c6cedc80-51dd-11eb-92e7-b3d30b0a6c5d.jpg)

  
  

#### Fact Table

  

1.  **songplays** - records in log data associated with song plays.

#### Dimension Tables

1.  **users** - users in the app

2.  **songs** - songs in music database

3.  **artists** - artists in music database

4.  **time** - timestamps of records in **songplays** broken down into specific units
---

## Project Files
  
  -   `create_table.py`  used to drop/create your Redshift staging and fact/dimension tables
  
-   `etl.py`  load data from S3 into staging tables on Redshift and then process that data into analytics tables on Redshift.

-   `sql_queries.py`  all SQL statements for the ETL process.

-   `dwh.cfg`  configurations used to connect to our resources.
  

## How To Run

1. create Redshift cluster and IAM role to access S3.
2. rename `dwh.cfg.example` to `dwh.cfg` and fill it with cluster info and IAM role ARN
3. run `python create_tables.py` that will create the tables.
4. run `python etl.py` that will populate the staging tables from s3, then load the data in the fact and dimension tables.

## Questions examples:
---

**the top 5 played songs?**

`SELECT songs.title, count(songplays.songplay_id) FROM songplays JOIN songs ON songplays.song_id = songs.song_id WHERE songplays.song_id != '' GROUP BY songs.title ORDER BY count(songplays.songplay_id) DESC LIMIT 5;`

|title| count |
|--|--|
|  Greece 2000| 55 |
|  You're The One| 37 |
|  Stronger| 28 |
|  Revelry| 27 |
|  Yellow| 24 |


---

**last 5 played song?**

`select songs.title from songs join songplays on songplays.song_id = songs.song_id where songplays.song_id != '' order by songplays.start_time desc limit 5; `

|title |
|--|
|  Anything You Say (Unreleased Version) |
|  The Haunting |
|  Rose |
|  Besos De Ceniza |
|  The Pretender |


---


**how many free and paid users used the app in NY?**

`select songplays.level, count(distinct songplays.user_id) from songplays join users on songplays.user_id = users.user_id where songplays.user_id is not null and songplays.location like '%New York%' group by songplays.level order by count(distinct songplays.user_id) desc; select songplays.level, count(distinct songplays.user_id) from songplays join users on songplays.user_id = users.user_id where songplays.user_id is not null and songplays.location like '%New York%' group by songplays.level order by count(distinct songplays.user_id) desc; `

|level| count |
|--|--|
|  free | 8 |
|  paid | 2 |


---


**top 10 users listened to different songs?**

`select users.first_name, users.last_name, users.gender, users.level, plays.played_songs from users join( select user_id as user_id, count(distinct song_id) as played_songs from songplays group by user_id order by count(distinct song_id) desc limit 10) as plays on users.user_id = plays.user_id;`

|first_name | last_name | gender | level | played_songs|
|--|--|--|--|--|
|Kate | Harrell | F | paid | 524|
|Layla | Griffin | F | paid | 313|
|Aleena | Kirby | F | paid | 407|
|Tegan | Levine | F | paid | 659|
|Matthew | Jones | M | paid | 259|
|Chloe | Cuevas | F | paid | 647|
|Mohammad | Rodriguez | M | paid | 258|
|Lily | Koch | F | free | 468|
|Jacob | Klein | M | paid | 294|
|Jacqueline | Lynch | F | paid | 336|


