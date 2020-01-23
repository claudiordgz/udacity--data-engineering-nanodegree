This directory contains the files for the project "Data Modeling with Postgres." The project is a set of exercises to learn to create a Database with Tables and a Star Schema and to create scripts that load sample data in these tables using Python. 

## Purpose

Sparkify is providing its users with enriched metadata of who is playing next on a music service. They are pulling artist and song data from an API (or from a data dump) and then use another API to obtain User Play data. There are multiple challenges to this service. The first challenge is having a complete database of all artists and songs, and the second one is matching the log information to the artist's name in their respective tables. 

Current log information contains artists from multiple countries in different languages, as well as extraneous symbols. Also, they have songs with multiple singers represented in long strings with no standard convention. 

Sparkify wants to make sure they can detect anomalies in the artist and song names in their log data, and they also want to keep growing their artists and songs datasets to provide enriched metadata to their users. Their goals are to continue building their data pipeline as they tackle solutions to each of these complex problems.

## Schema Design Justification

Our schema has a single Fact Table that will house the records of the songs played. 

### Fact Table - Songplays

  - `songplay_id BIGSERIAL PRIMARY KEY`: We expect dozens of NextSong events per day. If we assume 12 events per hour, we can have 20,428 years, which is enough time to figure out alternative storage solutions or lifecycle rules for our data. 
  - `start_time timestamp`: Using the built in type in PostgreSQL will allow us not to have to worry about Epoch conversions or Integer overflows. It also sets the stage to encode a Timezone with our column down the road should we have to provide the service in multiple regions. This property could have been a FOREIGN KEY to the ID in times table.
  - `user_id int NOT NULL`: By forcing not null, we make sure our Pipelines sanitize the inputs. 
  - `level varchar`: Denotes whether the play is free or paid.
  - `song_id varchar`: While a `varchar` suffices, this could have been a FOREIGN KEY to the songs table PRIMARY KEY.
  - `artist_id varchar`: While a `varchar` suffices, this could have been a FOREIGN KEY to the artists table PRIMARY KEY.
  - `session_id int`: Denotes the User Session.
  - `location varchar`: US City and the State of Songplay.
  - `user_agent varchar`: Browser User Agent (soon to be deprecated), might be a good idea to revise API.

### Dimension Tables

#### Users

  - `user_id int PRIMARY KEY`: We selected `int` due to the small size of the dataset, but a `bigint` is possibly a better choice.
  - `first_name varchar`
  - `last_name varchar`
  - `gender char(1)`
  - `level varchar`

#### Songs
  - `song_id varchar PRIMARY KEY`: Enforces Pipeline to sanitize input
  - `title varchar`
  - `artist_id varchar`
  - `year int`
  - `duration numeric`

#### Artists
  - `artist_id varchar PRIMARY KEY`: Enforces Pipeline to sanitize input
  - `name varchar`
  - `location varchar`
  - `latitude numeric`
  - `longitude numeric`

#### Times

A lot of information in this table is redundant, and we likely can remove it in favor of storing an ISO String. The ISO String would need to be transformed by every client of the data, but this means encoding all relevant information in a single string. This change would reduce the possibilities of stale or wrong data in case of a pipeline bug. 

We set all properties as `NOT NULL` because they are all based on a Unix Epoch, which means all of them should be valid.

  - `start_time timestamp PRIMARY KEY`
  - `hour int NOT NULL`
  - `day int NOT NULL`
  - `week int NOT NULL`
  - `month int NOT NULL`
  - `year int NOT NULL`
  - `weekday int NOT NULL`

### ETL Justification

By enforcing PRIMARY KEYS that are not nullable, now our pipeline needs to go through the trouble of dealing with blank strings. Checking for them is trivial and makes sure we save valid data into our tables.

We perform an UPSERT on Artists, Songs, and Users data. We tradeoff pipeline performance in exchange for making sure updates to the record are possible. This change would depend on the business needs, but it is entirely plausible that we receive new artist and song metadata (e.g., name change, new aliases, or modern band or artist picture).

In the case of Times INSERT, we DO NOTHING in case of conflict, because all other properties are going to be the same. 

### Analysis

We need to find out how Sparkify is doing on their quality when delivering curated data, the first step is to get a count of how many matches we receive on our songplays that have artist or songs:

`SELECT * FROM songplays WHERE artist_id != 'None' LIMIT 5;`
`SELECT * FROM songplays WHERE song_id != 'None' LIMIT 5;`
We obtain the single same result on both, so we dig further into the artist:

`SELECT title, name FROM (songplays JOIN songs ON songplays.song_id=songs.song_id) JOIN artists ON artists.artist_id=songplays.artist_id LIMIT 5;`

We observe that `Setanta matins` by `Elena` is the only match. What we can do is create a set of tests using dummy data that makes sure that our `song_filter` query can handle quotes and other extraneous characters. That way we can trust our pipeline to do its job without worrying that it will miss a lot of positive matches.

## Project Structure:

The directory structure is as follows:

  - `test.ipynb` displays the first few rows of each table to let you check your database.
  - `create_tables.py` drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.
  - `etl.ipynb` reads and processes a single file from song_data and log_data and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables.
  - `etl.py` reads and processes files from song_data and log_data and loads them into your tables. You can fill this out based on your work in the ETL notebook.
  - `sql_queries.py` contains all your sql queries, and is imported into the last three files above.

To run the Python Files it is recommended to open a console and perform the following:

```
$ python create_tables.py
$ python etl.py
```

