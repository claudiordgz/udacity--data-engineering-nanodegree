This directory contains the files for the project "Data Warehouse." The project is a set of exercises to learn to create a Database with Tables using AWS Redshift and to create scripts that load sample data in these tables using S3 and Redshift integrated tooling.

## Purpose

Sparkify is providing its users with enriched metadata of who is playing next on a music service. They are pulling artist and song data from an API (or from a data dump) and then use another API to obtain User Play data. There are multiple challenges to this service. The first challenge is having a complete database of all artists and songs, and the second one is matching the log information to the artist's name in their respective tables. 

Current log information contains artists from multiple countries in different languages, as well as extraneous symbols. Also, they have songs with multiple singers represented in long strings with no standard convention. 

Sparkify wants to make sure they can detect anomalies in the artist and song names in their log data, and they also want to keep growing their artists and songs datasets to provide enriched metadata to their users. Their goals are to continue building their data pipeline as they tackle solutions to each of these complex problems.

## Schema Design Justification

Our schema has a single Fact Table that will house the records of the songs played. 

The Staging Tables were kept straightforward.

### Fact Table - Songplays

  - `songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY`: We expect dozens of NextSong events per day. If we assume 12 events per hour, we can have 20,428 years using Redshift's Integer, which is enough time to figure out alternative storage solutions or lifecycle rules for our data. 
  - `start_time TIMESTAMP`: Using the built-in type in PostgreSQL will allow us not to have to worry about Epoch conversions or Integer overflows. It also sets the stage to encode a Timezone with our column down the road should we have to provide the service in multiple regions. This property could have been a FOREIGN KEY to the ID in times table.
  - `user_id INTEGER NOT NULL`: By forcing not null, we make sure our Pipelines sanitize the inputs. 
  - `level VARCHAR`: Denotes whether the play is free or paid.
  - `song_id VARCHAR NOT NULL`: While a `varchar` suffices, this could have been a FOREIGN KEY to the songs table PRIMARY KEY. We use `NOT NULL` to prevent garbage. 
  - `artist_id VARCHAR NOT NULL`: While a `varchar` suffices, this could have been a FOREIGN KEY to the artists table PRIMARY KEY.
  - `session_id INTEGER`: Denotes the User Session.
  - `location VARCHAR `: US City and the State of Songplay.
  - `user_agent VARCHAR `: Browser User Agent (soon to be deprecated), might be a good idea to revise API.

### Dimension Tables

#### Users

We set all to `NOT NULL`, but the business might want to approve Nullable gender. 

  - `user_id INTEGER NOT NULL SORTKEY PRIMARY KEY`
  - `first_name VARCHAR NOT NULL`
  - `last_name VARCHAR NOT NULL`
  - `gender VARCHAR NOT NULL`
  - `level VARCHAR NOT NULL`

#### Songs
  - `song_id VARCHAR NOT NULL SORTKEY PRIMARY KEY`: Enforces Pipeline to sanitize input
  - `title VARCHAR NOT NULL`
  - `artist_id VARCHAR NOT NULL`
  - `year INTEGER NOT NULL`
  - `duration FLOAT`

#### Artists
  - `artist_id VARCHAR NOT NULL SORTKEY PRIMARY KEY`: Enforces Pipeline to sanitize input
  - `name VARCHAR NOT NULL`
  - `location VARCHAR`
  - `latitude numeric`
  - `longitude numeric`

#### Times

A lot of information in this table is redundant, and we likely can remove it in favor of storing an ISO String. The ISO String would need to be transformed by every client of the data, but this means encoding all relevant information in a single string. This change would reduce the possibilities of stale or wrong data in case of a pipeline bug. 

We set all properties as `NOT NULL` because they are all based on a Unix Epoch, which means all of them should be valid.

  - `start_time timestamp NOT NULL DISTKEY SORTKEY PRIMARY KEY`
  - `hour INTEGER NOT NULL`
  - `day INTEGER NOT NULL`
  - `week INTEGER NOT NULL`
  - `month INTEGER NOT NULL`
  - `year INTEGER NOT NULL`
  - `weekday INTEGER NOT NULL`

### ETL Justification

By enforcing PRIMARY KEYS that are not nullable, now our pipeline needs to go through the trouble of dealing with blank strings. Checking for them is trivial and makes sure we save valid data into our tables.

We drop any duplicated item when inserting on Artist, Song, and User though a better strategy might be to choose to do a CASE and refresh data when a new one comes in.

### Analysis

All analysis queries are available in the `IAC_Analytics.ipynb` notebook.

## Project Structure:

The directory structure is as follows:

  - `IAC_Analytics.ipynb` Creates a Redshift Cluster, and also provides tooling to run Queries on the Created cluster. It also tracks the created infrastructure to delete it when done with data analysis.
  - `create_tables.py` drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.
  - `etl.py` reads and processes files from song_data and log_data and loads them into your tables. You can fill this out based on your work in the ETL notebook.
  - `iac_dwh.cfg` Configuration to setup Redshift Cluster, make sure to change AWS Profile to your user.
  - `dwh.cfg` Configuration to run `etl` and `create_tables` scripts. Make sure to load the password from a file or AWS SSM.
  - `utilities.py` Function to load password from hidden file or from AWS SSM Parameter Store.

To run the Python Files it is recommended to open a console and perform the following:

Create a Redshift Cluster.
Setup Configuration `dwh.cfg`.
Run scripts
```
$ python create_tables.py
$ python etl.py
```

Then analyze data.
