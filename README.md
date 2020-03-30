# Data Engineering Nanodegree (DEND)

This repository contains my notes and projects from Udacity's DEND.

It uses Anaconda and Jupyter Notebooks and was run on WSL.

## Environment

The Environment has been packaged in `dend.yml`.

To recreate:

```
$ conda env create -f <environment-name>.yml
```

To update dependencies:

```
$ conda env update -f <environment-name>.yml --prune
```

## Global Dependencies

The following might be needed depending on the Notebook or Script that you are executing.

### PostgreSQL

Steps might differ on your system but the gist is to create the following:

  1. Database named `studentdb`
  2. User named `student` with password `student`

```
$ sudo apt update
$ sudo apt install postgresql postgresql-contrib
$ sudo adduser student
$ sudo service postgresql start
$ sudo -u postgres psql -c "SELECT version();"
```

And then change the password for student in the db.

### Cassandra

I had to do some tricks to get Cassandra running on WSL but it is entirely possible, it might take a while to start.
