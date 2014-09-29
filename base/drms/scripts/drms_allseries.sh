#!/bin/bash

# Create the allseries table on the master:
psql -h hmidb -p 5432 jsoc -U production -c "CREATE TABLE drms.allseries(dbhost text NOT NULL, dbport integer NOT NULL, dbname text NOT NULL, seriesname text NOT NULL, author text NOT NULL, owner text NOT NULL, unitsize integer NOT NULL, archive integer NOT NULL, retention integer NOT NULL, tapegroup integer NOT NULL, primary_idx text NOT NULL, created text NOT NULL, description text, dbidx text, version text)"
psql -h hmidb -p 5432 jsoc -U production -c "ALTER TABLE drms.allseries ADD CONSTRAINT allseries_pkey PRIMARY KEY (dbhost, dbport, dbname, seriesname)"
psql -h hmidb -p 5432 jsoc -U production -c "CREATE INDEX allseries_dbhost on drms.allseries(dbhost)"
psql -h hmidb -p 5432 jsoc -U production -c "CREATE INDEX allseries_seriesname on drms.allseries(seriesname)"
psql -h hmidb -p 5432 jsoc -U production -c "GRANT SELECT ON drms.allseries TO PUBLIC"

# Copy all the series data from the master drms_series tables to the allseries table:
psql -h hmidb -p 5432 jsoc -U production -c "INSERT INTO drms.allseries(dbhost, dbport, dbname, seriesname, author, owner, unitsize, archive, retention, tapegroup, primary_idx, created, description, dbidx, version) SELECT 'hmidb', '5432', 'jsoc', seriesname, author, owner, unitsize, archive, retention, tapegroup, primary_idx, created, description, dbidx, version FROM drms_series()"

# Create a transfer table on the slave to hold the data to export from slave to master:
psql -h hmidb2 -p 5432 jsoc -U production -c "SELECT 'hmidb2'::text AS dbhost, '5432'::integer AS dbport, 'jsoc'::text AS dbname, seriesname, author, owner, unitsize, archive, retention, tapegroup, primary_idx, created, description, dbidx, version INTO TABLE drms.tmpseries FROM drms_series()"

# Replicate the transfer table on the master:
pg_dump -h hmidb2 -p 5432 jsoc -t drms.tmpseries -U production | psql -h hmidb -p 5432 jsoc -U production

# Copy the data from the master transfer table to the allseries table:
psql -h hmidb -p 5432 jsoc -U production -c "INSERT INTO drms.allseries(dbhost, dbport, dbname, seriesname, author, owner, unitsize, archive, retention, tapegroup, primary_idx, created, description, dbidx, version) SELECT dbhost, dbport, dbname, seriesname, author, owner, unitsize, archive, retention, tapegroup, primary_idx, created, description, dbidx, version FROM drms.tmpseries"

# Drop the transfer tables:
psql -h hmidb -p 5432 jsoc -U production -c "DROP TABLE drms.tmpseries"
psql -h hmidb2 -p 5432 jsoc -U production -c "DROP TABLE drms.tmpseries"
