-- Script to create the jsoc.export_md5 database table that is used to track recent export requests. When a new request
-- arrives at the JSOC, the export-system code looks at recent requests to determine if an existing request would
-- satisfy the new request as well (it checks for duplicate requests). If a duplicate request is encountered, and 
-- the original request results are still online, the request ID of the original request is returned to the user.

-- In PostgreSQL, there is no advantage to using the character data type, and in fact the padding required for that type results in a performance hit.
CREATE TABLE  jsoc.export_md5(md5 text NOT NULL, requestid text NOT NULL, exporttime timestamp with time zone NOT NULL);

-- (md5) is the primary key. jsoc_fetch searches for rows with the md5 hash.
ALTER TABLE jsoc.export_md5 ADD CONSTRAINT export_md5_pkey PRIMARY KEY (md5);

-- When jsoc_export_manage processes new export requests, the process may error out, in which case jsoc_export_manage must
-- delete the md5 hash for the failed export. Add an index on requestid - but also add the UNIQUE constraint (which creates 
-- an index as a side effect).
ALTER TABLE jsoc.export_md5 ADD CONSTRAINT export_md5_requestid UNIQUE (requestid);

-- jsoc_fetch deletes old export requests by searching for rows with the export time.
CREATE INDEX export_md5_exporttime ON jsoc.export_md5 USING btree (exporttime);

-- Everybody should be able to see this table.
GRANT SELECT ON jsoc.export_md5 TO public;

-- jsoc_fetch, which is run by DB user apache, inserts new md5 hashes.
GRANT INSERT ON jsoc.export_md5 TO apache;

-- jsoc_fetch,  which is run by DB user apache, deletes old md5 hashes.
GRANT DELETE ON jsoc.export_md5 TO apache;

-- jsoc_fetch could be run manually by the DB user production.
GRANT INSERT ON jsoc.export_md5 TO production;

-- jsoc_export_manage, which is run by DB user production, deletes md5 hashes for exports that have errored-out.
GRANT DELETE ON jsoc.export_md5 TO production;
