# Create the whitelist table. This is a cache that contains the whitelist file data:
CREATE TABLE drms.whitelist(seriesname text NOT NULL);
ALTER TABLE drms.whitelist ADD CONSTRAINT whitelist_pkey PRIMARY KEY (seriesname);
GRANT SELECT, INSERT, DELETE ON drms.whitelist TO PUBLIC;
