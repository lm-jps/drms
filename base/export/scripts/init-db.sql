-- as the netdrms production user, run this on each export database
DROP TABLE IF EXISTS jsoc.export_pending_reqs CASCADE;

CREATE TABLE jsoc.export_pending_reqs
(
  address      text not null,
  ip_address   text not null,
  request_id   text,
  start_time   timestamp with time zone,
  PRIMARY KEY (address, request_id)
);

-- the export-system code will search on both address and ip_address
CREATE INDEX export_pending_reqs_ip_address ON jsoc.export_pending_reqs (ip_address);

-- since any user can run jsoc_fetch, all users need SELECT, INSERT, and DELETE privileges
GRANT SELECT ON jsoc.export_pending_reqs TO public;
GRANT INSERT ON jsoc.export_pending_reqs TO public;
GRANT DELETE ON jsoc.export_pending_reqs TO public;


-- as the netdrms production user, run this on each export database
DROP TABLE IF EXISTS jsoc.export_pending_max_reqs;

CREATE TABLE jsoc.export_pending_max_reqs
(
  address      text not null,
  max_requests int CHECK (max_requests >= 0),
  PRIMARY KEY  (address)
);

GRANT SELECT ON jsoc.export_pending_max_reqs TO public;
