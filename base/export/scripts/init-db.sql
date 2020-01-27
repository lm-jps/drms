-- as the netdrms production user, run this on each export database
DROP TABLE IF EXISTS jsoc.export_pending_reqs CASCADE;

CREATE TABLE jsoc.export_pending_reqs
(
  address      text not null,
  ip_address   text not null,
  request_id   text,
  start_time   timestamp with time zone,
  PRIMARY KEY (address)
);

-- the export-system code will search on both address and ip_address
CREATE INDEX export_pending_reqs_ip_address ON jsoc.export_pending_reqs (ip_address);
