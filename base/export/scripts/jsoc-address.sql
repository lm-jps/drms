-- the returned row-type for the function jsoc.address
CREATE TYPE jsoc.address_info AS
(
	address varchar(256),
	localname varchar(64),
	domainname varchar(256),
	domainid int,
	confirmation varchar(64),
	starttime timestamp with time zone
);

CREATE TYPE jsoc.address_parse AS
(
	localname varchar(64),
	domainname varchar(256)
);

CREATE TYPE jsoc.user_info AS
(
	address varchar(256),
	id int,
	name varchar(64),
	snail_address text
);

-- insert into underlying jsoc.export_addresses and jsoc.export_addressdomains tables
--   if confirmation_in is '' or NULL, then a NULL is inserted in the confirmation column;
--   used by checkAddress.py to insert a row into jsoc.export_addresses;
--   does NOT insert into jsoc.export_user_info;
CREATE OR REPLACE FUNCTION jsoc.address_info_insert(address varchar(256), confirmation_guid varchar(64) DEFAULT NULL) RETURNS SETOF jsoc.address_info AS
$$
DECLARE
	address_localname varchar(64);
	address_domain varchar(256);
	address_domainid integer;
	confirmation_resolved varchar(64);
	parsed varchar(256)[2];
	start_time timestamp with time zone;

BEGIN
	SELECT localname, domainid INTO address_localname, address_domainid FROM jsoc.address_info_get(address);
	IF NOT FOUND THEN
		parsed := regexp_split_to_array(address, E'@');
		address_localname := parsed[1];
		address_domain := parsed[2];

		-- locate domain first, if it is in use already
		SELECT domainid INTO address_domainid FROM jsoc.export_addressdomains WHERE lower(domainname) = lower(address_domain);
		IF NOT FOUND THEN
			-- insert new domain
			SELECT domainid INTO address_domainid FROM nextval(jsoc.export_addressdomains_seq) AS domainid;
			INSERT INTO jsoc.export_addressdomains(domainid, domainname) VALUES (address_domainid, lower(address_domain));
		END IF;

	  -- insert into jsoc.export_addresses
		IF confirmation_guid = '' THEN
			confirmation_resolved := NULL;
		ELSE
			confirmation_resolved := confirmation_guid;
		END IF;

		INSERT INTO jsoc.export_addresses(localname, domainid, confirmation, starttime) VALUES (address_localname, address_domainid, confirmation_resolved, current_timestamp) RETURNING starttime INTO start_time;
		RETURN QUERY SELECT address, address_localname, address_domain, address_domainid, confirmation_resolved, start_time;
	END IF;

	RETURN;
END;
$$
LANGUAGE plpgsql;


-- update underlying jsoc.export_addresses table;
-- the three optional parameters operate like this:
--   if '' is provided, then set that value to NULL in the row; if NULL is provided, or the caller does not provide any value, then do not modify the value
CREATE OR REPLACE FUNCTION jsoc.address_info_update(address varchar(256), confirmation varchar(64) DEFAULT NULL, start_time timestamp with time zone DEFAULT NULL) RETURNS boolean AS
$$
DECLARE
	command text;
	address_localname varchar(64);
	address_domainid integer;
	return_value boolean;

BEGIN
	return_value := FALSE;

	SELECT localname, domainid INTO address_localname, address_domainid FROM jsoc.address_info_get(address);
	IF FOUND THEN
		-- update jsoc.export_addresses (cannot update localname or domainid)
		IF confirmation IS NOT NULL OR start_time IS NOT NULL THEN
			command := 'UPDATE jsoc.export_addresses SET ';

			IF confirmation is NOT NULL THEN
				IF confirmation = '' THEN
					command := command || 'confirmation = NULL';
				ELSE
					command := command || 'confirmation = ' || E'\'' || confirmation || E'\'';
				END IF;

				IF start_time IS NOT NULL THEN
					command := command || ', ';
				END IF;
			END IF;

			IF start_time IS NOT NULL THEN
				IF start_time = '' THEN
					command := command || 'starttime = NULL';
				ELSE
					command := command || 'starttime = ' || E'\'' || start_time || E'\'';
				END IF;
			END IF;

			command := command || ' WHERE localname = ' || E'\'' || address_localname || E'\'' || ' AND domainid = ' || address_domainid;
			EXECUTE command;
			IF NOT FOUND THEN
				RAISE EXCEPTION 'failing updating address';
			END IF;
		END IF;

		return_value := TRUE;
	END IF;

	RETURN return_value;
END;
$$
LANGUAGE plpgsql;


-- delete from underlying jsoc.export_addresses and jsoc.export_addressdomains tables
--   if `case_sensitive` is TRUE, then delete the row in jsoc.export_addresses whose localname matches the
CREATE OR REPLACE FUNCTION jsoc.address_info_delete(address_in varchar(256) DEFAULT NULL, case_sensitive boolean DEFAULT FALSE) RETURNS boolean AS
$$
DECLARE
	rows jsoc.export_addresses%ROWTYPE;
	address_localname varchar(64);
	address_domainid integer;
	return_value boolean;

BEGIN
	return_value := FALSE;
	SELECT localname, domainid INTO address_localname, address_domainid FROM jsoc.address_info_get(address_in);
	IF FOUND THEN
	  -- delete from jsoc.export_addresses
		IF case_sensitive IS TRUE THEN
			DELETE FROM jsoc.export_addresses WHERE localname = address_localname AND domainid = address_domainid;
		ELSE
			DELETE FROM jsoc.export_addresses WHERE lower(localname) = lower(address_localname) AND domainid = address_domainid;
		END IF;

		-- if we deleted a row, then check to see if the domain is still being used by other addresses
		IF NOT FOUND THEN
			RAISE EXCEPTION 'failure deleting address';
		END IF;

		-- delete from jsoc.export_addressdomains if the domain is no longer used
		SELECT * INTO rows FROM jsoc.export_addresses WHERE domainid = address_domainid;
		IF NOT FOUND THEN
			-- domain no longer used
			DELETE FROM jsoc.export_addressdomains WHERE domainid = address_domainid;
			IF NOT FOUND THEN
				RAISE EXCEPTION 'failure deleting address domain';
			END IF;
		END IF;

			return_value := TRUE;
	END IF;

	RETURN return_value;
END;
$$
LANGUAGE plpgsql;

-- returns address, localname, domainname, domainid, confirmation, starttime for address;
-- if address is NULL, empty, or the empty string, then returns rows for all addresses
CREATE OR REPLACE FUNCTION jsoc.address_info_get(address text DEFAULT NULL) RETURNS SETOF jsoc.address_info AS
$$
DECLARE
    row_out jsoc.address_info%ROWTYPE;

BEGIN
	IF (address = '') IS NOT FALSE THEN
		FOR row_out IN SELECT A.localname || '@' || D.domainname, A.localname, D.domainname, A.domainid, A.confirmation, A.starttime FROM jsoc.export_addresses AS A LEFT OUTER JOIN jsoc.export_addressdomains AS D USING (domainid)
		LOOP
			RETURN NEXT row_out;
		END LOOP;
	ELSE
		FOR row_out IN  SELECT A.localname || '@' || D.domainname, A.localname, D.domainname, A.domainid, A.confirmation, A.starttime FROM jsoc.export_addresses AS A LEFT OUTER JOIN jsoc.export_addressdomains AS D USING (domainid) WHERE lower(A.localname || '@' || D.domainname) = lower(address)
		LOOP
			RETURN NEXT row_out;
		END LOOP;
	END IF;

  RETURN;
END;
$$
LANGUAGE plpgsql;


-- add row into jsoc.export_user_info for address;
-- the two optional parameters operate like this:
--   if '' or NULL is provided, then set that value to NULL in the row
CREATE OR REPLACE FUNCTION jsoc.user_info_insert(user_address varchar(256), user_name varchar(64) DEFAULT NULL, user_snail text DEFAULT NULL) RETURNS SETOF jsoc.user_info AS
$$
DECLARE
	address_located varchar(256);
	command text;

BEGIN
	-- make sure the user_address is registered
	SELECT address INTO address_located FROM jsoc.address_info_get(user_address);
	IF NOT FOUND THEN
		RAISE EXCEPTION 'failure inserting user - address not registered';
	END IF;

	SELECT address INTO address_located FROM jsoc.export_user_info WHERE lower(address) = lower(user_address);
	IF NOT FOUND THEN
		command := 'INSERT INTO jsoc.export_user_info(address';

		IF user_name IS NOT NULL THEN
			command := command || ', name';
		END IF;

		IF user_snail IS NOT NULL THEN
			command := command || ', snail_address';
		END IF;

		command := command || ') VALUES (' || E'\'' || user_address || E'\'';

		IF user_name IS NOT NULL THEN
			IF user_name = '' THEN
				command := command || ', NULL';
			ELSE
				command := command || ', ' || E'\'' || user_name || E'\'';
			END IF;
		END IF;

		IF user_snail IS NOT NULL THEN
			IF user_snail = '' THEN
				command := command || ', NULL';
			ELSE
				command := command || ', ' || E'\'' || user_snail || E'\'';
			END IF;
		END IF;

		command := command || ') RETURNING address, id, name, snail_address';
		RETURN QUERY EXECUTE command;
	END IF;

	RETURN;
END;
$$
LANGUAGE plpgsql;


-- update underlying jsoc.export_user_info;
-- the optional parameters operate like this:
--   if '' is provided, then set that value to NULL in the row; if NULL is provided, or the caller does not provide any value, then do not modify the value
CREATE OR REPLACE FUNCTION jsoc.user_info_update(user_address varchar(256), name varchar(64) DEFAULT NULL, snail text DEFAULT NULL) RETURNS boolean AS
$$
DECLARE
	command text;
	address_found varchar(256);
	return_value boolean;

BEGIN
	return_value := FALSE;

	SELECT address INTO address_found FROM jsoc.export_user_info WHERE lower(address) = lower(user_address);
	IF FOUND THEN
		-- update jsoc.export_user_info
		IF name IS NOT NULL OR snail IS NOT NULL THEN
			command := 'UPDATE jsoc.export_user_info SET ';

			IF name IS NOT NULL THEN
				IF name = '' THEN
					command := command || 'name = NULL';
				ELSE
					command := command || 'name = ' || E'\'' || name || E'\'';
				END IF;

				IF snail IS NOT NULL THEN
					command := command || ', ';
				END IF;
			END IF;

			IF snail IS NOT NULL THEN
				IF snail = '' THEN
					command := command || 'snail_address = NULL';
				ELSE
					command := command || 'snail_address = ' || E'\'' || snail || E'\'';
				END IF;
			END IF;

			command := command || ' WHERE lower(address) = lower(' || E'\'' || user_address || E'\'' || ')';
			EXECUTE command;
			IF NOT FOUND THEN
				RAISE EXCEPTION 'failure updating user';
			END IF;
		END IF;

		-- successfully modified user info
		return_value := TRUE;
	END IF;

	RETURN return_value;
END;
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION jsoc.user_info_delete(user_address varchar(256), case_sensitive boolean DEFAULT FALSE) RETURNS boolean AS
$$
DECLARE
	return_value boolean;

BEGIN
	return_value := FALSE;

	-- delete from jsoc.export_user_info
	IF case_sensitive IS TRUE THEN
		DELETE FROM jsoc.export_user_info WHERE address = user_address;
	ELSE
		DELETE FROM jsoc.export_user_info WHERE lower(address) = lower(user_address);
	END IF;

	IF FOUND THEN
		return_value := TRUE;
	END IF;

	RETURN return_value;
END;
$$
LANGUAGE plpgsql;


-- returns address, id, name, snail_address for one or all users
-- if address is NULL, empty, or the empty string, then returns rows for all users
CREATE OR REPLACE FUNCTION jsoc.user_info_get(user_address text DEFAULT NULL) RETURNS SETOF jsoc.user_info AS
$$
DECLARE
    row_out jsoc.user_info%ROWTYPE;

BEGIN
	IF (user_address = '') IS NOT FALSE THEN
		FOR row_out IN SELECT address, id, name, snail_address FROM jsoc.export_user_info
		LOOP
			RETURN NEXT row_out;
		END LOOP;
	ELSE
		FOR row_out IN SELECT address, id, name, snail_address FROM jsoc.export_user_info WHERE lower(address) = lower(user_address)
		LOOP
			RETURN NEXT row_out;
		END LOOP;
	END IF;

  RETURN;
END;
$$
LANGUAGE plpgsql;

-- returns address, id, name, snail_address for one or all users
-- if address is NULL, empty, or the empty string, then returns rows for all users
CREATE OR REPLACE FUNCTION jsoc.user_info_get(user_id integer) RETURNS SETOF jsoc.user_info AS
$$
DECLARE
    row_out jsoc.user_info%ROWTYPE;

BEGIN
	FOR row_out IN SELECT address, id, name, snail_address FROM jsoc.export_user_info WHERE id = user_id
	LOOP
		RETURN NEXT row_out;
	END LOOP;

  RETURN;
END;
$$
LANGUAGE plpgsql;

-- add new export user to jsoc.export_addresses, jsoc.export_addressdomains, and jsoc.export_user_info; used by registerAddress.py during registration finalization
-- to remove confirmation from jsoc.export_addresses and insert into jsoc.export_user_info; if `address` already exists, and `confirmation` is not NULL in the DB row
-- then set `confirmation` to NULL, and insert user information into jsoc.export_user_info
--
-- can also be used to register a new user manually; this will occur if `address` does not exist in jsoc.export_addresses
--
--   the three optional parameters operate like this:
--     if '' or NULL is provided, then set that value to NULL in the row
--   not used by checkAddress.py since full registration requires the user to respond to an email asynchronously
CREATE OR REPLACE FUNCTION jsoc.user_register(user_address varchar(256), user_name varchar(64) DEFAULT NULL, user_snail text DEFAULT NULL) RETURNS boolean AS
$$
DECLARE
	address_located varchar(256);
	existing_confirmation varchar(64);
	confirmation_removed boolean;
	name_resolved varchar(64);
	snail_resolved text;
	row_inserted jsoc.address_info%ROWTYPE;
	return_value boolean;

BEGIN
	return_value := FALSE;

	if user_name = '' THEN
		name_resolved := NULL;
	ELSE
		name_resolved := user_name;
	END IF;

	IF user_snail = '' THEN
		snail_resolved := NULL;
	ELSE
		snail_resolved := user_snail;
	END IF;

	-- check for partially registered address
	SELECT address, confirmation INTO address_located, existing_confirmation FROM jsoc.address_info_get(user_address);
	IF FOUND THEN
		IF existing_confirmation IS NOT NULL AND existing_confirmation != '' THEN
			confirmation_removed := FALSE;

			-- partially registered; finalize registration by removing confirmation
			SELECT updated INTO confirmation_removed FROM jsoc.address_info_update(user_address, '') AS updated;
			IF NOT FOUND THEN
				RAISE EXCEPTION 'failure updating address';
			ELSE
				IF NOT confirmation_removed THEN
					RAISE EXCEPTION 'failure updating address';
				END IF;

				-- confirmation was removed; now insert into jsoc.export_user_info
				SELECT address INTO address_located FROM jsoc.user_info_insert(user_address, name_resolved, snail_resolved);
				IF NOT FOUND THEN
					RAISE EXCEPTION 'failure inserting user';
				END IF;

				return_value := TRUE;
			END IF;
		ELSE
			-- fully registered already; do nothing
			return_value := FALSE;
		END IF;
	ELSE
		-- not registered at all
		SELECT address INTO address_located FROM jsoc.address_info_insert(user_address, '');
		IF NOT FOUND THEN
			RAISE EXCEPTION 'failure inserting address';
		END IF;

		-- inserted into jsoc.export_addresses; now insert into jsoc.export_user_info
		SELECT address INTO address_located FROM jsoc.user_info_insert(user_address, name_resolved, snail_resolved);
		IF NOT FOUND THEN
				RAISE EXCEPTION 'failure inserting user';
		END IF;

		return_value := TRUE;
	END IF;

	RETURN return_value;
END;
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION jsoc.user_unregister(user_address varchar(256)) RETURNS boolean AS
$$
DECLARE
	address_located varchar(256);
	was_deleted boolean;
	return_value boolean;

BEGIN
	return_value := FALSE;

	SELECT address INTO address_located FROM jsoc.address_info_get(user_address);
	IF FOUND THEN
		SELECT deleted INTO was_deleted FROM jsoc.user_info_delete(user_address) AS deleted;
		IF NOT FOUND THEN
			RAISE EXCEPTION 'failure deleting user';
		END IF;

		SELECT deleted INTO was_deleted FROM jsoc.address_info_delete(user_address, FALSE) AS deleted;
		IF NOT FOUND THEN
			RAISE EXCEPTION 'failure deleting address';
		END IF;

		return_value := TRUE;
	END IF;

	RETURN return_value;
END;
$$
LANGUAGE plpgsql;


--table that contains registered export user information (address - text, email address, id - int id to link with `requestor` col in
-- jsoc.export and jsoc.export_new, name - text, user name, snail_address - text, physical mail address)
CREATE TABLE jsoc.export_user_info(address VARCHAR(256), id serial, name VARCHAR(64), snail_address text, PRIMARY KEY (address));
CREATE INDEX export_user_info_id on jsoc.export_user_info(id);
GRANT SELECT ON jsoc.export_user_info TO public;
GRANT INSERT ON jsoc.export_user_info TO apache;
GRANT USAGE ON jsoc.export_user_info_id_seq TO public;

CREATE TABLE jsoc.export_addressdomains(domainid int PRIMARY KEY, domainname varchar(256) NOT NULL UNIQUE);
CREATE INDEX export_addressdomains_domainname on jsoc.export_addressdomains(lower(domainname));
GRANT SELECT ON  jsoc.export_addressdomains TO public;
GRANT INSERT ON  jsoc.export_addressdomains TO apache;

CREATE TABLE jsoc.export_addresses(localname varchar(64) NOT NULL, domainid int NOT NULL, confirmation varchar(64), starttime timestamp with time zone NOT NULL, PRIMARY KEY (localname, domainid), FOREIGN KEY (domainid) REFERENCES jsoc.export_addressdomains(domainid));
CREATE INDEX export_addresses_localname on jsoc.export_addresses(localname);
CREATE INDEX export_addresses_confirmation on jsoc.export_addresses(confirmation);
GRANT SELECT ON jsoc.export_addresses TO public;
GRANT INSERT ON jsoc.export_addresses TO apache;

CREATE SEQUENCE jsoc.export_addressdomains_seq;
GRANT USAGE ON jsoc.export_addressdomains_seq TO public;
