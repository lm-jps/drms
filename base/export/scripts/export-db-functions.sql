CREATE OR REPLACE FUNCTION drms.image_exists(quality integer) RETURNS boolean AS
$$
DECLARE
  answer boolean;

BEGIN
  -- unless the top bit (bit 31) exists, an image is guaranteed to exist
  answer := (quality::bit(32) & X'80000000')::int = 0;
  RETURN answer;
END;
$$
LANGUAGE plpgsql;

-- assumes shadow table exists
CREATE OR REPLACE FUNCTION drms.get_linked_records(is_dynamic boolean, child_series varchar(64), link_columns varchar(32)[], link_column_values varchar(64)[], is_set boolean) RETURNS bigint AS
$$
DECLARE
  prime_key_column_list text;
  command text;
  where_clause text;
  child_recnum bigint;

BEGIN
  child_recnum = NULL;

  SELECT array_to_string(link_columns, ',') INTO prime_key_column_list;
  -- prime_key_column_list := array_to_string(',', link_columns);

  IF is_dynamic THEN
    -- dynamic
    IF NOT is_set THEN
      -- exit without returning a record number
      RETURN NULL;
    END IF;

    where_clause := '';
    FOR column_index IN 1..array_length(link_columns, 1)
    LOOP
      IF column_index > 1 THEN
        where_clause := where_clause || ' AND ';
      END IF;
      where_clause := where_clause || link_columns[column_index]::varchar || ' = ' || E'\'' || link_column_values[column_index] || E'\'';
    END LOOP;

    -- command := 'SELECT recnum INTO ' || child_recnum || ' FROM ' || child_series || '_shadow' || ' WHERE ' || where_clause;
    command := 'SELECT recnum FROM ' || child_series || '_shadow' || ' WHERE ' || where_clause;
    EXECUTE command INTO child_recnum;

    IF FOUND THEN
      RETURN child_recnum;
    ELSE
      RETURN NULL;
    END IF;
  ELSE
    -- static
    command := 'SELECT recnum INTO ' || child_recnum || ' FROM ' || child_series || '_shadow' || ' WHERE recnum = ' || link_column_values[0];
    IF FOUND THEN
      IF NOT child_recnum = -1 THEN
        -- exit without returning a record number
        RETURN NULL;
      END IF;

      RETURN child_recnum;
    ELSE
      RETURN NULL;
    END IF;
  END IF;

  RETURN child_recnum;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION drms.create_manifest(series varchar(64)) RETURNS boolean AS
$$
DECLARE
  command text;
  number_rows int;
  series_ns_table varchar(64);
  series_array varchar(64)[];
  series_namespace varchar(64);
  shadow_table varchar(64);
  manifest_table varchar(64);
  first boolean;
  segment_record RECORD;
  segment_index integer;
  segment_columns varchar(64)[];
  segment_columns_def_list text;
BEGIN
  series_ns_table := lower(series);
  series_array := regexp_split_to_array(series, '[.]');
  series_namespace := lower(series_array[1]);
  shadow_table := lower(series_array[2]) || '_shadow';
  manifest_table := lower(series_array[2]) || '_manifest';

  -- does manifest table exist
  command := 'SELECT count(*) FROM pg_class PGC JOIN pg_namespace PGN ON PGN.oid = PGC.relnamespace WHERE PGN.nspname = ' || E'\'' || series_namespace || E'\'' || ' AND PGC.relname = ' || E'\'' || manifest_table || E'\'';
  EXECUTE command INTO number_rows; -- don't use `FOUND`, it is totally messed up! A query that uses count that returns 0 rows will set FOUND to 't'

  IF number_rows = 1 THEN
    RETURN 'f';
  END IF;

  -- does shadow table exist
  command := 'SELECT count(*) FROM pg_class PGC JOIN pg_namespace PGN ON PGN.oid = PGC.relnamespace WHERE PGN.nspname = ' || E'\'' || series_namespace || E'\'' || ' AND PGC.relname = ' || E'\'' || shadow_table || E'\'';
  EXECUTE command INTO number_rows; -- don't use `FOUND`, it is totally messed up! A query that uses count that returns 0 rows will set FOUND to 't'

  IF number_rows = 0 THEN
    RETURN 'f';
  END IF;

  -- for creating manifest
  segment_columns_def_list := '';

  -- get list of segments from drms_segment
  first := 't';
  segment_index := 1;
  FOR segment_record IN EXECUTE 'SELECT * FROM ' || series_namespace || E'.drms_segment WHERE lower(seriesname) = \'' || series_ns_table || E'\'' LOOP
    IF NOT first THEN
      -- for creating manifest
      segment_columns_def_list := segment_columns_def_list || ', ';
    ELSE
      first := 'f';
    END IF;

    -- manifest creation
    segment_columns[segment_index] = lower(segment_record.segmentname);
    segment_columns_def_list := segment_columns_def_list || segment_columns[segment_index] || ' character(1) NOT NULL';
    segment_index := segment_index + 1;
  END LOOP;

  -- create table and indexes
  IF array_length(segment_columns, 1) > 0 THEN
    EXECUTE 'CREATE TABLE ' || series_namespace || '.' || manifest_table || '(recnum bigint PRIMARY KEY, ' || segment_columns_def_list || ')';
    FOR segment_index IN 1..array_length(segment_columns, 1) LOOP
      EXECUTE 'CREATE INDEX ' || manifest_table || '_' || segment_columns[segment_index] || ' ON ' || series_namespace || '.' || manifest_table || '(' || segment_columns[segment_index] || ')';
    END LOOP;
  ELSE
    -- series has no segments, so exports are for keywords only
    EXECUTE 'CREATE TABLE ' || series_namespace || '.' || manifest_table || '(recnum bigint PRIMARY, keywords$ character(1) NOT NULL)';
    EXECUTE 'CREATE INDEX ' || manifest_table || '_keywords ON ' || series_namespace || '.' || manifest_table || '(keywords$)';
  END IF;

  -- does manifest table exist now
  command := 'SELECT count(*) FROM pg_class PGC JOIN pg_namespace PGN ON PGN.oid = PGC.relnamespace WHERE PGN.nspname = ' || E'\'' || series_namespace || E'\'' || ' AND PGC.relname = ' || E'\'' || manifest_table || E'\'';
  EXECUTE command INTO number_rows; -- don't use `FOUND`, it is totally messed up! A query that uses count that returns 0 rows will set FOUND to 't'

  IF number_rows = 0 THEN
    RETURN 'f';
  ELSE
    RETURN 't';
  END IF;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION drms.delete_manifest(series varchar(64)) RETURNS boolean AS
$$
DECLARE
  command text;
  number_rows int;
  series_array varchar(64)[];
  series_namespace varchar(64);
  shadow_table varchar(64);
  manifest_table varchar(64);
BEGIN
  series_array := regexp_split_to_array(series, '[.]');
  series_namespace := lower(series_array[1]);
  manifest_table := lower(series_array[2]) || '_manifest';

  -- does manifest table exist
  command := 'SELECT count(*) FROM pg_class PGC JOIN pg_namespace PGN ON PGN.oid = PGC.relnamespace WHERE PGN.nspname = ' || E'\'' || series_namespace || E'\'' || ' AND PGC.relname = ' || E'\'' || manifest_table || E'\'';
  EXECUTE command INTO number_rows; -- don't use `FOUND`, it is totally messed up! A query that uses count that returns 0 rows will set FOUND to 't'

  IF number_rows = 0 THEN
    RETURN 'f';
  END IF;

  command := 'DROP TABLE ' || series_namespace || '.' || manifest_table;
  EXECUTE command;

  -- does manifest table existnow
  command := 'SELECT count(*) FROM pg_class PGC JOIN pg_namespace PGN ON PGN.oid = PGC.relnamespace WHERE PGN.nspname = ' || E'\'' || series_namespace || E'\'' || ' AND PGC.relname = ' || E'\'' || manifest_table || E'\'';
  EXECUTE command INTO number_rows; -- don't use `FOUND`, it is totally messed up! A query that uses count that returns 0 rows will set FOUND to 't'

  IF number_rows = 0 THEN
    RETURN 't';
  ELSE
    RETURN 'f';
  END IF;
END;
$$
LANGUAGE plpgsql;

DROP TYPE IF EXISTS drms_id_type CASCADE;
CREATE TYPE drms_id_type AS
(
  -- some series have very long names, so use > 64 chars
  drms_id varchar(128)
);

CREATE OR REPLACE FUNCTION drms.get_n_drms_ids(series varchar(64), segments varchar(64)[], number_ids int) RETURNS setof drms_id_type AS
$$
DECLARE
  command text;
  number_rows int;
  series_array varchar(64)[];
  series_namespace varchar(64);
  series_ns_table varchar(64);
  shadow_table varchar(64);
  manifest_table varchar(64);
  first boolean;
  segment_record RECORD;
  select_from_manifest_cmd text;
  make_manifest boolean;
  populate_manifest boolean;
  segment_columns varchar(64)[];
  segment_index integer;
  manifest_record RECORD;
  record_count bigint;
  check_count bigint;
  shadow_record RECORD;
  segment_columns_list text;
  segment_columns_def_list text;
  placeholders text;
  ps_ns text;
  ps_types text;
  max_recnum bigint;
  id_row drms_id_type%ROWTYPE;
BEGIN
  series_array := regexp_split_to_array(series, '[.]');
  series_namespace := lower(series_array[1]);
  series_ns_table := lower(series);
  shadow_table := lower(series_array[2]) || '_shadow';
  manifest_table := lower(series_array[2]) || '_manifest';

  first := 't';

  -- for creating manifest
  segment_columns_def_list := '';

  -- for selecting from manifest
  select_from_manifest_cmd := '';

  -- for prepared statement used to insert into manifest
  segment_columns_list := '';
  placeholders := '';
  ps_ns := '';
  ps_types := '';

  FOR segment_index IN 1..array_length(segments, 1) LOOP
    segment_columns[segment_index] := lower(segments[segment_index]);

    IF NOT first THEN
      -- for creating manifest
      segment_columns_def_list := segment_columns_def_list || ', ';

      -- for selecting from manifest
      select_from_manifest_cmd := select_from_manifest_cmd || ' UNION ';

      -- for prepared statement used to insert into manifest
      segment_columns_list := segment_columns_list || ', ';
      placeholders := placeholders || ', ';
      ps_ns := ps_ns || ', ';
      ps_types := ps_types || ', ';
    ELSE
      first := 'f';
    END IF;

    -- manifest creation
    segment_columns_def_list := segment_columns_def_list || segment_columns[segment_index] || ' character(1) NOT NULL';

    -- selecting from manifest
    select_from_manifest_cmd := select_from_manifest_cmd || 'SELECT recnum, ' || E'\'' || segment_columns[segment_index] || E'\'' || ' AS segment FROM ' || series_namespace || '.' || manifest_table || ' WHERE ' || quote_ident(segment_columns[segment_index]) || E' = \'N\'';

    -- inserting into manifest
    segment_columns_list := segment_columns_list || segment_columns[segment_index];
    placeholders := placeholders || E'$' || (segment_index + 1)::text; -- $1 for recnum
    ps_ns := ps_ns || E'\'N\'';
    ps_types := ps_types || 'character(1)';
  END LOOP;

  make_manifest := 'f';
  populate_manifest := 'f';
  record_count := 0; -- manifest rows

  -- does shadow table exist
  command := 'SELECT count(*) FROM pg_class PGC JOIN pg_namespace PGN ON PGN.oid = PGC.relnamespace WHERE PGN.nspname = ' || E'\'' || series_namespace || E'\'' || ' AND PGC.relname = ' || E'\'' || shadow_table || E'\'';
  EXECUTE command INTO number_rows; -- don't use `FOUND`, it is totally messed up! A query that uses count that returns 0 rows will set FOUND to 't'

  IF number_rows = 0 THEN
    RETURN;
  END IF;

  -- does manifest table exist
  command := 'SELECT count(*) FROM pg_class PGC JOIN pg_namespace PGN ON PGN.oid = PGC.relnamespace WHERE PGN.nspname = ' || E'\'' || series_namespace || E'\'' || ' AND PGC.relname = ' || E'\'' || manifest_table || E'\'';
  EXECUTE command INTO number_rows; -- don't use `FOUND`, it is totally messed up! A query that uses count that returns 0 rows will set FOUND to 't'

  IF number_rows = 0 THEN
    make_manifest := 't';
  END IF;

  IF make_manifest THEN
    -- create table and indexes
    IF array_length(segment_columns, 1) > 0 THEN
      EXECUTE 'CREATE TABLE ' || series_namespace || '.' || manifest_table || '(recnum bigint PRIMARY KEY, ' || segment_columns_def_list || ')';
      FOR segment_index IN 1..array_length(segment_columns, 1) LOOP
        EXECUTE 'CREATE INDEX ' || manifest_table || '_' || segment_columns[segment_index] || ' ON ' || series_namespace || '.' || manifest_table || '(' || segment_columns[segment_index] || ')';
      END LOOP;
    ELSE
      -- series has no segments, so exports are for keywords only
      EXECUTE 'CREATE TABLE ' || series_namespace || '.' || manifest_table || '(recnum bigint PRIMARY, keywords$ character(1) NOT NULL)';
      EXECUTE 'CREATE INDEX ' || manifest_table || '_keywords ON ' || series_namespace || '.' || manifest_table || '(keywords$)';
    END IF;
  ELSE
    -- manifest exists already; count the number of ids that can be generated from manifest; limit to `number_ids`
    EXECUTE 'SELECT count(*) FROM (' || select_from_manifest_cmd || ') AS FOO' INTO check_count;
    IF check_count < number_ids THEN
      populate_manifest := 't';
    ELSE
      FOR manifest_record IN EXECUTE select_from_manifest_cmd || ' ORDER BY recnum, segment LIMIT ' || number_ids::text LOOP
        id_row.drms_id := series_ns_table || ':' || manifest_record.recnum::text || ':' || manifest_record.segment;
        RETURN NEXT id_row;
        record_count := record_count + 1;
      END LOOP;
    END IF;
  END IF;

  IF populate_manifest THEN
    -- not enough ids in manifest file; populate manifest, and make ids
    EXECUTE 'PREPARE populate_manifest_ps (bigint, ' || ps_types || ') AS INSERT INTO ' || series_namespace || '.' || manifest_table || '(recnum, ' || segment_columns_list || ') VALUES (' || E'$1, ' || placeholders || ')'; -- $1 for recnum

    -- determine maximum
    command := 'SELECT max(recnum) FROM ' || series_namespace || '.' || manifest_table;
    EXECUTE command INTO max_recnum;

    IF max_recnum IS NULL THEN
      max_recnum := -1;
    END IF;

    FOR shadow_record IN EXECUTE 'SELECT recnum FROM ' || series_namespace || '.' || shadow_table || ' WHERE recnum > ' || max_recnum::text || ' ORDER BY recnum LIMIT ' || (number_ids * 2)::text LOOP
      -- insert into manifest table
      EXECUTE 'EXECUTE populate_manifest_ps(' || shadow_record.recnum::text || ', ' || ps_ns || ')';
    END LOOP;

    DEALLOCATE populate_manifest_ps;

    -- add `number_ids` - `record_count` DRMS IDs from manifest to return set
    FOR manifest_record IN EXECUTE select_from_manifest_cmd || ' ORDER BY recnum, segment LIMIT ' || (number_ids - record_count)::text LOOP
      id_row.drms_id := series_ns_table || ':' || manifest_record.recnum::text || ':' || manifest_record.segment;
      RETURN NEXT id_row;
      record_count := record_count + 1;
    END LOOP;
  END IF;

  RETURN;
END;
$$
LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION drms.update_drms_id(drms_ids varchar(128)[], new_value character(1), updated_values boolean DEFAULT 'f') RETURNS setof drms_id_type AS
$$
DECLARE
  id_array varchar(128)[];
  manifest_record RECORD;
  id_row drms_id_type%ROWTYPE;
BEGIN
  -- parse id --> lc_series, recnum, lc_segment
  FOR id_index IN 1..array_length(drms_ids, 1) LOOP
    id_array := regexp_split_to_array(drms_ids[id_index], '[:]');

    FOR manifest_record IN EXECUTE 'UPDATE ' || id_array[1] || '_manifest SET ' || id_array[3] || E' = \'' || new_value || E'\' WHERE recnum = ' || id_array[2] || ' RETURNING recnum' LOOP
      IF updated_values THEN
        id_row.drms_id = id_array[1] || ':' || manifest_record.recnum::text || ':' || id_array[3];
        RETURN NEXT id_row;
      END IF;
    END LOOP;

    IF FOUND THEN
      IF NOT updated_values THEN
        id_row.drms_id = 't';
        RETURN NEXT id_row;
      END IF;
    ELSE
      IF NOT updated_values THEN
        id_row.drms_id = 'f';
        RETURN NEXT id_row;
      END IF;
    END IF;

  END LOOP;

  RETURN;

END;
$$
LANGUAGE plpgsql;
