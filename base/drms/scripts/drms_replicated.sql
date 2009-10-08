CREATE OR REPLACE function drms_replicated() RETURNS SETOF rep_item AS $$
	DECLARE 
	     tn RECORD;
	     rec rep_item%ROWTYPE;
             clustercursor REFCURSOR;
	     tabcursor REFCURSOR;
	     query_obj TEXT;
	     q_string TEXT := 'SELECT schemaname from pg_tables where tablename like $qtag$sl_table%$qtag$'; 
--Get Slony schemanames --
  BEGIN
	-- Create a cursor of cluster names --
     FOR tn IN SELECT schemaname as sn from pg_tables where tablename like 'sl_table' ORDER BY schemaname LOOP
        OPEN clustercursor for EXECUTE 'SELECT tab_id, tab_relname, tab_nspname, tab_set, tab_idxname, tab_comment FROM ' || tn.sn || '.sl_table' ;
	LOOP
	    FETCH clustercursor INTO rec;
	    IF NOT FOUND THEN 
	          EXIT;
            END IF;
            RETURN NEXT rec;
        END LOOP;
        CLOSE clustercursor;
     END LOOP;
     RETURN;
END;
$$
LANGUAGE 'plpgsql'; 
