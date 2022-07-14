CREATE OR REPLACE FUNCTION drms_segment() RETURNS SETOF drmssg AS $$
DECLARE
  ns  RECORD;
  rec drmssg%ROWTYPE;
  next_row REFCURSOR;
BEGIN
  FOR ns IN SELECT name || '.drms_segment' as tn FROM admin.ns order by name LOOP
     OPEN next_row FOR EXECUTE 'SELECT * FROM ' || ns.tn;
     LOOP
       FETCH next_row INTO rec;
       IF NOT FOUND THEN 
          EXIT;
       END IF;
       RETURN NEXT rec;
     END LOOP;
     CLOSE next_row;
  END LOOP; 
  RETURN;
END;
$$
LANGUAGE plpgsql;
