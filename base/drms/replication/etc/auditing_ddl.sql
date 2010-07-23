SET search_path to <namespace>;

CREATE TABLE slony_auditing_metadata (
    id integer NOT NULL,
    schema_table character varying(200) NOT NULL,
    master_count integer NOT NULL,
    slave_count integer NOT NULL,
    match_flag boolean NOT NULL,
    lag_events integer NOT NULL,
    snap_dt timestamp with time zone DEFAULT now()
);


CREATE SEQUENCE slony_auditing_metadata_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;


ALTER SEQUENCE slony_auditing_metadata_id_seq OWNED BY slony_auditing_metadata.id;



CREATE TABLE slony_auditing_status_history (
    id integer NOT NULL,
    st_origin integer NOT NULL,
    st_received integer NOT NULL,
    st_last_event bigint NOT NULL,
    st_last_event_ts timestamp without time zone NOT NULL,
    st_last_received bigint NOT NULL,
    st_last_received_ts timestamp without time zone NOT NULL,
    st_last_received_event_ts timestamp without time zone NOT NULL,
    st_lag_num_events bigint NOT NULL,
    st_lag_time interval NOT NULL,
    snap_dt timestamp with time zone DEFAULT now()
);

CREATE SEQUENCE slony_auditing_status_history_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;


ALTER SEQUENCE slony_auditing_status_history_id_seq OWNED BY slony_auditing_status_history.id;


ALTER TABLE slony_auditing_metadata ALTER COLUMN id SET DEFAULT nextval('slony_auditing_metadata_id_seq'::regclass);


ALTER TABLE slony_auditing_status_history ALTER COLUMN id SET DEFAULT nextval('slony_auditing_status_history_id_seq'::regclass);
