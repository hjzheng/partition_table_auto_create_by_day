CREATE TABLE partition_and_purge_register
(
  table_name character varying(32) NOT NULL,
  purge_duration integer,
  is_partition character varying(1),
  last_run_time timestamp(6) without time zone,
  id bigserial NOT NULL,
  CONSTRAINT partition_and_purge_register_pkey PRIMARY KEY (table_name)
);

CREATE TABLE example_table
(
  SAMPLING_TIME timestamp NOT NULL,
  NAME character varying(32),
  DESCRIPTION character varying(128),
  ID bigserial NOT NULL,
  CONSTRAINT example_table_pkey PRIMARY KEY (SAMPLING_TIME, ID)
);


CREATE OR REPLACE FUNCTION create_partitions()
  RETURNS void AS
$BODY$
   declare 
    v_sql_str varchar(1024);
    v_table_name varchar(128);
    v_partition_name varchar(128);
    v_current_date varchar(16);
    v_table_oid INT;
    v_pk_oid INT;
    v_pk_str varchar(1024) := '-';
    v_pk_name varchar(128);
    v_index_oid INT;
    v_index_str varchar(1024)= '-';
    v_index_name varchar(128);
    v_count INT := 0;
    v_rule_str varchar(1024) := '-';
    v_rule_name varchar(128);
       
    c_purge_register record;    --cursor variable of purge register
    c_pk_column record;
    c_indexs record;
    c_index_column record;
    c_rule_column record;
  BEGIN
       --init
       --select to_char(current_date,'YYYYMMDD') into v_current_date;
       --v_current_date := p_current_date;
       
       FOR c_purge_register IN 
        SELECT * FROM partition_and_purge_register WHERE is_partition = 'Y'
        LOOP
          v_table_name := c_purge_register.table_name;
          -- get date of new partition
          SELECT max(substr(relname, length(v_table_name)+2)) INTO v_current_date FROM pg_class WHERE relfilenode IN
            (SELECT b.inhrelid FROM pg_class a join pg_inherits b ON a.relfilenode = b.inhparent WHERE a.relname = v_table_name);
          v_current_date := to_char(to_date(v_current_date,'YYYYMMDD') + 1,'YYYYMMDD');
          -- create table
          v_partition_name := v_table_name || '_' ||   v_current_date;
          v_sql_str := 'CREATE TABLE ' || v_partition_name || '(
  		CHECK (date_trunc( ''day'', SAMPLING_TIME) = DATE '''|| v_current_date ||''' )
			) INHERITS ('||v_table_name||');';
	  execute v_sql_str;

          -- get OID of table name
          SELECT oid INTO v_table_oid FROM pg_class WHERE relname = v_table_name;
                
          -- handle PK
          v_pk_str := '-';
          SELECT conindid INTO v_pk_oid FROM pg_constraint WHERE conrelid = v_table_oid AND contype = 'p';
          FOR c_pk_column IN 
            SELECT attname FROM pg_attribute WHERE attrelid = v_pk_oid
            LOOP
               v_pk_str := v_pk_str || ', ' ||c_pk_column.attname;
            END LOOP;

          IF v_pk_str != '-'
           THEN
	     SELECT SUBSTR(v_pk_str,3) INTO v_pk_str;  
	     v_pk_name := v_partition_name ||'_pkey';
	     v_sql_str := 'ALTER TABLE '||v_partition_name||' ADD CONSTRAINT '||v_pk_name||' primary key ('||v_pk_str||');';
	     execute v_sql_str;           
          END IF;


          
          --handle index
          v_count := 0;
          FOR c_indexs IN SELECT indexrelid FROM pg_index WHERE indrelid = v_table_oid AND indisprimary != 't'
           LOOP
             v_index_str := '-';
             v_index_oid := c_indexs.indexrelid;
             FOR c_index_column IN SELECT attname FROM pg_attribute WHERE attrelid = v_index_oid
              LOOP
                v_index_str := v_index_str ||', '||c_index_column.attname;
              END LOOP;
             SELECT SUBSTR(v_index_str,3) INTO v_index_str;  
             
             v_index_name := v_partition_name||'_index_'||v_count;
             v_sql_str := 'CREATE INDEX '||v_index_name||' ON '||v_partition_name||' ('||v_index_str||');';
             execute v_sql_str;
             v_count := v_count + 1;
           END LOOP;

          -- handle rule
          v_rule_str := '-';
          FOR c_rule_column IN SELECT attname FROM pg_attribute WHERE attrelid = v_table_oid AND attname NOT IN ('tableoid','cmax','xmax','cmin','xmin','ctid')
           LOOP
             v_rule_str := v_rule_str ||', '||'NEW.'||c_rule_column.attname;
           END LOOP;
          SELECT SUBSTR(v_rule_str,3) INTO v_rule_str;  

          v_rule_name := v_partition_name||'_insert';
          v_sql_str := 'CREATE RULE '||v_rule_name||' AS
			ON INSERT TO '||v_table_name||' WHERE
			( date_trunc( ''day'', SAMPLING_TIME) = DATE '''|| v_current_date ||''')
			DO INSTEAD
			INSERT INTO '||v_partition_name||' VALUES ('||v_rule_str||')';
	  execute v_sql_str;
           
        END LOOP;
  END;

$BODY$
  LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION purge_data()
  RETURNS void AS
$BODY$
   declare        
    c_purge_register record;    --cursor variable of purge register
    c_partitions  record;

    v_date_purge varchar(8);
    v_table_name varchar(128);
    v_partition_name varchar(128);
    v_sql_str varchar(1024);
    v_suffix_str varchar(128);
    v_aggregated_date timestamp;
    v_aggregated_table_name varchar(128);
    v_temp_str varchar(128);
    v_temp_int int;
  BEGIN
     -- begin purge data on loop
       FOR c_purge_register IN 
        SELECT * FROM partition_and_purge_register
        LOOP
          -- Get the date on which data should be purged.
          select to_char(current_date - c_purge_register.purge_duration ,'YYYYMMDD') into v_date_purge;
          v_table_name := c_purge_register.table_name;

          IF c_purge_register.is_partition = 'Y'
           THEN
             -- If the table is partition one, need to drop all partitions before now - duration
             -- Query all partitions of the table
             FOR c_partitions IN SELECT b.* FROM pg_class a join pg_inherits b ON a.relfilenode = b.inhparent WHERE a.relname = v_table_name
              LOOP
                SELECT relname INTO v_partition_name FROM pg_class WHERE relfilenode = c_partitions.inhrelid;
                SELECT substr(v_partition_name,length(v_table_name)+2) INTO v_suffix_str;
                IF to_date(v_suffix_str,'YYYYMMDD') <= to_date(v_date_purge,'YYYYMMDD')  
                 THEN
                   v_sql_str := 'DROP TABLE '||v_partition_name||' CASCADE;';
                   execute v_sql_str;
                END IF;
              END LOOP;
          ELSE
             -- If not a partition one, only simple delete the data before now - duration
             v_sql_str := 'DELETE FROM '||v_table_name||' WHERE date_trunc(''day'', SAMPLING_TIME) <= date '''||v_date_purge||''';';
             execute v_sql_str;
          END IF;

          -- Update partition_and_purge_register
          UPDATE partition_and_purge_register SET last_run_time = now() WHERE table_name = v_table_name; 
        END LOOP;
  END;

$BODY$
  LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION init_create_partitions()
  RETURNS void AS
$BODY$
   declare 
    v_current_date varchar(16);

  BEGIN
    -- Create ten days partitions from now on
    FOR v_i IN 1..10
     LOOP
       SELECT to_char(current_date + (v_i-1),'YYYYMMDD') INTO v_current_date;
       perform create_partitions(v_current_date);
     END LOOP;
    
  END;

$BODY$
  LANGUAGE plpgsql;
  
CREATE OR REPLACE FUNCTION create_partitions(p_current_date character varying)
  RETURNS void AS
$BODY$
   declare 
    v_sql_str varchar(1024);
    v_table_name varchar(128);
    v_partition_name varchar(128);
    v_current_date varchar(16);
    v_table_oid INT;
    v_pk_oid INT;
    v_pk_str varchar(1024) := '-';
    v_pk_name varchar(128);
    v_index_oid INT;
    v_index_str varchar(1024)= '-';
    v_index_name varchar(128);
    v_count INT := 0;
    v_rule_str varchar(1024) := '-';
    v_rule_name varchar(128);
       
    c_purge_register record;    --cursor variable of purge register
    c_pk_column record;
    c_indexs record;
    c_index_column record;
    c_rule_column record;
  BEGIN
       --init
       --select to_char(current_date,'YYYYMMDD') into v_current_date;
       v_current_date := p_current_date;
       
       FOR c_purge_register IN 
        SELECT * FROM partition_and_purge_register WHERE is_partition = 'Y'
        LOOP
          -- create table
          v_table_name := c_purge_register.table_name;
          v_partition_name := v_table_name || '_' ||   v_current_date;
          v_sql_str := 'CREATE TABLE ' || v_partition_name || '(
			CHECK (date_trunc( ''day'', SAMPLING_TIME) = DATE '''|| v_current_date ||''' )
			) INHERITS ('||v_table_name||');';
	  execute v_sql_str;

          -- get OID of table name
          SELECT oid INTO v_table_oid FROM pg_class WHERE relname = v_table_name;
                
          -- handle PK
          v_pk_str := '-';
          SELECT conindid INTO v_pk_oid FROM pg_constraint WHERE conrelid = v_table_oid AND contype = 'p';
          FOR c_pk_column IN 
            SELECT attname FROM pg_attribute WHERE attrelid = v_pk_oid
            LOOP
               v_pk_str := v_pk_str || ', ' ||c_pk_column.attname;
            END LOOP;

          IF v_pk_str != '-'
           THEN
	     SELECT SUBSTR(v_pk_str,3) INTO v_pk_str;  
	     v_pk_name := v_partition_name ||'_pkey';
	     v_sql_str := 'ALTER TABLE '||v_partition_name||' ADD CONSTRAINT '||v_pk_name||' primary key ('||v_pk_str||');';
	     execute v_sql_str;           
          END IF;


          
          --handle index
          v_count := 0;
          FOR c_indexs IN SELECT indexrelid FROM pg_index WHERE indrelid = v_table_oid AND indisprimary != 't'
           LOOP
             v_index_str := '-';
             v_index_oid := c_indexs.indexrelid;
             FOR c_index_column IN SELECT attname FROM pg_attribute WHERE attrelid = v_index_oid
              LOOP
                v_index_str := v_index_str ||', '||c_index_column.attname;
              END LOOP;
             SELECT SUBSTR(v_index_str,3) INTO v_index_str;  
             
             v_index_name := v_partition_name||'_index_'||v_count;
             v_sql_str := 'CREATE INDEX '||v_index_name||' ON '||v_partition_name||' ('||v_index_str||');';
             execute v_sql_str;
             v_count := v_count + 1;
           END LOOP;

          -- handle rule
          v_rule_str := '-';
          FOR c_rule_column IN SELECT attname FROM pg_attribute WHERE attrelid = v_table_oid AND attname NOT IN ('tableoid','cmax','xmax','cmin','xmin','ctid')
           LOOP
             v_rule_str := v_rule_str ||', '||'NEW.'||c_rule_column.attname;
           END LOOP;
          SELECT SUBSTR(v_rule_str,3) INTO v_rule_str;  

          v_rule_name := v_partition_name||'_insert';
          v_sql_str := 'CREATE RULE '||v_rule_name||' AS
			ON INSERT TO '||v_table_name||' WHERE
			( date_trunc( ''day'', SAMPLING_TIME) = DATE '''|| v_current_date ||''')
			DO INSTEAD
			INSERT INTO '||v_partition_name||' VALUES ('||v_rule_str||')';
	  execute v_sql_str;
           
        END LOOP;
  END;

$BODY$
  LANGUAGE plpgsql;
