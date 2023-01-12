            -- ------------------------------------------------------
            -- Partition Control System - Version 3.0.0
            --
            -- History:
            --
            -- Version 3.0.0 13 October 2022
            -- This is the initial, development release of the 
            -- Partition Control System, based on the Partition Manager,
            -- originally created in December 2010.
            -- ------------------------------------------------------
            --
            -- Thanks:
            -- Special thanks to Adrian Whitwham and Alex Gentile for 
            -- their work on the original Partition Manager.
            --
            -- ------------------------------------------------------
            -- Copyright (c) 2023 David E. Minor
            --
            -- Permission is hereby granted, free of charge, to any person obtaining a copy
            -- of this software and associated documentation files (the "Software"), to deal
            -- in the Software without restriction, including without limitation the rights
            -- to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
            -- copies of the Software, and to permit persons to whom the Software is
            -- furnished to do so, subject to the following conditions:
            --
            -- The above copyright notice and this permission notice shall be included in all
            -- copies or substantial portions of the Software.
            --
            -- THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
            -- IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
            -- FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
            -- AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
            -- LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
            -- OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
            -- SOFTWARE.
            --
            -- ------------------------------------------------------
            -- Initial Load of Partition Control System
            --
            -- Creates pcs_db database, creates event and loads stored procs
            -- Note:  PCS is stored and executed from the pcs_db
            -- database so that any database can use it's functionality without
            -- duplicating procedures and configuration.
            -- 
            -- ------------------------------------------------------     
            
            DELIMITER ;;

            CREATE DATABASE IF NOT EXISTS pcs_db;;
            USE pcs_db;;

            -- ------------------------------------------------------
            --
            -- pcs_event
            --
            -- Calls run_pcs, once a day, at server off peak.
            -- It creates new partitions and deletes old ones, if so configured.
            --
            -- ------------------------------------------------------

            DROP EVENT IF EXISTS pcs_event;;
            CREATE EVENT `pcs_event` ON SCHEDULE EVERY 1 DAY STARTS TIMESTAMP(CONCAT(CURDATE() + INTERVAL 1 DAY, " 00:00:00")) ON COMPLETION PRESERVE ENABLE COMMENT 'Partition Control System' 
            DO 
               BEGIN 
                  DECLARE IsReplica INT DEFAULT 0; 
		  DECLARE MySQL_8 INT DEFAULT 0;

                  INSERT INTO pcs_log (message_type, logging_proc, action_timestamp, message) values
                    ('Info', 'Event', NOW(), 'Starting Event');

		  SELECT cast(substring(version(), 1,1) AS UNSIGNED) INTO MySQL_8;

		  IF (MySQL_8 = 8) THEN
		    SELECT COUNT(1) INTO IsReplica FROM performance_schema.global_status WHERE variable_name = 'Slave_running' AND variable_value = 'ON';	
		  ELSE
                    SELECT COUNT(1) INTO IsReplica FROM information_schema.global_status WHERE variable_name = 'Slave_running' AND variable_value = 'ON'; 
                  END IF;

                  IF (IsReplica = 0) THEN 
                    INSERT INTO pcs_log (message_type, logging_proc, action_timestamp, message) values
                        ('Info', 'Event', NOW(), 'Instance reporting as a writer.  Starting Partition Control System');
                    CALL run_pcs; 
                  ELSE
                    INSERT INTO pcs_log (message_type, logging_proc, action_timestamp, message) values
                        ('Info', 'Event', NOW(), 'Instance reporting as a replica.  Skipping Partition Control System');
                  END IF; 

                  INSERT INTO pcs_log (message_type, logging_proc, action_timestamp, message) values
                      ('Info', 'Event', NOW(), 'Finished Event');
            END ;;

            -- ------------------------------------------------------
            --
            -- Create pcs_db tables, if needed.
            --
            -- NOTE:  This is duplicated in a later routine, 
            -- but needed here so triggers can be created.
            --
            -- ------------------------------------------------------

            CREATE TABLE IF NOT EXISTS pcs_log (
                  part_schema varchar(64) DEFAULT NULL,
                  part_table varchar(64) DEFAULT NULL,
                  part_partition varchar(64) DEFAULT NULL,
                  message_type enum('Error','Info') NOT NULL DEFAULT 'Info',
                  logging_proc enum('Tables','Create','Drop','Main','Init','Control_Table_Change','Event') NOT NULL DEFAULT 'Main',
                  action_timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  message varchar(512) NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;;

            CREATE TABLE IF NOT EXISTS pcs_config (
                  part_schema varchar(64) NOT NULL COMMENT 'Database of the table',
                  part_table varchar(64) NOT NULL COMMENT 'The partitioned table',
                  partition_column varchar(64) NOT NULL COMMENT 'Table column partitioned ',
                  table_state enum('Init','Active','Ignore') NOT NULL DEFAULT 'Init' COMMENT 'Indicates how the table will be processed. Init: to be partitioned. Active: partitioned. Ignore: no processing.',
                  keep_days int(10) unsigned NOT NULL DEFAULT '0' COMMENT 'Number of days to keep.  Min 3, Max 2048, 0 days indicates never drop',
                  comment varchar(512) DEFAULT NULL COMMENT 'Information about when this record was created or updated',
                  last_update timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT ' Record the last time something happened on this table/partition',
                  partition_boundary_hour enum('00','01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24') NOT NULL DEFAULT '00' COMMENT 'The hour of the day for switching to the next partition',
                  PRIMARY KEY (part_schema,part_table)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;;

            -- ------------------------------------------------------
            --
            -- pcs_config_triggers
            --
            -- Insert, Update and Delete triggers for pcs_config.
            --
            -- ------------------------------------------------------

            DROP TRIGGER IF EXISTS pcs_config_insert;;

            CREATE TRIGGER pcs_config_insert AFTER INSERT
            ON pcs_config FOR EACH ROW
            BEGIN
                  DECLARE l_message varchar(512);
                  set l_message = CONCAT("INSERT: table_state: ", NEW.table_state, ", keep_days: ", NEW.keep_days);
                  INSERT INTO pcs_log (part_schema, part_table, part_partition, message_type, logging_proc, action_timestamp, message) values
                  (NEW.part_schema, NEW.part_table, NEW.partition_column, 'Info', 'pcs_config_change', NOW(), l_message);
            END;;

            DROP TRIGGER IF EXISTS pcs_config_update;;

            CREATE TRIGGER pcs_config_update AFTER UPDATE
            ON pcs_config FOR EACH ROW
            BEGIN
                  DECLARE l_message varchar(512);
                  set l_message = CONCAT("UPDATE: table_state: ", NEW.table_state, ", keep_days: ", NEW.keep_days);
                  INSERT INTO pcs_log (part_schema, part_table, part_partition, message_type, logging_proc, action_timestamp, message) values
                  (NEW.part_schema, NEW.part_table, NEW.partition_column, 'Info', 'pcs_config_change', NOW(), l_message);
            END;;

            DROP TRIGGER IF EXISTS pcs_config_delete;;

            CREATE TRIGGER pcs_config_delete AFTER DELETE
            ON pcs_config FOR EACH ROW
            BEGIN
                  DECLARE l_message varchar(512);
                  set l_message = CONCAT("DELETE: table_state: ", OLD.table_state, ", keep_days: ", OLD.keep_days);
                  INSERT INTO pcs_log (part_schema, part_table, part_partition, message_type, logging_proc, action_timestamp, message) values
                  (OLD.part_schema, OLD.part_table, OLD.partition_column, 'Info', 'pcs_config_change', NOW(), l_message);
            END;;

            -- ------------------------------------------------------
            --
            -- pcs_version
            --
            -- A simple procedure that returns the PCS version.
            -- Can be used by Ansible to determine if it is using the correction version.
            --
            -- ------------------------------------------------------

            DROP PROCEDURE IF EXISTS pcs_version;;
            CREATE PROCEDURE `pcs_version`()
            BEGIN
                  SELECT '3.0.0' AS Version;
            END ;;

            -- ------------------------------------------------------
            --
            -- pcs_update_index
            --
            -- Procedure to test if a column is already part of the
            -- primary key.  If not, it addes it.
            --
            -- Note:  if run on a large table, it will create the needed index
            -- without warning!
            --
            -- ------------------------------------------------------

            DROP PROCEDURE IF EXISTS pcs_update_index;;
            CREATE PROCEDURE `pcs_update_index`(in P_partition_schema varchar(64), in P_table_name varchar(64), in P_partition_column varchar(64))
            MAIN_PCSX_PROC: BEGIN

            DECLARE IndexCount int default 0;
            DECLARE alter_sql varchar(512);
            DECLARE do_sql varchar(512);
            DECLARE PrimaryIndexList varchar(255) default NULL;
            DECLARE l_primary_column varchar(100);
            DECLARE l_message varchar(100);
            DECLARE l_DDL_ERROR int default 0;
            DECLARE Finished boolean default FALSE;

            DECLARE PrimaryIndexCursor CURSOR for
                  select column_name from information_schema.columns where table_schema = P_partition_schema and table_name = P_table_name and column_key = 'PRI';

            DECLARE CONTINUE HANDLER for not found set Finished = TRUE;
            DECLARE CONTINUE HANDLER for 1146  set l_DDL_ERROR=1146;  # Table does not exist
            DECLARE CONTINUE HANDLER for 1061  set l_DDL_ERROR=1061;  # Duplicate Key name
            DECLARE CONTINUE HANDLER for 1068  set l_DDL_ERROR=1068;  # Multiple primary keys defined
            DECLARE CONTINUE HANDLER for 1069  set l_DDL_ERROR=1068;  # Too many keys specified
            DECLARE CONTINUE HANDLER for 1070  set l_DDL_ERROR=1070;  # Too many key parts specified
            DECLARE CONTINUE HANDLER for 1071  set l_DDL_ERROR=1071;  # Specified key was too long
            DECLARE CONTINUE HANDLER for 1072  set l_DDL_ERROR=1072;  # Key column doesn't exist in table
            DECLARE CONTINUE HANDLER for 1073  set l_DDL_ERROR=1073;  # BLOB column can't be used in key specification with the use table type

            -- Do we need to add the column to the primary key?  Check and see

            SELECT COUNT(*) INTO IndexCount
                  FROM information_schema.columns
                  WHERE table_schema = P_partition_schema
                        AND table_name = P_table_name
                        AND column_name = P_partition_column
                        AND column_key = 'PRI';

            IF (IndexCount = 0) THEN
                 -- Yes, so loop through the current columns, if any, that are part of the primary key and build a new key list
                  set Finished = FALSE;

                  OPEN PrimaryIndexCursor;

                  CURSOR_LOOP: LOOP
                        FETCH PrimaryIndexCursor
                              into l_primary_column;

                        IF Finished THEN
                              close PrimaryIndexCursor;
                              leave CURSOR_LOOP;
                        END IF;

                        -- Append the list of existing primary key columns to PrimaryIndexList for the ALTER statement

                        IF (PrimaryIndexList IS NULL) THEN
                              set PrimaryIndexList = l_primary_column;
                        ELSE
                              set PrimaryIndexList = CONCAT(PrimaryIndexList, ", ", l_primary_column);
                        END IF;

                  END LOOP CURSOR_LOOP;

                  -- Did we find any?  Build the new Primary Index List based on what we found.

                  IF (PrimaryIndexList IS NULL) THEN
                        set PrimaryIndexList = P_partition_column;
                        set @alter_sql = CONCAT("ALTER TABLE ", P_partition_schema, ".", P_table_name, " ADD PRIMARY KEY(", PrimaryIndexList, ")");
                  ELSE
                        set PrimaryIndexList = CONCAT(PrimaryIndexList, ", ", P_partition_column);
                        set @alter_sql = CONCAT("ALTER TABLE ", P_partition_schema, ".", P_table_name, " DROP PRIMARY KEY, ADD PRIMARY KEY(", PrimaryIndexList, ")");
                  END IF;

                  set l_DDL_error = 0;

                  -- ALTER statement is built, remove the existing key and add the new primary key, with the new Column

                  prepare do_sql from @alter_sql;
                  execute do_sql;
                  deallocate prepare do_sql;

                  -- Look for errors and handle accordingly

                  IF l_DDL_error > 0 THEN
                        set l_message=concat('DDL_ERROR ',l_DDL_ERROR);

                        CASE  l_DDL_ERROR
                              WHEN 1146 THEN
                                    set l_message=concat(l_message,' Table does not exist  ');
                              WHEN 1061 THEN
                                    set l_message=concat(l_message,' Duplicate key name  ');
                              WHEN 1068 THEN
                                    set l_message=concat(l_message,' Multiple primary keys defined  ');
                              WHEN 1069 THEN
                                    set l_message=concat(l_message,' Too many keys specified  ');
                              WHEN 1070 THEN
                                    set l_message=concat(l_message,' Too many key parts specified  ');
                              WHEN 1071 THEN
                                    set l_message=concat(l_message,' Specified key was too long  ');
                              WHEN 1072 THEN
                                    set l_message=concat(l_message,' Key column does not exist in table  ');
                              WHEN 1073 THEN
                                    set l_message=concat(l_message,' BLOB column can not be used in key specification with the use table type  ');

                        END CASE;

                        insert into pcs_log (part_schema, part_table, part_partition, logging_proc, message_type, message)
                              values (P_partition_schema, P_table_name, 'N/A', 'Create', 'Error', l_message);
                  ELSE
                        set l_message=concat('PRIMARY KEY created on ', PrimaryIndexList);

                        insert into pcs_log (part_schema, part_table, part_partition, logging_proc, message_type, message)
                        values (P_partition_schema, P_table_name, 'N/A', 'Create', 'Info', l_message);
                  END IF;

            END IF;

            END MAIN_PCSX_PROC ;;

            -- ------------------------------------------------------
            --
            -- pcs_config_insert
            --
            -- Provides controlled access to insert rows in the
            -- pcs_config table.  This can be called by an automation tool
            -- without needed elevated access to pcs_db.
            --
            --  This procedure has been written purely to
            --  1) Facilitate the automated population of the pcs_config table
            --  2) Prevent unlogged access to the a pcs_config table by non root users
            --  3) Limit the access to the table to localhost access for non root access.
            --
            -- ------------------------------------------------------

            DROP PROCEDURE IF EXISTS pcs_config_insert;;
            CREATE PROCEDURE `pcs_config_insert`(in P_partition_schema varchar(64), in P_table_name varchar(64), in p_partition_column varchar(64), in p_keep_days int(10) unsigned)
            MAIN_PCSCI_PROC: BEGIN

            DECLARE l_user varchar(60);
            DECLARE l_ip   varchar(30);
            DECLARE l_partition_datatype varchar(64);
            DECLARE l_message           varchar(128);
            DECLARE RecordCount   int   default 0;
            DECLARE l_boundary_hour  enum('00','01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24');
            DECLARE l_table_state varchar(16) DEFAULT NULL;

            --  Check that the current user has an IP of localhost in the IP portion of its account.
            --
            --  1) Check that the p_partition_schema, p_table_name actually exists on this database - Error and logging message if it fails
            --  2) Check the datatype of the column to be partitioned is one of our allowed - Error and logging message if fails

            set l_user=substring_index(user(),'@',1);
            set l_ip=substring_index(user(),'@',-1) ;

            -- Informational logging message
            
            set l_message=concat('START - called by ''', l_user, ''' with IP ''',l_ip,''' ');
            insert into pcs_log (part_schema, part_table, logging_proc, message_type, message)
                  values ('N/A', 'N/A','Control_Table_Change','Info', l_message);

            -- Set the partition boundary to match the start time of the event if the event exists.

            select date_format( starts,'%H') into l_boundary_hour from information_schema.events
                  where event_name='pcs_event';

            IF l_boundary_hour IS NULL THEN
                  set l_boundary_hour='00';
            END IF;

            --  Check the datatype of the column to be partitioned is one of our allowed types.

            select data_type into l_partition_datatype from information_schema.columns
                  where table_name = P_table_name and column_name = p_partition_column and table_schema = P_partition_schema;

            IF  l_partition_datatype is null THEN
                  set l_message=concat('Table ''' , P_partition_schema,'.',P_table_name,''' with column ''',p_partition_column,''' does not exist');

                  insert into pcs_log (part_schema,part_table,logging_proc,message_type,message)
                        values (p_partition_schema, p_table_name,'Control_Table_Change','Error', l_message);
                  leave MAIN_PCSCI_PROC;
            END IF;

            IF  l_partition_datatype not in ('timestamp','datetime') THEN
                  set l_message=concat('Partitioning not allowed on ' , l_partition_datatype,' datatype ');

                  insert into pcs_log (part_schema,part_table,logging_proc,message_type,message)
                        values (p_partition_schema, p_table_name,'Control_Table_Change','Error', l_message);
                  leave MAIN_PCSCI_PROC;
            END IF;

            -- Check to see if the row is already there.  If so, just update the keep_days value
            
            SELECT table_state INTO l_table_state
                  FROM pcs_config
                  WHERE part_schema = P_partition_schema
                        AND part_table = P_table_name
                        AND partition_column = P_partition_column;

            IF l_table_state IS NULL THEN
                  -- Insert the new row into partition control table - with a status of 'Init' so the table gets partitioned on the first call to run_pcs
                  insert into pcs_config (part_schema, part_table, partition_column, table_state, keep_days, partition_boundary_hour)
                        values(p_partition_schema, p_table_name, p_partition_column, 'Init', p_keep_days, l_boundary_hour);
            ELSE
                  -- The row exists, so just update keep_days, if necessary
                  UPDATE pcs_config SET keep_days = p_keep_days WHERE part_schema = p_partition_schema AND part_table = p_table_name AND partition_column = p_partition_column;
            END IF;

            insert into pcs_log (part_schema,part_table,logging_proc,message_type,message)
                  values (p_partition_schema, p_table_name,'Control_Table_Change','Info', 'END - completed');

            END MAIN_PCSCI_PROC ;;


            -- ------------------------------------------------------
            --
            -- pcs_create
            --
            -- Adds a new partitions to an already partitioned table.
            --
            -- ------------------------------------------------------

            DROP PROCEDURE IF EXISTS pcs_create;;
            CREATE PROCEDURE `pcs_create`(in p_partition_schema varchar(64),in p_table_name varchar(64), p_partition_datatype varchar(64), p_hour_boundary enum('00','01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24'))
            MAIN_PCSC_PROC: BEGIN

            DECLARE mr_part_name varchar(20);
            DECLARE mr_part_desc bigint;

            DECLARE alter_sql varchar(512);
            DECLARE do_sql    varchar(512);
            DECLARE l_message varchar(100);

            DECLARE l_DDL_ERROR int default 0;
            DECLARE CONTINUE HANDLER for 1146 set l_DDL_ERROR=1146;  # Table does not exist
            DECLARE CONTINUE HANDLER for 1493 set l_DDL_ERROR=1493;  # Values less than must be strictly increasing for each partition
            DECLARE CONTINUE HANDLER for 1503 set l_DDL_ERROR=1503;  # A PRIMARY KEY must include all columns in the table's partitioning function
            DECLARE CONTINUE HANDLER for 1054 set l_DDL_ERROR=1054;  # Unknown column in partition function
            DECLARE CONTINUE HANDLER for 1505 set l_DDL_ERROR=1505;  # Partition management attempted on non partitioned table.
            DECLARE CONTINUE HANDLER for 1506 set l_DDL_ERROR=1506;  # Foreign keys not supported with partitioning
            DECLARE CONTINUE HANDLER for 1507 set l_DDL_ERROR=1507;  # Error in list of partitions
            DECLARE CONTINUE HANDLER for 1508 set l_DDL_ERROR=1508;  # Cannot remove all partitions, use DROP TABLE instead
            DECLARE CONTINUE HANDLER for 1517 set l_DDL_ERROR=1517;  # Same name partition
            DECLARE CONTINUE HANDLER for 1659 set l_DDL_ERROR=1659;  # Field not allowed type for this type of partitioning

            set l_message='';

            IF @CALLING_PSC_PROC !='RUN' OR @CALLING_PSC_PROC is null THEN
                  insert into pcs_log (part_schema,part_table,logging_proc,message_type,message)
                        values (p_partition_schema, p_table_name,'Create','Error', 'This procedure cannot be called directly');
                  leave MAIN_PCSC_PROC;
            ELSE
                  insert into pcs_log (part_schema,part_table,logging_proc,message_type,message)
                        values (p_partition_schema,p_table_name,'Create','Info', 'Starting Create');
            END IF;

            select partition_name,partition_description  into mr_part_name, mr_part_desc
                  from information_schema.partitions
                  where table_schema=p_partition_schema 
                        and table_name=p_table_name 
                        and partition_name is not null
                        order by partition_ordinal_position desc limit 1;

            IF p_partition_datatype='datetime' THEN
                  WHILE ( mr_part_desc <= to_days(date_add(now(),interval 30 day)) ) DO
                        set mr_part_name=concat("p",date_format(from_days(mr_part_desc),'%Y%m%d'));
                        set mr_part_desc=mr_part_desc +1;

                        set @alter_sql=concat("alter table ",p_partition_schema,'.',p_table_name, " add partition ( partition ",mr_part_name," values less than (",mr_part_desc,"))" );
                        set l_DDL_error=0;

                        prepare do_sql from @alter_sql;
                        execute do_sql;
                        deallocate prepare do_sql;

                        IF l_DDL_error > 0 THEN
                              set l_message=concat('DDL_ERROR ',l_DDL_ERROR);

                              CASE  l_DDL_ERROR
                                    WHEN 1146 THEN
                                          set l_message=concat(l_message,'  Table does not exist');
                                    WHEN 1493 THEN
                                          set l_message=concat(l_message,'  Values less than must be strictly increasing');
                                    WHEN 1503 THEN
                                          set l_message=concat(l_message,'  A PK must include all columns of partition');
                                    WHEN 1504 THEN
                                          set l_message=concat(l_message,'  Unknown column in partition function');
                                    WHEN 1505 THEN
                                          set l_message=concat(l_message,'  Partition management attempted on non partitioned table');
                                    WHEN 1506 THEN
                                          set l_message=concat(l_message,'  Foreign keys not supported with partitioning');
                                    WHEN 1507 THEN
                                          set l_message=concat(l_message,'  Error in list of partitions');
                                    WHEN 1508 THEN
                                          set l_message=concat(l_message,'  Cannot remove all partitions, use DROP TABLE');
                                    WHEN 1517 THEN
                                          set l_message=concat(l_message,'  Partition names must be unique');
                                    WHEN 1659 THEN
                                          set l_message=concat(l_message,'  Field not allowed type');
                              END CASE;

                              insert into pcs_log (part_schema,part_table,part_partition,logging_proc,message_type,message)
                                    values (p_partition_schema,p_table_name,mr_part_name,'Create','Error', l_message);
                        ELSE
                              set l_message=concat('Created with partition value ',mr_part_desc);
                              insert into pcs_log (part_schema,part_table,part_partition,logging_proc,message_type,message)
                                    values (p_partition_schema,p_table_name,mr_part_name,'Create','Info', l_message);
                        END IF;
                  END WHILE;
            ELSE
                  WHILE ( mr_part_desc < unix_timestamp(date_format(substr(mr_part_name,2,8),'%Y%m%d')) ) DO
                        set mr_part_desc = unix_timestamp((date_format(from_unixtime(mr_part_desc),'%Y-%m-%d')))+86400;
                  END WHILE;

                  set mr_part_desc = unix_timestamp((date_format(from_unixtime(mr_part_desc),concat('%Y-%m-%d ',p_hour_boundary))));
                  set mr_part_desc=mr_part_desc+86400;

                  WHILE (mr_part_desc < unix_timestamp(date_format(DATE_ADD(NOW(), INTERVAL 30 DAY),concat('%Y-%m-%d ',p_hour_boundary))) ) DO
                        set mr_part_name=concat('p', date_format(from_unixtime(mr_part_desc),'%Y%m%d') );
                        set @alter_sql=concat("alter table ",p_partition_schema,'.',p_table_name, " add partition ( partition ",mr_part_name," values less than (",mr_part_desc,"))" );
                        set l_DDL_error=0;
                        prepare do_sql from @alter_sql;
                        execute do_sql;
                        deallocate prepare do_sql;

                        IF l_DDL_error > 0 THEN
                              set l_message=concat('DDL_ERROR ',l_DDL_ERROR);

                              CASE l_DDL_ERROR
                                    WHEN 1146 THEN
                                          set l_message=concat(l_message,'  Table does not exist');
                                    WHEN 1493 THEN
                                          set l_message=concat(l_message,'  Values less than must be strictly increasing');
                                    WHEN 1503 THEN
                                          set l_message=concat(l_message,'  A PK must include all columns of partition');
                                    WHEN 1504 THEN
                                          set l_message=concat(l_message,'  Unknown column in partition function');
                                    WHEN 1505 THEN
                                          set l_message=concat(l_message,'  Partition management attempted on non partitioned table');
                                    WHEN 1506 THEN
                                          set l_message=concat(l_message,'  Foreign keys not supported with partitioning');
                                    WHEN 1507 THEN
                                          set l_message=concat(l_message,'  Error in list of partitions');
                                    WHEN 1508 THEN
                                          set l_message=concat(l_message,'  Cannot remove all partitions, use DROP TABLE');
                                    WHEN 1517 THEN
                                          set l_message=concat(l_message,'  Partition names must be unique');
                                    WHEN 1659 THEN
                                          set l_message=concat(l_message,'  Field not allowed type');
                              END CASE;

                              insert into pcs_log (part_schema,part_table,part_partition,logging_proc,message_type,message)
                                    values (p_partition_schema,p_table_name,mr_part_name,'Create','Error', l_message);
                        ELSE
                              set l_message=concat('Created on boundary hour: ', p_hour_boundary,' partition value: ',mr_part_desc);
                              insert into pcs_log (part_schema,part_table,part_partition,logging_proc,message_type,message)
                                    values (p_partition_schema,p_table_name,mr_part_name,'Create','Info', l_message);
                        END IF;
                        set mr_part_desc=mr_part_desc+86400;
                  END WHILE;
            END IF;

            END MAIN_PCSC_PROC ;;


            -- ------------------------------------------------------
            --
            -- pcs_drop
            --
            -- Drops one or more partitions, based on p_keep_days
            --
            -- ------------------------------------------------------

            DROP PROCEDURE IF EXISTS pcs_drop;;            
            CREATE PROCEDURE `pcs_drop`(in P_partition_schema varchar(64),in P_table_name varchar(64), in p_keep_days int)
            MAIN_PCSD_PROC: BEGIN
            DECLARE l_keep_days int;
            DECLARE l_max_partition_ordinal_posn int;
            DECLARE l_part_name varchar(64);
            DECLARE l_message varchar(50);
            DECLARE drop_sql  varchar(512);
            DECLARE do_sql    varchar(512);

            IF @CALLING_PSC_PROC !='RUN' or @CALLING_PSC_PROC is null THEN
                  insert into pcs_log (part_schema,part_table,logging_proc,message_type,message)
                        values (p_partition_schema,p_table_name,'Drop','Error', 'This procedure cannot be called directly');
                  leave MAIN_PCSD_PROC;
            END IF;

            set l_keep_days=p_keep_days;

            IF l_keep_days=0 THEN
                  insert into pcs_log (part_schema,part_table,logging_proc,message_type,message)
                  values (p_partition_schema,p_table_name,'Drop','Info', 'Ignoring partition drop as keep days set to 0');
                  leave MAIN_PCSD_PROC;
            ELSE
                  set l_message=concat('Starting partition drop with keep_days=',l_keep_days);
                  insert into pcs_log (part_schema,part_table,logging_proc,message_type,message)
                  values (p_partition_schema,p_table_name,'Drop','Info', l_message);
            END IF;

            -- keep_days needs to be between 3 and 2048 days.
            IF p_keep_days < 3 THEN
                  set l_keep_days=3;
            END IF;
            
            IF p_keep_days > 2048 THEN
                  set l_keep_days = 2048;
            END IF;

            -- Get the total number of partitions for a specific table
            select max(partition_ordinal_position) into l_max_partition_ordinal_posn
                  from information_schema.partitions  
                  where table_name=p_table_name
                        and table_schema=p_partition_schema 
                        and partition_name is not null;

            -- While total number of partitions is greater then keep_days, plus 30 for 30 extra future partitions created

            WHILE_LOOP: WHILE  ( l_max_partition_ordinal_posn > (l_keep_days+30)) DO
                  select partition_name into l_part_name
                        from information_schema.partitions  
                        where table_name=p_table_name 
                              and table_schema=p_partition_schema
                              and partition_name is not null 
                              and partition_ordinal_position=1;

                  IF l_part_name = concat("p",date_format(date_sub(now(),interval 3 day) ,'%Y%m%d')) THEN
                        insert into pcs_log (part_schema,part_table,part_partition,logging_proc,message_type,message)
                              values (p_partition_schema,p_table_name,l_part_name,'Drop','Error', '3 day history threshold breached. Drop aborted.');
                        LEAVE WHILE_LOOP;
                  END IF;

                  set @drop_sql=concat('alter table ',p_partition_schema,'.',p_table_name, ' drop  partition ', l_part_name);

                  prepare do_sql from @drop_sql;
                  execute do_sql;
                  deallocate prepare do_sql;

                  insert into pcs_log (part_schema,part_table,part_partition,logging_proc,message_type,message)
                        values (p_partition_schema,p_table_name,l_part_name,'Drop','Info', 'Dropped');

                  set l_max_partition_ordinal_posn=l_max_partition_ordinal_posn-1;

            END WHILE;

            END MAIN_PCSD_PROC ;;


            -- ------------------------------------------------------
            --
            -- pcs_init
            --
            -- Creates the initial partition of a table, based on
            -- the contents of pcs_config table
            --
            -- ------------------------------------------------------

            DROP PROCEDURE IF EXISTS pcs_init;;
            CREATE PROCEDURE `pcs_init`(in P_partition_schema varchar(64), in P_table_name varchar(64), in p_partition_column varchar(64), in p_partition_datatype varchar(64),p_hour_boundary enum('00','01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24'))
            MAIN_PCSI_PROC: BEGIN

            DECLARE RecordCount         int default 0;
            DECLARE cur_day             int default 0;
            DECLARE l_partition_schema  varchar(64);
            DECLARE l_message           varchar(128);
            DECLARE l_partition_cmd     varchar(512);
            DECLARE l_do_sql            varchar(512);
            DECLARE l_partition_name    varchar(12);
            DECLARE l_partition_to_days varchar(50);
            DECLARE l_partition_unix    varchar(50);

            DECLARE l_DDL_ERROR int default 0;
            DECLARE CONTINUE HANDLER for 1146 set l_DDL_ERROR=1146;  # Table does not exist
            DECLARE CONTINUE HANDLER for 1493 set l_DDL_ERROR=1493;  # Values less than must be strictly increasing for each partition
            DECLARE CONTINUE HANDLER for 1503 set l_DDL_ERROR=1503;  # A PRIMARY KEY must include all columns in the table's partitioning function
            DECLARE CONTINUE HANDLER for 1054 set l_DDL_ERROR=1054;  # Unknown column in partition function
            DECLARE CONTINUE HANDLER for 1505 set l_DDL_ERROR=1505;  # Partition managment attempted on non partitioned table.
            DECLARE CONTINUE HANDLER for 1506 set l_DDL_ERROR=1506;  # Foreign keys not supported with partitioning
            DECLARE CONTINUE HANDLER for 1507 set l_DDL_ERROR=1507;  # Error in list of partitions
            DECLARE CONTINUE HANDLER for 1508 set l_DDL_ERROR=1508;  # Cannot remove all partitions, use DROP TABLE instead
            DECLARE CONTINUE HANDLER for 1517 set l_DDL_ERROR=1517;  # Same name partition
            DECLARE CONTINUE HANDLER for 1659 set l_DDL_ERROR=1659;  # Field not allowed type for this type of partitioning

            IF @CALLING_PCS_PROC !='RUN' or @CALLING_PCS_PROC is null THEN
                  insert into pcs_log (part_schema,part_table,logging_proc,message_type,message)
                        values (p_partition_schema, p_table_name,'Init','Error', 'This procedure cannot be called directly');
                  leave MAIN_PCSI_PROC;
            END IF;

            set Recordcount=0;
            select count(*) into RecordCount
                  from information_schema.partitions 
                  where table_schema=p_partition_schema 
                        and table_name=p_table_name  
                        and table_schema=database() 
                        and partition_name is not null;
            
            IF RecordCount > 0 THEN
                  set l_message=concat('Table already partitioned');
                  insert into  pcs_log (part_schema,part_table,logging_proc,message_type, message)
                        values (p_partition_schema,p_table_name,'Init','Error',l_message);
                  leave MAIN_PCSI_PROC;
            END IF;

            --
            -- Verify that p_partition_column is part of the primary key and add it, if necessary
            --
            CALL pcs_update_index(P_partition_schema, P_table_name, P_partition_column);

            -- Build partitioning SQL

            -- Get diff between minimum datetime value from table and now
            set @cur_day_cmd=CONCAT('SELECT IFNULL(datediff(now(), min(',p_partition_column,')),3) INTO @cur_day FROM ',p_partition_schema,'.',p_table_name);
         
            prepare l_do_sql from @cur_day_cmd;
            execute l_do_sql; deallocate prepare l_do_sql;

            IF p_partition_datatype='timestamp' THEN
                  set @l_partition_cmd=concat(' alter table ',p_partition_schema,'.',p_table_name,'  PARTITION BY RANGE (UNIX_TIMESTAMP(`',p_partition_column,'`)) (');
             
                  WHILE @cur_day > -2 DO 
                        set l_partition_name=concat("p",date_format(date_sub(now(), interval @cur_day DAY),'%Y%m%d'));
                        set l_partition_unix=unix_timestamp(date_sub(now(), interval @cur_day DAY));
                        set @l_partition_cmd=concat(@l_partition_cmd,'PARTITION ',l_partition_name, ' values less than (' ,l_partition_unix,')');

                        IF @cur_day > -1 THEN  
                              set @l_partition_cmd=concat(@l_partition_cmd,', '); 
                        ELSE 
                              set @l_partition_cmd=concat(@l_partition_cmd,')'); 
                        END IF;
                  
                        SET @cur_day = @cur_day - 1;
                  END WHILE;
            ELSE
                  set @l_partition_cmd=concat(' alter table ',p_partition_schema,'.',p_table_name,'  PARTITION BY RANGE (TO_DAYS(`',p_partition_column,'`)) (');

                  WHILE @cur_day > -2 DO            
                        set l_partition_name=concat("p",date_format(date_sub(now(), interval @cur_day DAY),'%Y%m%d'));
                        set l_partition_to_days=to_days(date_sub(now(), interval @cur_day DAY));
                        set @l_partition_cmd=concat(@l_partition_cmd,'PARTITION ',l_partition_name, ' values less than (' ,l_partition_to_days,')');
            
                        IF @cur_day > -1 THEN 
                              set @l_partition_cmd=concat(@l_partition_cmd,', '); 
                        ELSE 
                              set @l_partition_cmd=concat(@l_partition_cmd,')'); 
                        END IF;
                        SET @cur_day = @cur_day - 1;
                  END WHILE;
            END IF;

            prepare l_do_sql from @l_partition_cmd;
            execute l_do_sql; deallocate prepare l_do_sql;

            IF l_DDL_error > 0 THEN
                  set l_message=concat('DDL_ERROR ',l_DDL_ERROR);
                  
                  CASE  l_DDL_ERROR
                        WHEN 1146 THEN
                              set l_message=concat(l_message,'  Table does not exist');
                        WHEN 1493 THEN
                              set l_message=concat(l_message,'  Values less than must be strictly increasing');
                        WHEN 1503 THEN
                              set l_message=concat(l_message,'  A PK must include all columns of partition');
                        WHEN 1504 THEN
                              set l_message=concat(l_message,'  Unknown column in partition function');
                        WHEN 1505 THEN
                              set l_message=concat(l_message,'  Partition management attempted on non partitioned table');
                        WHEN 1506 THEN
                              set l_message=concat(l_message,'  Foreign keys not supported with partitioning');
                        WHEN 1507 THEN
                              set l_message=concat(l_message,'  Error in list of partitions');
                        WHEN 1508 THEN
                              set l_message=concat(l_message,'  Cannot remove all partitions, use DROP TABLE');
                        WHEN 1517 THEN
                              set l_message=concat(l_message,'  Partition names must be unique');
                        WHEN 1659 THEN
                              set l_message=concat(l_message,'  Field not allowed type');
                  END CASE;

                  insert into  pcs_log (part_schema,part_table,logging_proc,message_type, message)
                        values (p_partition_schema, p_table_name,'Init','Error',l_message);

                  leave MAIN_PCSI_PROC;
            END IF;

            update pcs_config set table_state='Active' where part_table=p_table_name and part_schema=p_partition_schema;

            set l_message=concat('Table has been partitioned');

            IF p_partition_datatype='timestamp' THEN
                  set l_message=concat(l_message, 'on hour boundary ', p_hour_boundary);
            end if;

            insert into  pcs_log (part_schema,part_table,part_partition,logging_proc,message_type, message)
                  values (p_partition_schema, p_table_name,l_partition_name, 'Init','Info',l_message);

            END MAIN_PCSI_PROC ;;


            -- ------------------------------------------------------
            --
            -- run_pcs
            --
            -- The main 'run' procedure called from the event or manually
            -- It adds/removes partitions based on the contents of
            -- pcs_config table.
            --
            -- ------------------------------------------------------

            DROP PROCEDURE IF EXISTS run_pcs;;
            CREATE PROCEDURE `run_pcs`()
            MAIN_PCSRUN_PROC: BEGIN
            DECLARE GoodToProcess   int default 0;
            DECLARE finished        boolean default false;
            DECLARE RecordCount     int   default 0;
            DECLARE ForeignKeyCount int   default 0;

            DECLARE l_partition_schema   varchar(64);
            DECLARE l_table_name         varchar(64);
            DECLARE l_partition_column   varchar(64);
            DECLARE l_partition_datatype varchar(64);
            DECLARE l_message            varchar(128);

            DECLARE l_table_state   enum('Init','Active','Ignore');
            DECLARE l_boundary_hour enum('00','01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24');
            DECLARE l_keep_days     int(10) unsigned;

            DECLARE cur_partition_control CURSOR for
            select part_schema,part_table,partition_column, table_state, keep_days,partition_boundary_hour
                  from pcs_config;

            DECLARE continue handler for not found set finished = true;

            SET @CALLING_PCS_PROC = 'RUN';

            insert into  pcs_log (message_type,logging_proc, message)
                  values ('Info','RUN','Starting PCS Run V3.0.0');

            set finished=false;
            OPEN cur_partition_control;

            CURSOR_LOOP: LOOP
                  FETCH cur_partition_control
                        into l_partition_schema, l_table_name, l_partition_column, l_table_state, l_keep_days, l_boundary_hour;

                  IF finished THEN
                        close cur_partition_control;
                        leave CURSOR_LOOP;
                  END IF;

                  insert into  pcs_log (part_schema,part_table,message_type,logging_proc, message)
                        values (l_partition_schema, l_table_name,'Info','RUN','Processing table');

                  set RecordCount=0; set GoodToProcess=1;

                  select count(*) into RecordCount from information_schema.columns
                        where table_name=l_table_name and column_name =l_partition_column and table_schema=l_partition_schema;

                  IF (RecordCount = 0) THEN
                        set l_message=concat('Table ' ,l_table_name, ' with column ' , l_partition_column,' does not exist');
                        insert into  pcs_log (part_schema,part_table,message_type, logging_proc,message)
                              values (l_partition_schema,l_table_name,'Error','RUN',l_message);
                        set GoodToProcess=0; ITERATE CURSOR_LOOP;
                  END IF;

                  select data_type into l_partition_datatype from information_schema.columns
                        where table_name=l_table_name and column_name =l_partition_column and table_schema=l_partition_schema;

                  IF l_partition_datatype not in ('timestamp','datetime') THEN
                        set l_message=concat('Partitioning not allowed on ' , l_partition_datatype,' datatype ');
                        insert into  pcs_log (part_schema,part_table,message_type,logging_proc, message)
                              values (l_partition_schema,l_table_name,'Error','RUN',l_message);
                        set GoodToProcess=0; ITERATE CURSOR_LOOP;
                  END IF;

                  set ForeignKeyCount=0;
                  select count(*) into ForeignKeyCount
                        from information_schema.table_constraints tc,
                        information_schema.key_column_usage kcu
                        where tc.constraint_type='FOREIGN KEY'
                              and tc.constraint_name=kcu.constraint_name
                              and tc.table_schema=l_partition_schema
                              and kcu.table_schema=l_partition_schema
                              and (tc.table_name=l_table_name or kcu.referenced_table_name=l_table_name);

                  If ForeignKeyCount > 0 THEN
                        insert into  pcs_log (part_schema,part_table,message_type,logging_proc, message)
                              values (l_partition_schema,l_table_name,'Error','RUN','Foreign key not allowed for partitioning');
                        set GoodToProcess=0; ITERATE CURSOR_LOOP;
                  END IF;

                  IF GoodToProcess=1 THEN
                        CASE  l_table_state
                              WHEN 'Init' THEN
                                    call pcs_init (l_partition_schema,l_table_name, l_partition_column,l_partition_datatype,l_boundary_hour);
                                    call pcs_create(l_partition_schema,l_table_name,l_partition_datatype,l_boundary_hour);
                              WHEN 'Active' THEN
                                    call pcs_drop(l_partition_schema,l_table_name,l_keep_days);
                                    call pcs_create(l_partition_schema, l_table_name,l_partition_datatype,l_boundary_hour);
                              WHEN 'Ignore' THEN
                                    DO 0;
                        END CASE;
                  END IF;

            END LOOP CURSOR_LOOP;

            insert into  pcs_log (message_type,logging_proc, message) values ('Info','RUN','Finished PCS Run V3.0.0');
            SET @CALLING_PCS_PROC='None';

            END MAIN_PCSRUN_PROC ;;


            -- ------------------------------------------------------
            --
            -- pcs_check
            --
            -- A procedure called by an external monitoring application
            -- to check that all partitions are correct and created.
            --
            -- ------------------------------------------------------

            DROP PROCEDURE IF EXISTS pcs_check;;
            CREATE PROCEDURE `pcs_check`()
            MAIN_PSCC_PROC: BEGIN

            DECLARE mr_part_name varchar(20);
            DECLARE mr_part_desc bigint;

            DECLARE alter_sql varchar(512);
            DECLARE do_sql    varchar(512);
            DECLARE l_message varchar(512);
            DECLARE message   varchar(512);
            
            DECLARE l_table_schema    varchar(64);
            DECLARE l_table_name      varchar(64);
            DECLARE l_keep_days       int(10);
            DECLARE c_keep_days       int(10);
            DECLARE l_part_expression varchar(64);
            DECLARE l_max_part        varchar(64);
            DECLARE l_min_part        varchar(64);
            DECLARE l_part_column     varchar(64);
            DECLARE l_part_datatype   varchar(64);

            DECLARE no_more_ids INT DEFAULT 0;

            DECLARE cur_id CURSOR FOR                        
            SELECT table_schema,
                   table_name,
                   partition_expression,
                   replace(replace(substring_index(substr(partition_expression,position('(' in partition_expression)+1),')',1) ,'`',''),'''','') as part_column,
                   max(partition_description) as max_partition,
                   min(partition_description) as min_partition,
                   keep_days
                   from information_schema.partitions, pcs_config
                   where partition_name is not null
                        and partition_method = 'RANGE' 
                        and table_schema = part_schema 
                        and table_name = part_table 
                        and table_state = 'Active'
                   group by table_schema,table_name,partition_expression;

            DECLARE EXIT HANDLER FOR NOT FOUND 
            BEGIN
                  CLOSE cur_id;
            END;

            OPEN cur_id;

                  while 1 DO
                        FETCH cur_id INTO l_table_schema, l_table_name, l_part_expression,l_part_column, l_max_part, l_min_part, l_keep_days;

                        select data_type into l_part_datatype 
                              from information_schema.columns
                              where table_name=l_table_name 
                                    and column_name =l_part_column 
                                    and table_schema=l_table_schema;

                        set l_message=' INFO - partition OK'; 
                        set c_keep_days=l_keep_days-1;

                        IF  l_part_datatype='datetime' AND ( l_max_part <= TO_DAYS(DATE_ADD(NOW(),INTERVAL 29 DAY)) ) THEN
                             set l_message=CONCAT(' ERROR -  max partition date: ', FROM_DAYS(l_max_part), ' should be: ', DATE_FORMAT(DATE_ADD(NOW(), INTERVAL 29 DAY),'%Y-%m-%d'));
                        ELSEIF
                             l_part_datatype='datetime' AND ( l_min_part > TO_DAYS(DATE_SUB(NOW(),INTERVAL c_keep_days DAY)) ) THEN
                             set l_message=CONCAT(' WARN -  min partition date: ', FROM_DAYS(l_min_part), ' should be: ', DATE_FORMAT(DATE_SUB(NOW(), INTERVAL l_keep_days DAY),'%Y-%m-%d'));
                        ELSEIF
                             l_part_datatype='timestamp' AND (l_max_part < UNIX_TIMESTAMP(DATE_FORMAT(DATE_ADD(NOW(), INTERVAL 29 DAY),'%Y%m%d') )) THEN
                             set l_message=CONCAT(' ERROR -  max partition date: ', FROM_UNIXTIME(l_max_part,'%Y-%m-%d'), ' should be: ', DATE_FORMAT(DATE_ADD(NOW(), INTERVAL 29 DAY),'%Y-%m-%d'));
                        ELSEIF
                             l_part_datatype='timestamp' AND ( l_min_part > UNIX_TIMESTAMP(DATE_FORMAT(DATE_SUB(NOW(), INTERVAL c_keep_days DAY), '%Y%m%d') )) THEN
                             set l_message=CONCAT(' ERROR -  min partition date: ', FROM_UNIXTIME(l_min_part,'%Y-%m-%d'), ' should be: ', DATE_FORMAT(DATE_SUB(NOW(), INTERVAL l_keep_days DAY),'%Y-%m-%d'));
                        ELSEIF
                             l_part_datatype NOT IN ('timestamp','datetime') THEN
                             set l_message=' ERROR - unrecognized partition datatype';
                        END IF;

                        set message=concat(l_message, ' for ' , l_table_schema, '.', l_table_name, '.', l_part_column, ' (', l_part_datatype, ') max value: ', from_days(l_max_part),', min value: ', from_days(l_min_part));
                        select message;

                  END WHILE;

            CLOSE cur_id;
            END MAIN_PSCC_PROC ;;


            -- ------------------------------------------------------
            --
            -- pcs_tables
            --
            -- A procedure that creates the pcs_log and
            -- pcs_config tables, created associated triggers 
            -- for logging and loads them, if tables in any database 
            -- on the server are already partitioned.
            --
            -- ------------------------------------------------------

            DROP PROCEDURE IF EXISTS pcs_tables;;
            CREATE PROCEDURE `pcs_tables`()
            MAIN_PSCT_PROC: BEGIN
            
            prepare create_table from   
            "CREATE TABLE IF NOT EXISTS pcs_log (
                  part_schema varchar(64) DEFAULT NULL,
                  part_table varchar(64) DEFAULT NULL,
                  part_partition varchar(64) DEFAULT NULL,
                  message_type enum('Error','Info') NOT NULL DEFAULT 'Info',
                  logging_proc enum('Tables','Create','Drop','Main','Init','Control_Table_Change') NOT NULL DEFAULT 'Main',
                  action_timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  message varchar(512) NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8";

            execute create_table;
            deallocate prepare create_table;

            prepare create_table from 
            "CREATE TABLE IF NOT EXISTS pcs_config (
                  part_schema varchar(64) NOT NULL COMMENT 'Database of the table',
                  part_table varchar(64) NOT NULL COMMENT 'The partitioned table',
                  partition_column varchar(64) NOT NULL COMMENT 'Table column partitioned ',
                  table_state enum('Init','Active','Ignore') NOT NULL DEFAULT 'Init' COMMENT 'Indicates how the table will be processed. Init: to be partitioned. Active: partitioned. Ignore: no processing.',
                  keep_days int(10) unsigned NOT NULL DEFAULT '0' COMMENT 'Number of days to keep.  Min 3, Max 2048, 0 days indicates never drop',
                  comment varchar(512) DEFAULT NULL COMMENT 'Information about when this record was created or updated',
                  last_update timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT ' Record the last time something happened on this table/partition',
                  partition_boundary_hour enum('00','01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24') NOT NULL DEFAULT '00' COMMENT 'The hour of the day for switching to the next partition',
                  PRIMARY KEY (part_schema,part_table)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8";
            
            execute create_table;
            deallocate prepare create_table;

            insert ignore into pcs_config(part_schema, part_table, partition_column, table_state, keep_days)
                  select distinct table_schema, table_name,      
                        replace(replace(   substring_index(substr(partition_expression,position('(' in partition_expression)+1)  ,')',1) ,'`',''),'''',''),'Active',0 
                        from information_schema.partitions 
                        where partition_name is not null 
                              and partition_method = 'RANGE';

            insert into pcs_log (message_type, logging_proc, message)
                  values ('Info', 'Tables', 'FINISHED TABLES PROC');

            END MAIN_PSCT_PROC ;;

            DELIMITER ;
