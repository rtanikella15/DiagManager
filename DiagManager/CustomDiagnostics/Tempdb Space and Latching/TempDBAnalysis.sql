-- Revision 1.1 - EricBu
-- reversion 1.2 --jackli, added version store inputbuffer etc
USE Tempdb
GO
WHILE 1=1
BEGIN

PRINT '-- Current time'
select getdate()
DECLARE @runtime datetime
SET @runtime = GETDATE()

PRINT '-- sys.dm_db_file_space_usage --'
select	CONVERT (varchar(30), @runtime, 121) AS runtime, 
		DB_NAME() AS DbName, 
	SUM (user_object_reserved_page_count)*8 as usr_obj_kb,
	SUM (internal_object_reserved_page_count)*8 as internal_obj_kb,
	SUM (version_store_reserved_page_count)*8  as version_store_kb,
	SUM (unallocated_extent_page_count)*8 as freespace_kb,
	SUM (mixed_extent_page_count)*8 as mixedextent_kb
FROM sys.dm_db_file_space_usage 
RAISERROR ('', 0, 1) WITH NOWAIT

PRINT '-- Usage By File --'
SELECT	CONVERT (varchar(30), @runtime, 121) AS runtime, 
		DB_NAME() AS DbName, 
name AS FileName, 
size/128.0 AS CurrentSizeMB, 
size/128.0 - CAST(FILEPROPERTY(name, 'SpaceUsed') AS INT)/128.0 AS FreeSpaceMB
FROM sys.database_files
RAISERROR ('', 0, 1) WITH NOWAIT

PRINT '-- Transaction Counters --'
select CONVERT (varchar(30), @runtime, 121) AS runtime, 
		DB_NAME() AS DbName, *
from sys.dm_os_performance_counters
where Object_Name like '%:Transactions%'
RAISERROR ('', 0, 1) WITH NOWAIT

PRINT '-- sys.dm_db_session_space_usage --'
select	top 10 CONVERT (varchar(30), @runtime, 121) AS runtime,
				DB_NAME() AS DbName, 
				SS.* ,T.text [Query Text]
FROM	sys.dm_db_session_space_usage SS
        LEFT join sys.dm_exec_requests CN
		on SS.session_id = CN.session_id
        OUTER APPLY sys.dm_exec_sql_text(CN.sql_handle) T
ORDER BY (user_objects_alloc_page_count + internal_objects_alloc_page_count) DESC
RAISERROR ('', 0, 1) WITH NOWAIT

PRINT '-- sys.dm_db_task_space_usage --'
SELECT top 10 CONVERT (varchar(30), @runtime, 121) AS runtime,		
		DB_NAME() AS DbName, 
		TS.* ,
        T.text [Query Text]
FROM	sys.dm_db_task_space_usage TS
        INNER JOIN sys.sysprocesses ER 
		ON ER.ecid= TS.exec_context_id
			AND ER.spid = TS.session_id
        OUTER APPLY sys.dm_exec_sql_text(ER.sql_handle) T
ORDER BY (user_objects_alloc_page_count + internal_objects_alloc_page_count) DESC
RAISERROR ('', 0, 1) WITH NOWAIT


PRINT '-- version store transactions --'
select	CONVERT (varchar(30), @runtime, 121) AS runtime,a.*,b.kpid,b.blocked,b.lastwaittype,b.waittime
		, b.waitresource,b.dbid,b.cpu,b.physical_io,b.memusage,b.login_time,b.last_batch,b.open_tran
		,b.status,b.hostname,b.program_name,b.cmd,b.loginame,request_id
from	sys.dm_tran_active_snapshot_database_transactions a 
		inner join sys.sysprocesses b  
		on a.session_id = b.spid  
RAISERROR ('', 0, 1) WITH NOWAIT


RAISERROR ('-- version store transactions with input buffer', 0, 1) WITH NOWAIT
select CONVERT (varchar(30), @runtime, 121) AS runtime,b.spid,c.*
from sys.dm_tran_active_snapshot_database_transactions a
inner join sys.sysprocesses b
on a.session_id = b.spid
outer apply sys.dm_exec_sql_text(sql_handle) c
RAISERROR ('' , 0, 1) WITH NOWAIT

PRINT '-- Output of sessions with open transactions from sysprocesses'
SELECT  CONVERT (varchar(30), @runtime, 121) AS runtime,
		DB_NAME() AS DbName, * 
FROM	sys.sysprocesses
WHERE	open_tran = 1
RAISERROR ('', 0, 1) WITH NOWAIT

PRINT '-- Output of Open transactions with queries'
SELECT CONVERT (varchar(30), @runtime, 121) AS runtime,
		DB_NAME() AS DbName, 
    [s_tst].[session_id],
    [s_es].[login_name] AS [Login Name],
    DB_NAME (s_tdt.database_id) AS [Database],
    [s_tdt].[database_transaction_begin_time] AS [Begin Time],
    [s_tdt].[database_transaction_log_bytes_used] AS [Log Bytes],
    [s_tdt].[database_transaction_log_bytes_reserved] AS [Log Rsvd],
    [s_eqp].[query_plan] AS [Last Plan],
	T.text
FROM
    sys.dm_tran_database_transactions [s_tdt]
JOIN
    sys.dm_tran_session_transactions [s_tst]
ON
    [s_tst].[transaction_id] = [s_tdt].[transaction_id]
JOIN
    sys.[dm_exec_sessions] [s_es]
ON
    [s_es].[session_id] = [s_tst].[session_id]
LEFT OUTER JOIN
    sys.dm_exec_requests [s_er]
ON
    [s_er].[session_id] = [s_tst].[session_id]
OUTER APPLY
    sys.dm_exec_query_plan ([s_er].[plan_handle]) AS [s_eqp]
OUTER APPLY sys.dm_exec_sql_text(sql_handle) T
WHERE s_tdt.database_transaction_status =1
ORDER BY
    [Begin Time] ASC
RAISERROR ('', 0, 1) WITH NOWAIT


PRINT '-- Usage by objects --'
SELECT TOP 10
       CONVERT (varchar(30), @runtime, 121) AS runtime,
	   DB_NAME() AS DbName, 
       Cast(ServerProperty('ServerName') AS NVarChar(128)) AS [ServerName],
       DB_ID() AS [DatabaseID],
       DB_Name() AS [DatabaseName],
       [_Objects].[schema_id] AS [SchemaID],
       Schema_Name([_Objects].[schema_id]) AS [SchemaName],
       [_Objects].[object_id] AS [ObjectID],
       RTrim([_Objects].[name]) AS [TableName],
       (~(Cast([_Partitions].[index_id] AS Bit))) AS [IsHeap],       
       SUM([_Partitions].used_page_count) * 8192 UsedPageBytes,
       SUM([_Partitions].reserved_page_count) * 8192 ReservedPageBytes
FROM 
       [sys].[objects] AS [_Objects]
       INNER JOIN [sys].[dm_db_partition_stats] AS [_Partitions]
       ON ([_Objects].[object_id] = [_Partitions].[object_id])
WHERE ([_Partitions].[index_id] IN (0, 1))
GROUP BY [_Objects].[schema_id],
              [_Objects].[object_id],
              [_Objects].[name],
              [_Partitions].[index_id]
ORDER BY UsedPageBytes DESC;
RAISERROR ('', 0, 1) WITH NOWAIT


PRINT '-- wait-type-Pagelatch-tempdb --'
SELECT CONVERT (varchar(30), @runtime, 121) AS runtime, 
	session_id,    
	start_time,                    
	status,                    
	command,                        
	db_name(database_id),
	blocking_session_id,          
	wait_type,           
	wait_time,                      
	last_wait_type,
	wait_resource,                  
	open_transaction_count,
	cpu_time,        
	total_elapsed_time,
	logical_reads                  
FROM sys.dm_exec_requests
WHERE wait_type like 'PAGE%LATCH_%' AND wait_resource like '2:%'
RAISERROR ('', 0, 1) WITH NOWAIT

PRINT '-- Output from sys.dm_os_waiting_tasks --'
select CONVERT (varchar(30), @runtime, 121) AS runtime, session_id, wait_duration_ms, resource_description
FROM sys.dm_os_waiting_tasks WHERE wait_type like 'PAGE%LATCH_%' AND resource_description like '2:%'
RAISERROR ('', 0, 1) WITH NOWAIT



WAITFOR DELAY '00:01:00'
END
GO 