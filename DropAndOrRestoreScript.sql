/********************************
DB DROP "AND" RESTORE SCRIPT, NEXT VERSION WILL DROP "AND/OR" RESTORE

INSTRUCTION: 
    - FIRST GO THROUGH THE SCRIPT, IT'S HUMAN READABLE
    - SET PATH AND NAME FROM LINE 36 - 38 AND RUN
NOTES: 
    - SCRIPT INTENDED FOR DB BACKUP FILES, NOT DB INSTANCE ITSELF
    - REQUIRES SQL AGENT TO BE RUNNING
    - FOR CSVIUGA MAIN DB, SHOULD TAKE ABOUT 6 MINUTES FOR RESTORE
    - TEMPDB IS A SYSTEMDB, MAY NEED TO USE ANOTHER DB (IGNORE DROP ERROR)
    - GUI VERSION COMING SOON (USING SMOs & .NET)

Author: CSC/OT
**********************************/

if DB_ID('TempDB') is null
   create database TempDB   
use tempdb;

--+--+--+--+--+--+--+--+--+--+--+--+--+--

DROP proc IF EXISTS sp_restore_script;
GO
CREATE PROC sp_restore_script
as

declare @BK_FILE_LOCATION as varchar(1000)
declare @RESTORE_LOCATION as varchar(1000)
declare @DB_NAME as varchar(155)

declare @restore_data_location as varchar(1000);
declare @restore_log_location as varchar(1000);

-- INPUT DATA HERE (Assuming Windows File System i.e., Backslash, Forward or Double Slash - For next version, Make dynamic) --
set @BK_FILE_LOCATION = 'C:\ClusterStorage\Volume1\Backup\SomeBackupFile.bak'
set @RESTORE_LOCATION = 'C:\ClusterStorage\Volume1\YourInstance\MSSQL13.Instance\MSSQL\DATA'
set @DB_NAME = N'Your_DB_Name'

set @restore_data_location = concat(@RESTORE_LOCATION, '\', @DB_NAME, '.mdf');
set @restore_log_location = concat(@RESTORE_LOCATION, '\', @DB_NAME, + '_log' + '.ldf');

declare @sql nvarchar(1000);
if exists (select 1 from sys.databases where [name] = @DB_Name)
begin
    set @SQL = N'
                use ' + @DB_Name + N' 
                alter database ' + @DB_Name + N' set single_user with rollback immediate;
                use [TempDB]
                drop database ' + @DB_Name
                ;
    exec (@sql)
end

waitfor delay '00:00:05'

declare @table table (LogicalName varchar(128),[PhysicalName] varchar(128), [Type] varchar, [FileGroupName] varchar(128), [Size] varchar(128), 
            [MaxSize] varchar(128), [FileId]varchar(128), [CreateLSN]varchar(128), [DropLSN]varchar(128), [UniqueId]varchar(128), [ReadOnlyLSN]varchar(128), [ReadWriteLSN]varchar(128), 
            [BackupSizeInBytes]varchar(128), [SourceBlockSize]varchar(128), [FileGroupId]varchar(128), [LogGroupGUID]varchar(128), [DifferentialBaseLSN]varchar(128), [DifferentialBaseGUID]varchar(128), [IsReadOnly]varchar(128), [IsPresent]varchar(128), [TDEThumbprint]varchar(128), [SnapshotUrl]varchar(128)
)
declare @logical_name_data varchar(128), @logical_name_log varchar(128)
insert into @table
exec('
RESTORE FILELISTONLY 
   FROM DISK=''' +@BK_FILE_LOCATION+ '''
   ')

   set @logical_name_data=(select [logicalname] from @table where type='D')
   set @logical_name_log=(select [logicalname] from @table where type='L')

restore database @DB_NAME
    from disk = @BK_FILE_LOCATION
    with move @logical_name_data to @restore_data_location,
    move  @logical_name_log to @restore_log_location;

go

--+--+--+--+--+--+--+--+--+--+--+--+--+--

DROP proc IF EXISTS sp_parallel_execution;
GO
CREATE PROC sp_parallel_execution
@SQLCommand NVARCHAR(4000),
@QueueLen INT = 100
AS
SET NOCOUNT ON
DECLARE @DBName sysname = DB_NAME();
DECLARE @SQL NVARCHAR(4000);

/* Check for current queue */
IF @QueueLen < ( 
       SELECT COUNT(a.queued_date)
       FROM msdb.dbo.sysjobs as j WITH (NOLOCK)
       INNER JOIN msdb.dbo.sysjobactivity as a WITH (NOLOCK)
       ON j.job_id = a.job_id
       WHERE j.name like 'Parallel_Execution_Command_%'
              and a.queued_date is NOT null
)
BEGIN
  RAISERROR('Can''t execute a query. SQL Agent queue length is bigger than current threshold %d.',16,1, @QueueLen);
  RETURN -1;
END

/* Generate new job */
DECLARE @CommandName sysname = 'Parallel_Execution_Command_'
       + REPLACE(CAST(NEWID() as NVARCHAR(36)),'-','_')
       + '_' + CONVERT(NVARCHAR(150), HASHBYTES ('SHA2_512', @SQLCommand), 1);
EXEC msdb.dbo.sp_add_job @job_name = @CommandName;

/* Generate first job's step with code execution */
SET @SQL = 'DECLARE @SQL NVARCHAR(4000);' + CHAR(10)
       + 'SET @SQL = ''' + REPLACE(@SQLCommand,'''','''''') + ''';' + CHAR(10)
       + 'EXEC sp_executesql @SQL;'
EXEC msdb.dbo.sp_add_jobstep @job_name = @CommandName, @step_name = N'Parallel_Execution_Command_1', @subsystem = N'TSQL', 
       @database_name = @DBName, @command = @SQL, @on_success_action = 3, @on_fail_action = 3;

/* Generate second job's step with deleting the job */
SET @SQL = 'EXEC msdb.dbo.sp_delete_job  @job_name = ''' + @CommandName + ''';';
EXEC msdb.dbo.sp_add_jobstep @job_name = @CommandName, @step_name = N'Parallel_Execution_Command_2', @subsystem = N'TSQL', 
       @database_name = @DBName, @command = @SQL;

/* Adding job to the server and executing it */
EXEC msdb.dbo.sp_add_jobserver @job_name = @CommandName;
EXEC msdb.dbo.sp_start_job @job_name = @CommandName;
RETURN 0;
go

--+--+--+--+--+--+--+--+--+--+--+--+--+--

drop proc if exists sp_progress_script
go
create proc sp_progress_script
as
waitfor delay '00:00:9'
SELECT r.command as COMMAND, CONVERT(VARCHAR(20),DATEADD(ms,r.estimated_completion_time,GETDATE()),20) AS [ETA COMPLETION TIME],
CONVERT(NUMERIC(6,2),r.estimated_completion_time/1000.0/60.0) AS [MINUTES TO COMPLETE]
FROM sys.dm_exec_requests r WHERE command IN ('RESTORE DATABASE');
go

--+--+--+--+--+--+--+--+--+--+--+--+--+--

begin
    declare @restore_sp as varchar(100);
    SET @restore_sp = '
        exec sp_restore_script
    ';
  exec sp_parallel_execution @restore_sp;
end

declare @time_msg as varchar(255)='CALCULATING TIME FOR COMPLETION...';
print @time_msg;

exec sp_progress_script
drop proc if exists sp_progress_script
drop proc if exists sp_parallel_execution;
drop proc if exists sp_restore_script;
-- drop database tempdb;