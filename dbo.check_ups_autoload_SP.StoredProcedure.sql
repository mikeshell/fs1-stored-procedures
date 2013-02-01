USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[check_ups_autoload_SP]    Script Date: 02/01/2013 11:09:50 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Michael Shell
-- Create date: Jan 24, 2013
-- Description:	This job will check for 0 records inserted from UPS Autoload, indicating that Dev 1 Autoload process shouldb e restarted
-- =============================================
CREATE PROCEDURE [dbo].[check_ups_autoload_SP]
AS

DECLARE @records varchar(10)
DECLARE @body1 varchar(255)

BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    -- Insert statements for procedure here
	SET @records = (SELECT COUNT(*) from upsFile)
	SET @body1 = @records + ' records are in the upsfile table'
	BEGIN EXEC msdb.dbo.sp_send_dbmail @profile_name='katom', @recipients='itsupport@katom.com',	@subject='UPS File Import Count',	@body=@body1, @body_format ='HTML',	@blind_copy_recipients='katom.archive@gmail.com' PRINT 'Email Sent' END
END
GO
