USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[fedExTracking_SP2]    Script Date: 01/30/2013 16:26:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[fedExTracking_SP2]
AS
BEGIN
	DECLARE @jobID INT
	INSERT INTO jobHistory(jobName, startdT) values('FEDEXTracking', GETDATE())
	SET @jobID = @@identity

	--DATA CLEAN UP
	delete w
	from fedExWorktable2 w join fedExFile f on f.[Tracking Number] = REPLACE(w.[Tracking Number], '"', '')
										AND REPLACE(w.[reference], '"', '') = f.orderID
	
	INSERT INTO fedExFile([tracking Number], orderID, [timeStamp])
	SELECT DISTINCT REPLACE([Tracking Number], '"', ''), REPLACE([reference], '"', ''), GETDATE()
	FROM fedExWorktable2 
	where Delivered IS NULL
			and REPLACE([Tracking Number], '"', '') NOT IN (select [tracking Number] from fedExFile)
	ORDER BY 1
		
	TRUNCATE TABLE fedExWorktable2
		
	UPDATE jobHistory SET endDT = GETDATE() WHERE jobID = @jobID

END
GO
