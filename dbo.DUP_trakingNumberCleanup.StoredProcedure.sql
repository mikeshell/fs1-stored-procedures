USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[DUP_trakingNumberCleanup]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[DUP_trakingNumberCleanup]
	-- Add the parameters for the stored procedure here
	
AS
BEGIN
	DECLARE @trackingNum VARCHAR(100), @id int, @position varchar(100)

SET @position = ''

DECLARE lagCursor CURSOR FOR 
select Id, [Tracking Number]
from trackingInfo
where [Tracking Number] in
(select [Tracking Number] from trackingInfo group by [Tracking Number] having COUNT(*) > 1)
order by 2, 1

OPEN lagCursor
FETCH NEXT FROM lagCursor INTO @id, @trackingNum

WHILE @@FETCH_STATUS = 0
   BEGIN
	IF @position = @trackingNum
	   begin
		delete from trackingInfo where Id = @id
	   end
	ELSE
	   BEGIN
		set @position = @trackingNum
	   END
	FETCH NEXT FROM lagCursor INTO @id, @trackingNum
   END

CLOSE lagCursor
DEALLOCATE lagCursor
	
END
GO
