USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[FA_HouseItems_Process]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[FA_HouseItems_Process]
AS
BEGIN	
	
	DECLARE @sql VARCHAR(2000), @year SMALLINT, @month TINYINT, @CODE varchar(500), @qtySold INT, @cYear SMALLINT
	
	DECLARE @body1 VARCHAR(MAX), @email1 VARCHAR(500), @subject1 varchar(150), @bgcolor VARCHAR(500), @counter TINYINT, @subBody VARCHAR(MAX)
	 
	SET @sql = ''
	SET @cYear = YEAR(GETDATE())

	UPDATE FA_HouseOrderItems
	SET [1] = NULL, [2] = NULL, [3] = NULL, [4] = NULL, [5] = NULL, [6] = NULL, 
		[7] = NULL, [8] = NULL, [9] = NULL, [10] = NULL, [11] = NULL, [12] = NULL, 
		[13] = NULL, [3MOAVG] = NULL, [6MOAVG] = NULL, [APO_6MO] = NULL

	DECLARE catCursor CURSOR FOR 
	select YEAR([Order Date]), MONTH([Order Date]), l.[No_], ISNULL(SUM(l.[Quantity]), 0)
	from fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] h
		join fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] l on h.[No_] = l.[Document No_]
		join FA_HouseOrderItems t on t.code = l.[No_] collate Latin1_General_CS_AS
	where [Bill-to Name] = 'FROSTY ACRES BRANDS'
		and datediff(month, [Order Date], getdate()) < 13
	group by YEAR([Order Date]), MONTH([Order Date]), l.[No_]
	order by l.[No_], YEAR([Order Date]), MONTH([Order Date])

	OPEN catCursor
	FETCH NEXT FROM catCursor INTO @year, @month, @CODE, @qtySold

	WHILE @@FETCH_STATUS = 0
	   BEGIN
		SET @sql = 'UPDATE FA_HouseOrderItems SET ['
		
		IF @year < @cYear
		   BEGIN
			SET @sql = @sql + CAST((@month-8) AS VARCHAR(10))
		   END
		ELSE
		   BEGIN
			SET @sql = @sql + CAST((@month+4) AS VARCHAR(10))
		   END
		
		SET @sql = @sql + '] = ' + CAST(@qtySold AS VARCHAR(20)) + ' WHERE code = ''' + @CODE + ''''
		
		EXEC(@sql)
		
		FETCH NEXT FROM catCursor INTO @year, @month, @CODE, @qtySold
	   END

	CLOSE catCursor
	DEALLOCATE catCursor
	
	set @body1 = 'Your report is ready, <A HREF="http://www.katom.local/fa_houseitems.asp">Please Click here to view it!</A>'
	-- PREPPING THE EMAIL TO SEND
	SET @email1 = 'pbible@katom.com;'
	--	SET @email1 = 'david@katom.com'
	SET @subject1 = 'Frosty Acres House Item Stock'

	EXEC msdb.dbo.sp_send_dbmail @profile_name='katom', @recipients=@email1,	@subject=@subject1,	@body=@body1, @body_format ='HTML',	@blind_copy_recipients='katom.archive@gmail.com'	

END
GO
