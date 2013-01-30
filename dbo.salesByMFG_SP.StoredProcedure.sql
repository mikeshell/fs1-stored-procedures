USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[salesByMFG_SP]    Script Date: 01/30/2013 16:26:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		David Lu
-- Create date: 03/28/2011
-- Description:	Pulling Sales Data by category or Mfg
-- =============================================
CREATE PROCEDURE [dbo].[salesByMFG_SP]
AS
BEGIN
	DECLARE @today datetime, @startDT datetime, @endDT datetime, @weekDay tinyint,
			@year Smallint, @month tinyint, @day tinyint, @startingWeek tinyint, @startingYear smallint, @endingWeek tinyint,
			@mfgID varchar(3), @wk int, @sales money, @sql varchar(max), @customer varchar(20)

	SET @today = GETDATE()
	SET @weekDay = DATEPART(WEEKDAY, @today)
	SET @endDT = DATEADD(DAY, -@weekday, @today)
	SET @startDT = DATEADD(DAY, -7, DATEADD(WEEK, -8, @endDT))
	SET @startingWeek = DATEPART(WEEK, @startDT)
	SET @startingYear = DATEPART(WEEK, @startDT)
	SET @endingWeek = DATEPART(WEEK, @endDT)
	SET @year = YEAR(@today)
	SET @month = MONTH(@today)
	SET @day = DAY(@today)
	
	TRUNCATE TABLE salesByMFG
	INSERT INTO salesByMFG(MFGid, mfgName, salesGroup)
	SELECT LEFT(l.[No_], 3), v.[Name], h.[Customer Source]
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] h
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] l ON l.[Document No_] = h.[No_]
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Vendor] v on v.[No_] = LEFT(l.[No_], 3)
	WHERE h.[Posting Date] BETWEEN @startDT AND @endDT
			AND h.[Source Code] = 'SALES'
			AND LEN(ISNULL(h.[Customer Source], '')) > 0
			AND charindex('-', l.[No_]) > 0
			AND l.[No_] not like '100-%'
	GROUP BY LEFT(l.[No_], 3), v.[Name], h.[Customer Source]
	ORDER BY 1, 3 ASC
	
	DECLARE catCursor CURSOR FOR  
	SELECT LEFT(l.[No_], 3), h.[Customer Source],
			DATEPART(WEEK, h.[Posting Date]) weekOfYear,
			CAST(SUM(ISNULL(l.[Quantity], 0)*ISNULL(l.[Unit Price], 0)) as decimal(10, 2)) salesTT
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] h
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] l ON l.[Document No_] = h.[No_]
	WHERE h.[Posting Date] BETWEEN @startDT AND @endDT
			AND h.[Source Code] = 'SALES'
			AND LEN(ISNULL(h.[Customer Source], '')) > 0
			AND charindex('-', l.[No_]) > 0
			AND l.[No_] not like '100-%'
	GROUP BY DATEPART(WEEK, h.[Posting Date]), dbo.getfirstDateOfWeek(DATEPART(WEEK, h.[Posting Date])), LEFT(l.[No_], 3), h.[Customer Source]
	ORDER BY 1, 3 ASC
	
	OPEN catCursor
	FETCH NEXT FROM catCursor INTO @mfgID, @customer, @wk, @sales

	WHILE @@FETCH_STATUS = 0
	   BEGIN
		SET @sql = 'UPDATE salesByMFG SET wk' + CAST((@wk-@startingWeek) AS VARCHAR(2)) 
			+ ' = ' + CAST(@sales AS VARCHAR(20)) + ', hasData = 1 WHERE mfgID = ''' + @mfgID + ''' and salesGroup = ''' + @customer + ''''

		EXEC (@sql)
			
		FETCH NEXT FROM catCursor INTO @mfgID, @customer, @wk, @sales
	   END

	CLOSE catCursor
	DEALLOCATE catCursor
	
	DECLARE catCursor CURSOR FOR  
	SELECT LEFT(l.[No_], 3), h.[Customer Source],
			@year,
			CAST(SUM(ISNULL(l.[Quantity], 0)*ISNULL(l.[Unit Price], 0)) as decimal(10, 2))
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] h
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] l ON l.[Document No_] = h.[No_]
	WHERE h.[Posting Date] BETWEEN '1/1/' + CAST(@year as varchar(4)) AND DATEADD(DAY, -1, @today)
			AND h.[Source Code] = 'SALES'
			AND LEN(ISNULL(h.[Customer Source], '')) > 0
			AND charindex('-', l.[No_]) > 0
			AND l.[No_] not like '100-%'
	GROUP BY LEFT(l.[No_], 3), h.[Customer Source]
	UNION
	SELECT LEFT(l.[No_], 3), h.[Customer Source],
			@year-1,
			CAST(SUM(ISNULL(l.[Quantity], 0)*ISNULL(l.[Unit Price], 0)) as decimal(10, 2))
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] h
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] l ON l.[Document No_] = h.[No_]
	WHERE h.[Posting Date] BETWEEN '1/1/' + CAST((@year-1) as varchar(4)) AND DATEADD(YEAR, -1, DATEADD(DAY, -1, @today))
			AND h.[Source Code] = 'SALES'
			AND LEN(ISNULL(h.[Customer Source], '')) > 0
			AND charindex('-', l.[No_]) > 0
			AND l.[No_] not like '100-%'
	GROUP BY LEFT(l.[No_], 3), h.[Customer Source]
	UNION
	SELECT LEFT(l.[No_], 3), h.[Customer Source],
			@year+1,
			CAST(SUM(ISNULL(l.[Quantity], 0)*ISNULL(l.[Unit Price], 0)) as decimal(10, 2))
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] h
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] l ON l.[Document No_] = h.[No_]
	WHERE YEAR(h.[Posting Date]) = @year-1
			AND h.[Source Code] = 'SALES'
			AND LEN(ISNULL(h.[Customer Source], '')) > 0
			AND charindex('-', l.[No_]) > 0
			AND l.[No_] not like '100-%'
	GROUP BY LEFT(l.[No_], 3), h.[Customer Source]
	ORDER BY 1, 2 ASC
	
	OPEN catCursor
	FETCH NEXT FROM catCursor INTO @mfgID, @customer, @wk, @sales

	WHILE @@FETCH_STATUS = 0
	   BEGIN
		
		SET @sql = 'UPDATE salesByMFG SET ' + 
						CASE @wk
							WHEN (@year-1) THEN 'LYTD = ' + CAST(@sales AS VARCHAR(20)) 
							WHEN (@year+1) THEN 'LastYear = ' + CAST(@sales AS VARCHAR(20)) 
							ELSE 'YTD = ' + CAST(@sales AS VARCHAR(20)) 
						END + ', hasData = 1 WHERE mfgID = ''' + @mfgID + ''' and salesGroup = ''' + @customer + ''''

		EXEC (@sql)
			
		FETCH NEXT FROM catCursor INTO @mfgID, @customer, @wk, @sales
	   END

	CLOSE catCursor
	DEALLOCATE catCursor
	
	DELETE FROM salesByMFG WHERE hasData = 0
	UPDATE salesByMFG 
	SET delta = 
			CASE 
				WHEN LYTD > 0 THEN (YTD/LYTD)-1
				ELSE 100
			END,
		salesTracking = 52*YTD/@endingWeek 
	WHERE hasData = 1
	
END
GO
