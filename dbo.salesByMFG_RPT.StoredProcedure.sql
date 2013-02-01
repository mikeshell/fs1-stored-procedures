USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[salesByMFG_RPT]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		David Lu
-- Create date: 03/28/2011
-- Description:	Pulling Sales Data by category or Mfg
-- =============================================
CREATE PROCEDURE [dbo].[salesByMFG_RPT]
	@orderBy varchar(255), @salesGroup varchar(50)
AS
BEGIN
	DECLARE @today datetime, @startDT datetime, @endDT datetime, @weekDay tinyint,
			@year Smallint, @month tinyint, @day tinyint, @startingWeek tinyint, @endingWeek tinyint,
			@sql VARCHAR(MAX)

	SET @today = GETDATE()
	SET @weekDay = DATEPART(WEEKDAY, @today)
	SET @endDT = DATEADD(DAY, -@weekday, @today)
	SET @startDT = DATEADD(DAY, -7, DATEADD(WEEK, -8, @endDT))
	SET @startingWeek = DATEPART(WEEK, @startDT)
	SET @year = YEAR(@today)
	SET @month = MONTH(@today)
	SET @day = DAY(@today)
		
	SELECT DATEPART(WEEK, [Posting Date]) weekOfYear, CAST(dbo.getfirstDateOfWeek(DATEPART(WEEK, [Posting Date])) AS VARCHAR(11)) dt
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header]
	WHERE [Posting Date] BETWEEN @startDT AND @endDT
			AND [Source Code] = 'SALES'
			AND LEN(ISNULL([Customer Source], '')) > 0
			AND [Customer Source] = 'I'
	GROUP BY DATEPART(WEEK, [Posting Date]), dbo.getfirstDateOfWeek(DATEPART(WEEK, [Posting Date]))	
	
	SET @sql = 'SELECT mfgID, mfgName, 
				SUM(ISNULL(wk1, 0)) wk1, SUM(ISNULL(wk2, 0)) wk2, SUM(ISNULL(wk3, 0)) wk3, 
				SUM(ISNULL(wk4, 0)) wk4, SUM(ISNULL(wk5, 0)) wk5, SUM(ISNULL(wk6, 0)) wk6, 
				SUM(ISNULL(wk7, 0)) wk7, SUM(ISNULL(wk8, 0)) wk8, SUM(ISNULL(wk9, 0)) wk9,  
				SUM(ISNULL(YTD, 0)) YTD, SUM(ISNULL(LYTD, 0)) LYTD, SUM(ISNULL(lASTYEAR, 0)) lASTYEAR
			FROM salesByMFG
			WHERE salesGroup in (''' + REPLACE(@salesGroup, ', ', ''', ''') + ''')
			GROUP BY mfgID, mfgName
			'
	
	IF LEN(@orderBy) > 0
	   BEGIN
			SET @sql = @sql + 'ORDER BY YTD desc'
	   END
	
	EXEC(@sql)
END
GO
