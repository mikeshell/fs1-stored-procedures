USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[salesByMedium_RPT]    Script Date: 01/30/2013 16:26:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		David Lu
-- Create date: 03/28/2011
-- Description:	Pulling Sales Data by category or Mfg
-- =============================================
CREATE PROCEDURE [dbo].[salesByMedium_RPT]
	@salesGroup varchar(50)
AS
BEGIN
	DECLARE @today datetime, @startDT datetime, @endDT datetime, @weekDay tinyint,
			@year Smallint, @month tinyint, @day tinyint, @startingWeek tinyint, @endingWeek tinyint,
			@sql VARCHAR(MAX)

	SET @today = GETDATE()
	SET @weekDay = DATEPART(WEEKDAY, @today)
	SET @endDT = DATEADD(DAY, -@weekday, @today)
	SET @startDT = DATEADD(DAY, -7, DATEADD(WEEK, -5, @endDT))
	SET @startingWeek = DATEPART(WEEK, @startDT)
	SET @year = YEAR(@today)
	SET @month = MONTH(@today)
	SET @day = DAY(@today)
		
	SELECT CAST(dbo.getfirstDateOfWeek2([Posting Date]) AS VARCHAR(11)) dt
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header]
	WHERE [Posting Date] BETWEEN @startDT AND @today
			AND [Source Code] = 'SALES'
			AND LEN(ISNULL([Customer Source], '')) > 0
			AND [Customer Source] = 'I'
	GROUP BY dbo.getfirstDateOfWeek2([Posting Date])
	
	-- SALES BY MEDIUM	
	SELECT orderMedium, ISNULL([W1os], 0) [W1os], ISNULL([W1oo], 0) [W1oo], ISNULL([W1is], 0) [W1is], ISNULL([W1io], 0) [W1io],
				ISNULL([W2os], 0) [W2os], ISNULL([W2oo], 0) [W2oo], ISNULL([W2is], 0) [W2is], ISNULL([W2io], 0) [W2io],
				ISNULL([W3os], 0) [W3os], ISNULL([W3oo], 0) [W3oo], ISNULL([W3is], 0) [W3is], ISNULL([W3io], 0) [W3io],
				ISNULL([W4os], 0) [W4os], ISNULL([W4oo], 0) [W4oo], ISNULL([W4is], 0) [W4is], ISNULL([W4io], 0) [W4io],
				ISNULL([W5os], 0) [W5os], ISNULL([W5oo], 0) [W5oo], ISNULL([W5is], 0) [W5is], ISNULL([W5io], 0) [W5io],
				ISNULL([W6os], 0) [W6os], ISNULL([W6oo], 0) [W6oo],	ISNULL([W6is], 0) [W6is], ISNULL([W6io], 0) [W6io],
				ISNULL([WTDos], 0) [WTDos], ISNULL([WTDoo], 0) [WTDoo],	ISNULL([WTDis], 0) [WTDis], ISNULL([WTDio], 0) [WTDio],
				ISNULL([W1os], 0) + ISNULL([W2os], 0) + ISNULL([W3os], 0) + ISNULL([W4os], 0) + ISNULL([W5os], 0) + ISNULL([W6os], 0) [osalesTT],
				ISNULL([W1is], 0) + ISNULL([W2is], 0) + ISNULL([W3is], 0) + ISNULL([W4is], 0) + ISNULL([W5is], 0) + ISNULL([W6is], 0) [isalesTT],
				ISNULL([W1oo], 0) + ISNULL([W2oo], 0) + ISNULL([W3oo], 0) + ISNULL([W4oo], 0) + ISNULL([W5oo], 0) + ISNULL([W6oo], 0) [oorderTT],
				ISNULL([W1io], 0) + ISNULL([W2io], 0) + ISNULL([W3io], 0) + ISNULL([W4io], 0) + ISNULL([W5io], 0) + ISNULL([W6io], 0) [iorderTT]
	FROM salesRptByMedium
	
	-- SALES BY SALESMEN	
	SET @sql = 'SELECT CASE 
						WHEN e.employeeName IS NULL THEN orderMedium
						ELSE e.employeeName
					   END orderMedium, e.department, 
					   ISNULL([W1os], 0) [W1os], ISNULL([W1oo], 0) [W1oo], ISNULL([W1is], 0) [W1is], ISNULL([W1io], 0) [W1io],
						ISNULL([W2os], 0) [W2os], ISNULL([W2oo], 0) [W2oo], ISNULL([W2is], 0) [W2is], ISNULL([W2io], 0) [W2io],
						ISNULL([W3os], 0) [W3os], ISNULL([W3oo], 0) [W3oo], ISNULL([W3is], 0) [W3is], ISNULL([W3io], 0) [W3io],
						ISNULL([W4os], 0) [W4os], ISNULL([W4oo], 0) [W4oo], ISNULL([W4is], 0) [W4is], ISNULL([W4io], 0) [W4io],
						ISNULL([W5os], 0) [W5os], ISNULL([W5oo], 0) [W5oo], ISNULL([W5is], 0) [W5is], ISNULL([W5io], 0) [W5io],
						ISNULL([W6os], 0) [W6os], ISNULL([W6oo], 0) [W6oo],	ISNULL([W6is], 0) [W6is], ISNULL([W6io], 0) [W6io],
						ISNULL([WTDos], 0) [WTDos], ISNULL([WTDoo], 0) [WTDoo],	ISNULL([WTDis], 0) [WTDis], ISNULL([WTDio], 0) [WTDio],
						ISNULL([W1os], 0) + ISNULL([W2os], 0) + ISNULL([W3os], 0) + ISNULL([W4os], 0) + ISNULL([W5os], 0) + ISNULL([W6os], 0) [osalesTT],
						ISNULL([W1is], 0) + ISNULL([W2is], 0) + ISNULL([W3is], 0) + ISNULL([W4is], 0) + ISNULL([W5is], 0) + ISNULL([W6is], 0) [isalesTT],
						ISNULL([W1oo], 0) + ISNULL([W2oo], 0) + ISNULL([W3oo], 0) + ISNULL([W4oo], 0) + ISNULL([W5oo], 0) + ISNULL([W6oo], 0) [oorderTT],
						ISNULL([W1io], 0) + ISNULL([W2io], 0) + ISNULL([W3io], 0) + ISNULL([W4io], 0) + ISNULL([W5io], 0) + ISNULL([W6io], 0) [iorderTT]
				FROM salesRptBySalesmen s left join employee e on LOWER(s.orderMedium) = LOWER(e.empid)
				WHERE e.department IN (''' + REPLACE(@salesGroup, ', ', ''', ''') + ''')
				ORDER BY ISNULL([WTDis], 0) + ISNULL([WTDos], 0) DESC'
	EXEC(@sql)
END
GO
