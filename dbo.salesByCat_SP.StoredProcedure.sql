USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[salesByCat_SP]    Script Date: 01/30/2013 16:26:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		David Lu
-- Create date: 03/28/2011
-- Description:	Pulling Sales Data by category or Mfg
-- =============================================
CREATE PROCEDURE [dbo].[salesByCat_SP]
	@cat varchar(255)
AS
BEGIN
	DECLARE @today datetime, @startDT datetime, @endDT datetime, @weekDay tinyint,
			@year Smallint, @month tinyint, @day tinyint, @startingWeek tinyint, @endingWeek tinyint

	SET @today = GETDATE()
	SET @weekDay = DATEPART(WEEKDAY, @today)
	SET @endDT = DATEADD(DAY, -@weekday, @today)
	SET @startDT = DATEADD(DAY, -7, DATEADD(WEEK, -8, @endDT))
	SET @startingWeek = DATEPART(WEEK, @startDT)
	SET @year = YEAR(@today)
	SET @month = MONTH(@today)
	SET @day = DAY(@today)
	
	IF LEN(@cat) > 0
	   BEGIN		
		SELECT ISNULL(c.superCat, 'Not Categorized') category
		FROM products p LEFT JOIN categories c ON p.primaryCatCode = c.CODE
		WHERE p.active = 1
		GROUP BY c.superCat
	   END
	
	SELECT YEAR([Posting Date]) yyyy, DATEPART(WEEK, [Posting Date]) weekOfYear, CAST(dbo.getfirstDateOfWeek2([Posting Date]) AS VARCHAR(11)) dt
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header]
	WHERE [Posting Date] BETWEEN @startDT AND @endDT
			AND [Source Code] = 'SALES'
			AND LEN(ISNULL([Customer Source], '')) > 0
			AND [Customer Source] = 'I'
	GROUP BY YEAR([Posting Date]), DATEPART(WEEK, [Posting Date]),CAST(dbo.getfirstDateOfWeek2([Posting Date]) AS VARCHAR(11))
	ORDER BY 1, 2
		
	IF LEN(@cat) = 0
	   BEGIN
		SELECT ISNULL(c.superCat, 'Not Categorized') category,
				DATEPART(WEEK, h.[Posting Date]) weekOfYear,
				CAST(SUM(ISNULL(l.[Quantity], 0)*ISNULL(l.[Unit Price], 0)) as decimal(10, 2)) salesTT
		FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] h
			JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] l ON l.[Document No_] = h.[No_]
			LEFT JOIN products p ON p.CODE = l.[No_] collate Latin1_General_CS_AS
			LEFT JOIN categories c ON p.primaryCatCode = c.CODE
		WHERE h.[Posting Date] BETWEEN @startDT AND @endDT
				AND h.[Source Code] = 'SALES'
				AND LEN(ISNULL(h.[Customer Source], '')) > 0
				AND h.[Customer Source] = 'I'
				AND l.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING', '100-REFUNDS')
		GROUP BY DATEPART(WEEK, h.[Posting Date]), dbo.getfirstDateOfWeek(DATEPART(WEEK, h.[Posting Date])), c.superCat
		UNION
		SELECT ISNULL(c.superCat, 'Not Categorized') category,
				@year,
				CAST(SUM(ISNULL(l.[Quantity], 0)*ISNULL(l.[Unit Price], 0)) as decimal(10, 2))
		FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] h
			JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] l ON l.[Document No_] = h.[No_]
			LEFT JOIN products p ON p.CODE = l.[No_] collate Latin1_General_CS_AS
			LEFT JOIN categories c ON p.primaryCatCode = c.CODE
		WHERE h.[Posting Date] BETWEEN '1/1/' + CAST(@year as varchar(4)) AND DATEADD(DAY, -1, @today)
				AND h.[Source Code] = 'SALES'
				AND LEN(ISNULL(h.[Customer Source], '')) > 0
				AND h.[Customer Source] = 'I'
				AND l.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING', '100-REFUNDS')
		GROUP BY c.superCat
		UNION
		SELECT ISNULL(c.superCat, 'Not Categorized') category,
				@year-1,
				CAST(SUM(ISNULL(l.[Quantity], 0)*ISNULL(l.[Unit Price], 0)) as decimal(10, 2))
		FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] h
			JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] l ON l.[Document No_] = h.[No_]
			LEFT JOIN products p ON p.CODE = l.[No_] collate Latin1_General_CS_AS
			LEFT JOIN categories c ON p.primaryCatCode = c.CODE
		WHERE h.[Posting Date] BETWEEN '1/1/' + CAST((@year-1) as varchar(4)) AND DATEADD(YEAR, -1, DATEADD(DAY, -1, @today))
				AND h.[Source Code] = 'SALES'
				AND LEN(ISNULL(h.[Customer Source], '')) > 0
				AND h.[Customer Source] = 'I'
				AND l.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING', '100-REFUNDS')
		GROUP BY c.superCat
		ORDER BY 1, 2 ASC
			
		SELECT ISNULL(c.superCat, 'Not Categorized') category, COUNT(*) prodCount
		FROM products p LEFT JOIN categories c ON p.primaryCatCode = c.CODE
		WHERE p.active = 1
		GROUP BY c.superCat
	   END
	 ELSE		
	   BEGIN		
		SELECT isnull(m.mfgName, 'NO  MFG INFO') mfgName,
				DATEPART(WEEK, h.[Posting Date]) weekOfYear,
				CAST(SUM(ISNULL(l.[Quantity], 0)*ISNULL(l.[Unit Price], 0)) as decimal(10, 2)) salesTT
		FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] h
			JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] l ON l.[Document No_] = h.[No_]
			LEFT JOIN products p ON p.CODE = l.[No_] collate Latin1_General_CS_AS
			LEFT JOIN categories c ON p.primaryCatCode = c.CODE
			LEFT JOIN mfg m ON m.mfgID = LEFT(l.[No_], 3) collate Latin1_General_CS_AS
		WHERE h.[Posting Date] BETWEEN @startDT AND @endDT
				AND h.[Source Code] = 'SALES'
				AND LEN(ISNULL(h.[Customer Source], '')) > 0
				AND h.[Customer Source] = 'I'
				AND l.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING', '100-REFUNDS', 'B3')
				AND LEN(l.[No_]) > 0
				AND ISNULL(c.superCat, 'Not Categorized')  = @cat
		GROUP BY DATEPART(WEEK, h.[Posting Date]), dbo.getfirstDateOfWeek(DATEPART(WEEK, h.[Posting Date])), m.mfgName		
		ORDER BY 1, 2 ASC
	   END
END
GO
