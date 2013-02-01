USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[cat_and_mfg_weekly_stat]    Script Date: 02/01/2013 11:09:50 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[cat_and_mfg_weekly_stat]
AS
BEGIN
	--UPDATE THIS WEEK MFG PERFORMANCE	
	UPDATE mfg SET week2 = NULL
	UPDATE mfg SET week2 = week1
	UPDATE mfg SET week1 = NULL
		
	TRUNCATE TABLE mfgWorktable
	INSERT INTO mfgWorktable(mfgid, salesTT)
	SELECT CAST(LEFT(il.[No_], 3) AS VARCHAR(3)), SUM(il.[Amount])
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] i
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] il 
			ON i.[No_]  = il.[Document No_]
		LEFT JOIN mfg m ON m.mfgID = LEFT(il.[No_], 3) COLLATE Latin1_General_CS_AS
	WHERE datepart(week, getdate()) = datepart(week, i.[Order Date])
		AND YEAR(i.[Order Date]) = YEAR(getdate())	
		AND il.[No_] in (SELECT [No_] FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] WHERE LEN(ISNULL([No_], '')) > 3)
		AND il.[No_] NOT LIKE '100-%'
	GROUP BY LEFT(il.[No_], 3)

	UPDATE m
	SET m.week1 = w.salesTT
	FROM mfg m JOIN mfgworktable w on m.mfgid = w.mfgid

	--CALCULATING THIS WEEK CAT STAT 
	UPDATE categories SET week2 = NULL
	UPDATE categories SET week2 = week1
	UPDATE categories SET week1 = NULL
	
	Declare @total money
			
	TRUNCATE TABLE mfgworktable 
	INSERT INTO mfgworktable(mfgid, salesTT)
	SELECT CASE 
				WHEN p.primaryCatcode IS NULL THEN 'uncat'
				ELSE p.primaryCatcode
			END catCode, 
			SUM(il.[Amount])
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] i
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] il 
			ON i.[No_]  = il.[Document No_]
		LEFT JOIN products p ON p.code = il.[No_] COLLATE Latin1_General_CS_AS
	WHERE datepart(week, i.[Order Date]) = datepart(week, getdate())
		AND year(i.[Order Date]) = YEAR(getdate())	
		AND il.[No_] in (SELECT [No_] FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] WHERE LEN(ISNULL([No_], '')) > 3)
		AND il.[No_] NOT LIKE '100-%'
	GROUP BY p.primaryCatcode
	ORDER BY 1
	
	SELECT @total = SUM(il.[Amount])
		FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] i
			JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] il 
				ON i.[No_]  = il.[Document No_]
		WHERE datepart(week, i.[Order Date]) = datepart(week, getdate())
			AND year(i.[Order Date]) = YEAR(getdate())	
			AND il.[No_] IN (SELECT [No_] FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] WHERE LEN(ISNULL([No_], '')) > 3)
		AND il.[No_] NOT LIKE '100-%'
			AND il.[No_] NOT IN (SELECT code COLLATE SQL_Latin1_General_CP1_CI_AS FROM products)	

	UPDATE mfgworktable
	SET salesTT = ISNULL(salesTT, 0) + @total	
	WHERE mfgid = 'uncat'

	UPDATE c
	SET c.week1 = m.salesTT
	FROM categories c JOIN mfgworktable m ON c.CODE = m.mfgid

	SET @total = 0
	TRUNCATE TABLE mfgworktable 
	INSERT INTO mfgworktable(mfgid, salesTT)
	SELECT CASE 
				WHEN p.primaryCatcode IS NULL THEN 'uncat'
				ELSE p.primaryCatcode
			END catCode, 
			SUM(il.[Amount])
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] i
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] il 
			ON i.[No_]  = il.[Document No_]
		LEFT JOIN products p ON p.code = il.[No_] COLLATE Latin1_General_CS_AS
	WHERE DATEDIFF(DAY, i.[Order Date], GETDATE()) <31
		AND il.[No_] in (SELECT [No_] FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] WHERE LEN(ISNULL([No_], '')) > 3)
		AND il.[No_] NOT LIKE '100-%'
	GROUP BY p.primaryCatcode
	ORDER BY 1

	SELECT @total = SUM(il.[Amount])
		FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] i
			JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] il 
				ON i.[No_]  = il.[Document No_]
		WHERE DATEDIFF(DAY, i.[Order Date], GETDATE()) <31
			AND il.[No_] IN (SELECT [No_] FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] WHERE LEN(ISNULL([No_], '')) > 3)
		AND il.[No_] NOT LIKE '100-%'
			AND il.[No_] NOT IN (SELECT code COLLATE SQL_Latin1_General_CP1_CI_AS FROM products)	

	UPDATE mfgworktable
	SET salesTT = ISNULL(salesTT, 0) + @total	
	WHERE mfgid = 'uncat'

	UPDATE c
	SET c.sales30 = m.salesTT
	FROM categories c JOIN mfgworktable m ON c.CODE = m.mfgid
	
	SET @total = 0
	TRUNCATE TABLE mfgworktable 
	INSERT INTO mfgworktable(mfgid, salesTT)
	SELECT CASE 
				WHEN p.primaryCatcode IS NULL THEN 'uncat'
				ELSE p.primaryCatcode
			END catCode, 
			SUM(il.[Amount])
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] i
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] il 
			ON i.[No_]  = il.[Document No_]
		LEFT JOIN products p ON p.code = il.[No_] COLLATE Latin1_General_CS_AS
	WHERE DATEDIFF(DAY, i.[Order Date], GETDATE()) BETWEEN 31 AND 60
		AND il.[No_] in (SELECT [No_] FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] WHERE LEN(ISNULL([No_], '')) > 3)
		AND il.[No_] NOT LIKE '100-%'
	GROUP BY p.primaryCatcode
	ORDER BY 1

	SELECT @total = SUM(il.[Amount])
		FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] i
			JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] il 
				ON i.[No_]  = il.[Document No_]
		WHERE DATEDIFF(DAY, i.[Order Date], GETDATE()) BETWEEN 31 AND 60
			AND il.[No_] IN (SELECT [No_] FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] WHERE LEN(ISNULL([No_], '')) > 3)
		AND il.[No_] NOT LIKE '100-%'
			AND il.[No_] NOT IN (SELECT code COLLATE SQL_Latin1_General_CP1_CI_AS FROM products)	

	UPDATE mfgworktable
	SET salesTT = ISNULL(salesTT, 0) + @total	
	WHERE mfgid = 'uncat'

	UPDATE c
	SET c.sales3060 = m.salesTT
	FROM categories c JOIN mfgworktable m ON c.CODE = m.mfgid

END
GO
