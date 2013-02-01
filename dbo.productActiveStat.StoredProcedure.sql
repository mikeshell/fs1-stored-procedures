USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[productActiveStat]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[productActiveStat]
AS
BEGIN
	TRUNCATE TABLE productStat
	TRUNCATE TABLE productStat_worktable

	-- TOTAL PRODUCTS
	INSERT INTO productStat(mfgid, mfgName, numProdInNAV)
	SELECT LEFT(i.[No_], 3), v.[Name], COUNT(*)
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] i
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Vendor] v ON LEFT(i.[No_], 3) = v.[No_]
	WHERE [Vendor Posting Group] IN ('AP-DIST', 'AP-MAN')
	GROUP BY LEFT(i.[No_], 3), v.[Name]

	--TOTAL DISCONTINUED PRODUCTS
	INSERT INTO productStat_worktable
	SELECT v.[No_], COUNT(*)
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] i
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Vendor] v ON LEFT(i.[No_], 3) = v.[No_]
	WHERE [Status] = 2
		AND [Vendor Posting Group] IN ('AP-DIST', 'AP-MAN')
	GROUP BY v.[No_]

	UPDATE t
	SET t.discontinuedNAV = ISNULL(t2.total, 2)
	FROM productStat t JOIN productStat_worktable t2 ON t.mfgid = t2.mfgid

	TRUNCATE TABLE productStat_worktable

	--TOTAL DISCONTINUED PRODUCTS
	INSERT INTO productStat_worktable
	SELECT mfgid, COUNT(*)
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] i
		JOIN products p ON p.CODE = i.[No_] COLLATE Latin1_General_CS_AS
	WHERE [Status] = 2
		AND qtyOnHand > 0
	GROUP BY mfgid

	UPDATE t
	SET t.discontinuedButActive = ISNULL(t2.total, 2)
	FROM productStat t JOIN productStat_worktable t2 ON t.mfgid = t2.mfgid

	TRUNCATE TABLE productStat_worktable

	--TOTAL [WEB ITEM] = 1 PRODUCTS
	INSERT INTO productStat_worktable
	SELECT v.[No_], COUNT(*)
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] i
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Vendor] v ON LEFT(i.[No_], 3) = v.[No_]
	WHERE [Status] <> 2 AND [Web Item] = 1
		AND [Vendor Posting Group] IN ('AP-DIST', 'AP-MAN')
	GROUP BY v.[No_]

	UPDATE t
	SET t.webItemNAV = ISNULL(t2.total, 2)
	FROM productStat t JOIN productStat_worktable t2 ON t.mfgid = t2.mfgid

	TRUNCATE TABLE productStat_worktable

	--TOTAL liveOnWeb PRODUCTS
	INSERT INTO productStat_worktable
	SELECT mfgid, COUNT(*)
	FROM products
	WHERE ACTIVE = 1 AND isWeb = 1
	GROUP BY mfgid

	UPDATE t
	SET t.liveOnWeb = ISNULL(t2.total, 2)
	FROM productStat t JOIN productStat_worktable t2 ON t.mfgid = t2.mfgid

	TRUNCATE TABLE productStat_worktable

	--TOTAL notCatNAV PRODUCTS
	INSERT INTO productStat_worktable
	SELECT LEFT([No_], 3), COUNT(*)
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item]
	WHERE [No_] IN (SELECT [Item No_] FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item Category Codes])
	GROUP BY LEFT([No_], 3)

	UPDATE t
	SET t.notCatNAV = numProdInNAV - ISNULL(t2.total, 2)
	FROM productStat t JOIN productStat_worktable t2 ON t.mfgid = t2.mfgid

	TRUNCATE TABLE productStat_worktable

	--TOTAL NOT CATEGORIZED
	INSERT INTO productStat_worktable
	SELECT mfgid, COUNT(*)
	FROM products
	WHERE ACTIVE = 1 
		and [isWeb] = 1
		and primaryCatCode is null
	GROUP BY mfgid, mfgname
	ORDER BY mfgid

	UPDATE t
	SET t.notCatOnWEB = ISNULL(t2.total, 2)
	FROM productStat t JOIN productStat_worktable t2 ON t.mfgid = t2.mfgid

	TRUNCATE TABLE productStat_worktable
END
GO
