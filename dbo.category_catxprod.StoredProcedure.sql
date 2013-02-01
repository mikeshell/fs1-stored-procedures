USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[category_catxprod]    Script Date: 02/01/2013 11:09:50 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[category_catxprod]
AS
BEGIN
	TRUNCATE TABLE categoryWorktable
	
	INSERT INTO categoryWorktable(tCatName, tCode, tActive)
	SELECT CASE 
			WHEN c.[Description] IS NULL THEN 'DL-' + LOWER(ic.[Category Code])
			ELSE c.[Description]
		   END, ic.[Item No_], 
		   ic.[Primary Category]
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item Category Codes] ic
		LEFT JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Category Code] c ON c.Code = ic.[Category Code]
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] i ON i.[No_] = ic.[Item No_]
	WHERE [Blocked] = 0
	UNION
	SELECT CASE 
			WHEN c.[Description] IS NULL THEN 'DL-' + LOWER(ic.[Category Code])
			ELSE c.[Description]
		   END, ic.[Item No_], 
		   ic.[Primary Category]
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item Category Codes] ic
		LEFT JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Category Code] c ON c.Code = ic.[Category Code]
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] i ON i.[No_] = ic.[Item No_]
		JOIN products p on p.CODE = i.[No_] COLLATE Latin1_General_CS_AS
	WHERE p.qtyOnHand > 0

	UPDATE c
	SET c.tCatName = cc.[Description]
	FROM categoryWorktable c JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Category Codes] cc 
				ON LOWER(REPLACE(c.tCatName, 'DL-', '')) = LOWER(cc.Code) COLLATE Latin1_General_CS_AS
	WHERE LEFT(tCatName, 3) = 'DL-'

	UPDATE categoryWorktable 
	SET tCatName = REPLACE(tCatName, 'DL-', '')
	WHERE LEFT(tCatName, 3) = 'DL-'

	--DELETING ALL CATEGORIZATION FOR ALL NON ACTIVE CATEGORY
	DELETE FROM categoryWorktable
	WHERE tCatName NOT IN (SELECT code FROM categories WHERE ACTIVE = 1)

	--DELETING ALL CATEGORIZATION FOR ALL NON ACTIVE PRODUCTS
	DELETE FROM categoryWorktable
	WHERE tCode NOT IN (SELECT code FROM products)
	
	--REMOVE DUPLICATE CATEGORIZATION WHERE PRIMARYCAT FLAG SUPERSEDE ALL
	DECLARE @catName VARCHAR(1000), @prodCode VARCHAR(1000), @tID INT
	
	DECLARE cCursor CURSOR FOR  	
	SELECT tCatName, tCode
	FROM categoryWorktable
	GROUP BY tCatName, tCode
	HAVING COUNT(*) > 1
		
	OPEN cCursor
	FETCH NEXT FROM cCursor INTO @catName, @prodCode

	WHILE @@FETCH_STATUS = 0
	   BEGIN
		SET @tID = ''
		
		SELECT TOP 1 @tID = tID
		FROM categoryWorktable
		WHERE tCatName = @catName AND tCode = @prodCode 
		ORDER BY tActive DESC
		
		DELETE FROM categoryWorktable
		WHERE tCatName = @catName AND tCode = @prodCode AND tID <> @tID

		FETCH NEXT FROM cCursor INTO @catName, @prodCode
	   END

	CLOSE cCursor
	DEALLOCATE cCursor
	

	--DELETING ALL CATEGORIZATION FROM CATXPROD TABLE IF IT DOESNT EXIST IN NAV	
	DELETE cx
	FROM categoryWorktable cw RIGHT JOIN catxProd cx ON cw.tCatName = cx.catCode AND cw.tCode = cx.prodCode
	WHERE cw.tCatName IS NULL AND nav = 1
	
	--UPDATING PRIMARYCAT FLAG
	/**
	UPDATE cx
	SET cx.primaryCat = cw.tActive	
	FROM categoryWorktable cw JOIN catxProd cx ON cw.tCatName = cx.catCode AND cw.tCode = cx.prodCode
	WHERE cx.primaryCat <> cw.tActive AND nav = 1
	**/
	
	--DELETING ALL MATCHING RECORD BETWEEN CATEGORYWORKATBLE AND CATXPROD	
	DELETE cw
	FROM categoryWorktable cw JOIN catxProd cx ON cw.tCatName = cx.catCode AND cw.tCode = cx.prodCode
	
	--INSERTING NEW CATEGORIZATION INTO CATXPROD
	INSERT INTO catxProd(catCode, prodCode, primaryCat, nav)
	SELECT tCatName, tCode, tActive, 1 FROM categoryWorktable
	
	TRUNCATE TABLE categoryWorktable
	
	--HOLIDAY CATEGORIZATION
	--EXEC dbo.[category_holiday]
	
	--CATEGORIZATION OUTSIDE OF NAV
	EXEC [dbo].[category_catxprod_supplement]	
	
	--ADDING VENDOR CATEGORIZATION AT THE VENDOR LEVEL IF THEY ARE NOT CATEGORIZED
	EXEC [dbo].[category_catxprod_vendor]	
	
	--CATEGORIZATION FOR SALES AND NAFED PRODUCTS
	EXEC [dbo].[category_catxprod_sales]
	
	UPDATE cx
	SET cx.catID = c.id
	FROM catxProd cx JOIN categories c ON cx.catCode = c.CODE

	UPDATE cx
	SET cx.prodID = p.prodid
	FROM catxProd cx JOIN products p ON cx.prodCode = p.code
	
	--DELETE ALL CATEGORIZATION TO NONE ENDLEAF	
	DELETE FROM catxprod
	WHERE catCode IN (SELECT code FROM categories WHERE endleaf = 0)
	
	--CATXPROD CLEAN UP. 
	--1. REMOVING DUP ENTRY 
	--2. REASSIGNED PRIMARYCAT DESIGNATION IF PRIMARYCAT = 0 AND THERE IS ONLY 1 ENTRY
	EXEC dbo.[category_catxprod_cleanUp]
	
	--SETTING PRIMARY 
	UPDATE products SET primaryCatCode = NULL, primaryCatID = NULL

	UPDATE p 
	SET p.primaryCatCode = cp.catCode, p.primaryCatID = cp.catID
	FROM catxprod cp JOIN products p ON cp.prodCode = p.CODE
	WHERE cp.primarycat = 1
	
END
GO
