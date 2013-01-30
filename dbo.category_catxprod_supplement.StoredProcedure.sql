USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[category_catxprod_supplement]    Script Date: 01/30/2013 16:26:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[category_catxprod_supplement]
AS
BEGIN
	--INITIATE CATXPRODTEMP
	TRUNCATE TABLE catxprodTemp

	--INSERTING PRODUCT CATEGORIES INTO A TEMP TABLE
	INSERT INTO catxprodTemp(prodCode, catCode, primaryCat, [action])
	select code, category1, 1, 'I' 
	from catxprodWorktable 
	where len(category1) > 0

	INSERT INTO catxprodTemp(prodCode, catCode, primaryCat, [action])
	select code, category2, 0, 'I' 
	from catxprodWorktable
	where len(category2) > 0

	INSERT INTO catxprodTemp(prodCode, catCode, primaryCat, [action])
	select code, category3, 0, 'I' 
	from catxprodWorktable
	where len(category3) > 0

	INSERT INTO catxprodTemp(prodCode, catCode, primaryCat, [action])
	select code, category4, 0, 'I' 
	from catxprodWorktable
	where len(category4) > 0

	--MARK CATEGORIZATION NOT TO IMPORTED FOR EMAILED PURPOSE
	--DON'T IMPORT PRODUCTS THAT ARE NOT ACTIVE
	UPDATE catxprodTemp
	set [action] = 'P'
	WHERE prodCode NOT IN (SELECT code FROM products WHERE active = 1 and isWeb = 1)

	--MARK CATEGORIZATION NOT TO IMPORTED FOR EMAILED PURPOSE
	--DON'T IMPORT PRODUCTS THAT ARE NOT ASSIGNED TO A NON-ACTIVE CATEGORY
	UPDATE catxprodTemp
	set [action] = 'C'
	WHERE catcode NOT IN (SELECT code FROM categories WHERE active = 1 and endleaf = 1)

	--MARK CATEGORIZATION NOT TO IMPORTED FOR EMAILED PURPOSE
	--DON'T IMPORT DUPLICATE CATEGORIZED PROD
	UPDATE cx
	SET cx.[action] = 'D'
	FROM catxprodTemp cx JOIN catxProd c ON cx.catCode = c.catCode AND cx.prodCode = c.prodCode

	--SET ALL CURRENT CATEGORIZATION IN CATXPROD TO PRIMARY = 0
	UPDATE cx
	SET cx.primaryCat = 0
	FROM catxprod cx JOIN catxprodTemp ct ON cx.prodCode = ct.prodCode AND cx.catCode = ct.catCode 
	WHERE ct.primaryCat = 1
	
	--ADDED THE PROD CATEGORIZATION INTO CATXPROD
	INSERT INTO catxProd (catCode, prodCode, [primarycat], nav)
	SELECT catCode, prodCode, [primarycat], 0
	FROM catxprodTemp 
	WHERE [action] IN ('I', 'X')

	-- HOUSE CLEANING
	TRUNCATE TABLE catxprodWorktable 
	
	IF EXISTS(SELECT * FROM catxprodTemp WHERE [action] <>'I')
	   BEGIN
		EXEC dbo.[category_catxprod_supplement_email]
	   END	
	
	TRUNCATE TABLE catxprodTemp
END
GO
