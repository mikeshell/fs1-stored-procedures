USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[category_catxprod_vendor]    Script Date: 01/30/2013 16:26:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[category_catxprod_vendor]
AS
BEGIN
	--CREATING THE LIST OF MFG WITH ACTIVE PRODUCTS TO WORK WITH
	--productTMP FIELDS DEFINITION
	--mfgID = MFGID
	--mfgName = MFGNAME
	--active = CATID ASSOCIATED TO THE MFG
	--primaryCatID = IDENTIFY IF THE CATEOGRY EXISTS FOR THE DESIGNATED MFGID
	--mivaProdID = NUMBER OF PRODUCTS FOR THE DESIGNATED MFG
	TRUNCATE TABLE productTMP

	INSERT INTO productTMP(mfgID, mfgName, active, primaryCatID, mivaProdID)
	SELECT p.mfgID, p.mfgName, 
			CASE 
				WHEN c.catname IS NULL THEN 0
				ELSE 1
			END, c.id, COUNT(DISTINCT p.CODE) 
	FROM products p LEFT JOIN categories c ON c.mfgid = p.mfgid AND c.ACTIVE = 1
	WHERE p.active = 1 
		AND p.isWeb = 1
	GROUP BY p.mfgID, p.mfgName, 
			CASE 
				WHEN c.catname IS NULL THEN 0
				ELSE 1
			END, c.id

	--EXCLUDE ALL MFG CATEGORIES WITH CHILDREN
	DELETE FROM productTMP
	WHERE primaryCatID IN 
			(
			SELECT ct.primaryCatID
			FROM productTMP ct JOIN categories c on ct.primaryCatID = c.PARENT_ID
			GROUP BY ct.primaryCatID
			)
			
	--REMOVE ALL MFG WHERE THEY HAVE FULLY BEEN CATEGORIZED 
	DELETE ct
	FROM productTMP ct JOIN categories c on ct.primaryCatID = c.ID 
	WHERE ct.active = 1
		AND ct.mivaProdID = c.numProd

	--CREATING NEW CATEGORY FOR VENDOR WITH NO CATEGORIZATION
	TRUNCATE TABLE catTemp

	INSERT INTO catTemp
	SELECT dbo.RemoveNonAlphaNumericCharacters(mfgName), 'shop-by-vendor', mfgName, 1, 5844
	FROM productTMP 
	WHERE active = 0

	EXEC dbo.addNewCategory

	UPDATE p
	SET p.primaryCatID = c.id,
		p.ACTIVE = 1
	FROM productTMP p JOIN categories c ON p.mfgName = c.CATNAME
	WHERE p.primaryCatID IS NULL

	--ADDING CATEGORIZATION FOR VENDOR CAT INTO CATXPROD
	INSERT INTO catxprod(catID, catCode, prodID, prodCode, primaryCat, nav)
	SELECT pt.primaryCatID, c.CODE, p.prodid, p.CODE, 0, 0
	FROM productTMP pt JOIN products p ON pt.mfgID = p.mfgID
						JOIN categories c ON c.ID = pt.primaryCatID AND c.primaryCat = 1
						LEFT JOIN catxprod cp ON cp.catID = pt.primaryCatID AND cp.prodID = p.prodid
	WHERE p.active = 1 
		AND p.isWeb = 1
		AND cp.catID IS NULL
END
GO
