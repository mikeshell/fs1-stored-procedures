USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[SLIFeed_SP]    Script Date: 01/30/2013 16:26:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[SLIFeed_SP]
AS
BEGIN

	DECLARE @jobID INT
	INSERT INTO jobHistory(jobName, startdT) values('SEARCH_FEED', GETDATE())
	SET @jobID = @@identity

	TRUNCATE TABLE SLIFeed

	INSERT INTO SLIFeed (prodID, [Product Number], [Product Name], [Product URL], [Description], [Product Image],[Product Thumbnail Image], [Price], [Accessory], lastProcessed, inventoryCount, contactus)
	SELECT
		mivaProdid,
		code,
		[Name],
		'http://www.katom.com/' + code + '.html',
		CASE
			WHEN extendedDescription IS NULL THEN dbo.cleaner(prodDesc)
			ELSE dbo.cleaner(extendedDescription)
		END,
		'http://' + mfgid + '.katomcdn.com/' + LOWER([image]) + '_large.jpg',
		'http://' + mfgid + '.katomcdn.com/' + LOWER([image]) + '.jpg',
		CAST(Price as decimal(10, 2)),
		0,
		updateDT,
		qtyOnHand, 0	
	FROM products
	WHERE active = 1 and isWeb =1 

	DECLARE @SQL VARCHAR(500), @sli VARCHAR(100), @refineable INT

	SET @sql = ''

	DECLARE aCursor CURSOR FOR 
	SELECT SLIColumn FROM sliColDef

	OPEN aCursor
	FETCH NEXT FROM aCursor INTO @sli

	WHILE @@FETCH_STATUS = 0
	   BEGIN
			IF (@refineable = 31)
			   BEGIN
				SET @sql = 'UPDATE s ' +
							'SET s.[' + @sli + '] = r.[Refinable Value] ' +
							'FROM SLIFeed s JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item Refinables] r ' + 
										' ON s.[Product Number] = r.[Item No_] COLLATE Latin1_General_CS_AS ' +
							'WHERE LOWER([Refinable No_]) = LOWER(''' + CAST(@sli AS VARCHAR(100)) + ''') AND ' +
								'LOWER([Refinable No_]) NOT LIKE ''%related-items%'' AND LOWER([Refinable No_]) NOT LIKE ''%Accessories%'''
			   END
			ELSE
			   BEGIN
				SET @sql = 'UPDATE s ' +
							'SET s.[' + @sli + '] = r.[Refinable Value] ' +
							'FROM SLIFeed s JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item Refinables] r ' + 
										' ON s.[Product Number] = r.[Item No_] COLLATE Latin1_General_CS_AS ' +
							'WHERE LOWER([Refinable No_]) = LOWER(''' + CAST(@sli AS VARCHAR(100)) + ''')'
			   END

			EXEC(@sql)
		FETCH NEXT FROM aCursor INTO @sli
	   END

	CLOSE aCursor
	DEALLOCATE aCursor

	--SETTING FREE SHIPPING FLAG
	UPDATE s
	SET s.freeshipping = p.FreeShipping
	FROM SLIFeed s JOIN products p ON s.[Product Number] = p.CODE
	
	--SETTING UOM
	UPDATE s
	SET s.[Priced Per:] = p.uom
	FROM SLIFeed s JOIN products p ON s.[Product Number] = p.CODE
	
	--MARKING SPECIAL PRICING FOR MAP PRICING PRODUCTS
	UPDATE SLIFeed
	SET specialPrice = 1
	WHERE [Product Number] IN (SELECT Code FROM products WHERE ISNULL(MAP, 0) = 1)
	
	UPDATE SLIFeed
	SET specialPrice = 1				 
	WHERE LEFT([Product Number], 3) ='599'
	
	--ENSURING PRICE IS CORRECT
	UPDATE s
	SET s.[Price] = CAST(p.[Price] as decimal(10, 2))
	FROM products p JOIN sliFeed s on p.code = s.[Product Number]
	WHERE CAST(p.[Price] as decimal(10, 2)) <> CAST(s.[Price] as decimal(10, 2))
	
	
	--CORRECTING PRICING AND SPECIAL PROGRAM MARKING
	UPDATE s
	SET s.specialPrice = 0
	FROM SLIFeed s join products p on s.[Product Number] = p.CODE
	WHERE ISNULL(p.MAP_Program, 'K') = 'K'  

	--MARKING onSale product		
	UPDATE f
	SET onSale = 1 
	FROM SLIFeed f JOIN products p on f.[Product Number] = p.CODE
	WHERE onSale = 0 AND p.clearance = 1
	

	--POPULATING CATEGORIES INFO, MARKING IF IT'S A NEW PRODUCT, SALESRANK AND FREE LIFTGATE
	UPDATE s
	SET s.prodCat = p.categories,
		s.salesRank = p.salesRank,
		s.newProduct = 
			CASE
				WHEN DATEDIFF(DAY, p.addeddt, GETDATE()) < 31 THEN 1
				ELSE 0	
			END,
		s.freeLiftgate = 0
	FROM SLIFeed s JOIN products p ON s.[Product Number] = p.code

	UPDATE s
	SET s.navCategory = p.categories,
		s.catID = REPLACE(p.catCODE, ', ', '|')
	FROM SLIFeed s JOIN products p ON s.[Product Number] = p.code
--	WHERE p.categories NOT LIKE '%Related Items%'

	UPDATE s
	SET s.primarycatcode = c.CODE,
		s.sisterCat = c.sisterCat,
		s.sisterCatID = c.sisterCatID		
	FROM SLIFeed s JOIN products p ON s.[Product Number] = p.code
				   JOIN categories c ON c.id = p.primaryCatID
--	WHERE c.catname NOT LIKE '%Related Items%' AND catname NOT LIKE '%Accessories%' 

	--REMOVE ALL RELATED ITEMS CAT FROM CATEGORIZATION
	UPDATE SLIFeed SET navCategory = NULL, catID = NULL WHERE navCategory = 'Related Items'
	
	UPDATE SLIFeed 
	SET navCategory = REPLACE(navCategory, '|Related Items', ''), 
		catID = REPLACE(navCategory, '|related-items', '') 
	WHERE CHARINDEX('|Related Items', navCategory) > 0
	
	UPDATE SLIFeed 
	SET navCategory = REPLACE(navCategory, 'Related Items|', ''), 
		catID = REPLACE(navCategory, 'related-items|', '') 
	WHERE CHARINDEX('Related Items|', navCategory) > 0
	
	--ASSIGNING THE CLEARANCE CATEGORY ONLY, IF THE ITEM IS ALSO MARKED AS RELATED ITEM	
	UPDATE s
	SET s.primarycatcode = replace(p.catCode, 'related-items, ', ''),
		s.sisterCat = c.sisterCat,
		s.sisterCatID = c.sisterCatID,
		s.navCategory = p.categories,
		s.catID = REPLACE(p.catCODE, ', ', '|')
	FROM SLIFeed s JOIN products p ON s.[Product Number] = p.code
				   JOIN categories c ON c.code = replace(p.catCode, 'related-items, ', '')
	WHERE p.primaryCatCode = 'related-items' and p.catCode like '%clearance%'
	
	
	--MARKING ACCESSORIES WITH THE CORRECT CATNAME
	UPDATE s
	SET s.[Accessories] = c.[catName],
		s.[Accessory] = 1
	FROM SLIFeed s JOIN products p ON s.[Product Number] = p.code
				   JOIN categories c ON c.id = p.primaryCatID
	WHERE c.catname LIKE '%Accessories%' and s.accessories is null

	UPDATE s
	SET s.[Accessories] = [Refinable No_],
		s.[Accessory] = 1
	FROM SLIFeed s JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item Refinables] r
				ON s.[Product Number] = r.[Item No_] COLLATE Latin1_General_CS_AS
	WHERE LOWER([Refinable No_]) = 'category' AND LOWER([Refinable Value]) LIKE '%accessories%' and s.accessories is null

	--MARK A PRODUCT IF IT IS ECO FRIENDLY
	UPDATE s
	SET s.[GoGreen] = 'Go Green! Products'	
	FROM SLIFeed s JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item Refinables] r
				ON s.[Product Number] = r.[Item No_] COLLATE Latin1_General_CS_AS
	WHERE [Refinable No_] = 'ADDINFO' AND s.[GoGreen] IS NULL AND
		([Refinable Value] LIKE '%Energy Efficient Product%' OR [Refinable Value] LIKE '%Eco Friendly%')
		
	
	--UPDATING WITH CORRECT MFGNAME
	UPDATE s
	SET s.[Manufacturer] = m.mfgShortName 
	FROM SLIFeed s join mfg m on LEFT(s.[Product Number], 3) = m.mfgID
	WHERE m.Active = 1 and m.mfgShortName <> s.[Manufacturer] 
	
	--MARKING ONSALE FOR ITEMS MARKED AS SALES ITEM
	UPDATE SLIFeed SET OnSale = 1 WHERE [Product Number] in (SELECT code From onSale WHERE DATEDIFF(DAY, endDT, GETDATE()) < 1 AND DATEDIFF(DAY, startDT, GETDATE()) > -1) and onSale = 0
		
	--CORRECTING THE MPN INFO	
	UPDATE s
	SET s.mpn = p.mpn
	FROM SLIFeed s join products p on s.[Product Number] = p.CODE
	
	--CORRECTING THE MFG INFO
	UPDATE s
	SET s.Manufacturer = p.mfgName 
	FROM SLIFeed s join products p on s.[Product Number] = p.CODE
		
	DELETE FROM sliFeed WHERE [Product Number] like '599-%'
	
	--TESTING SOLUTION FOR REFINABLE CATEGORY VALUE
	UPDATE slifeed set Category = null

	UPDATE s
	SET s.Category = c.CATNAME
	FROM catxProd cx join categories c on cx.catCode = c.CODE
					 join SLIFeed s on s.[Product Number] = cx.prodcode				  
	WHERE cx.primaryCat = 1 and PARENT_ID = 0 and s.Category is null

	UPDATE s
	SET s.Category = c.CATNAME
	FROM catxProd cx join categories c on cx.catCode = c.CODE
					 join SLIFeed s on s.[Product Number] = cx.prodcode	
	WHERE cx.primaryCat = 1 and PARENT_ID = superCatid and s.Category is null AND c.primaryCat = 1

	UPDATE s
	SET s.Category =(SELECT CATNAME FROM categories WHERE id = c.PARENT_ID and primaryCat = 1)
	FROM catxProd cx join categories c on cx.catCode = c.CODE
					 join SLIFeed s on s.[Product Number] = cx.prodcode	
	WHERE cx.primaryCat = 1 
		AND c.primaryCat = 1
		and c.superCatid NOT IN (5844, 5612) and s.Category is null
		
	UPDATE s
	SET s.Category = 
			   CASE 
				WHEN CHARINDEX('RESIDENTIAL', CATNAME) = 0 THEN 'Residential ' + CATNAME
				ELSE CATNAME
			   END 
	FROM catxProd cx JOIN categories c ON cx.catCode = c.CODE
							  JOIN SLIFeed s on s.[Product Number] = cx.prodCode
	where superCatID = 5612 and cx.primaryCat = 1 and c.primaryCat = 1 and s.Category is null
	
	UPDATE jobHistory SET endDT = GETDATE() WHERE jobID = @jobID
END
GO
