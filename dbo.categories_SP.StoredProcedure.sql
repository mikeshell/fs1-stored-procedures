USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[categories_SP]    Script Date: 02/01/2013 11:09:50 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[categories_SP]
AS
BEGIN
	DECLARE @jobID INT
	INSERT INTO jobHistory(jobName, startdT) values('CAT_UPD', GETDATE())
	SET @jobID = @@identity
	--RESET ALL COLUMNS
	UPDATE categories SET breadcrumb = null, supercat = null, endleaf = 0, sliBreadcrumb = null, URL = NULL, vendor = 0
	
	--CLEANING UP DUPLICATE CAT INSTANCES
	EXEC dbo.categoryDuplicateCleanup_SP
	
	--DETERMINING BREADCRUMB, SUPERCAT, ENDLEAF, VENDOR	
	DECLARE @id INT, @endLeaf BIT

	SET @endLeaf = 0

	--IDENTIFYING ENDLEAF
	DECLARE catCursor CURSOR FOR 
	SELECT m.ID
	FROM categories m
	WHERE m.ACTIVE = 1 and m.id <> 99999
	ORDER BY id asc

	OPEN catCursor
	FETCH NEXT FROM catCursor INTO @ID

	WHILE @@FETCH_STATUS = 0
	   BEGIN
		IF EXISTS(SELECT id FROM categories WHERE PARENT_ID = @ID and ACTIVE = 1)
		   BEGIN
			SET @endLeaf = 0
		   END
		ELSE
		   BEGIN
			SET @endLeaf = 1
		   END
		   
		UPDATE categories
		SET endleaf = @endLeaf
		WHERE ID = @id
		
		FETCH NEXT FROM catCursor INTO @ID
	   END

	CLOSE catCursor
	DEALLOCATE catCursor

	--DETERMINING SUPERCAT, VENDOR
	EXEC dbo.[categories_SuperCat]
		   
	UPDATE categories SET vendor = 1 WHERE superCatID = 5844
	
	--SETTING URL LINK   
	UPDATE categories
	SET url =
		CASE
			WHEN endleaf = 1 THEN 'http://restaurant-supplies.katom.com/cat/' + CODE + '.html'
			WHEN ID = 5844 THEN 'http://www.katom.com/brands.A-C.1.html'
			WHEN PARENT_ID = 5844 THEN 'http://www.katom.com/vendor/' + CODE + '.html'
			ELSE 'http://www.katom.com/cat/' + CODE + '.html'
		END
	WHERE ACTIVE = 1
	
	DECLARE @code VARCHAR(255), @url VARCHAR(1000)

	DECLARE catCursor CURSOR FOR
	SELECT CODE
	FROM categories 
	WHERE ACTIVE = 1
	GROUP BY CODE
	HAVING COUNT(*) > 1

	OPEN catCursor
	FETCH NEXT FROM catCursor INTO @code

	WHILE @@FETCH_STATUS = 0
	   BEGIN
		SET @url = NULL
		SELECT @url = URL 
		FROM categories 
		WHERE ACTIVE = 1 AND primaryCat = 1 AND CODE = @code
		
		UPDATE  categories
		SET url = @url
		WHERE CODE = @code

		FETCH NEXT FROM catCursor INTO @code 
	   END
	   
	CLOSE catCursor
	DEALLOCATE catCursor

	--DETERMINING BREADCRUMB
	DECLARE @parentid INT, @catname VARCHAR(2000), @vendor BIT,
		@result VARCHAR(MAX), @sliResult VARCHAR(MAX), @tParentID INT, @tcatname VARCHAR(2000), 
		@tcode VARCHAR(1000), @tActive BIT, @turl VARCHAR(2000), @counter INT

	SET @counter = 0
	SET @tParentID = 0
		
	TRUNCATE TABLE categoryWorktable 

	DECLARE catCursor CURSOR FOR 
	SELECT ID, PARENT_ID, CODE, CATNAME, url
	FROM categories
	WHERE ACTIVE = 1 and id <> 99999 and primarycat = 1
	ORDER BY id asc

	OPEN catCursor
	FETCH NEXT FROM catCursor INTO @ID, @parentid, @code, @catName, @url

	WHILE @@FETCH_STATUS = 0
	   BEGIN
		SET @result =    ''
		SET @sliResult = '<div class="breadcrumb"><a href="http://www.katom.com" title="KaTom Restaurant Supply">KaTom Restaurant Supply</a>'
		
		IF @parentID > 0 
		   BEGIN
			INSERT INTO categoryWorktable VALUES(@counter, @catName, @code, 1, @url, null)
			SET @tParentID = @parentID		

			WHILE @tParentID > 0 
			   BEGIN
				IF EXISTS(SELECT parent_ID FROM categories WHERE ID = @tParentID AND ACTIVE = 1)
				   BEGIN
					SET @counter = @counter + 1
					
					SELECT @tParentID = parent_ID, @tCatName = catName, @tCode = code, @tActive = ACTIVE, @turl = url 
					FROM categories WHERE ID = @tParentID AND ACTIVE = 1 AND primaryCat = 1					
					
					INSERT INTO categoryWorktable VALUES(@counter, @tCatName, @tCode, @tActive, @turl, null)
				   END
				ELSE
				   BEGIN
					SET @tParentID = 0
				   END				
			   END
		
			IF @tParentID = 0 
			   BEGIN			
				DECLARE bread_cursor CURSOR FOR
				SELECT tCatName, tCode, tURL FROM categoryWorktable WHERE tActive = 1 ORDER BY tCounter desc

				OPEN bread_cursor
				FETCH NEXT FROM bread_cursor INTO @tCatName, @tCode, @tURL

				WHILE @@FETCH_STATUS = 0
				   BEGIN									
					IF @tCode <> @code
					   BEGIN
						SET @result = @result + 
							CASE 
								WHEN LEN(@result) > 0 THEN ' > ' 
								ELSE ''
							END + @tCatName
						SET @sliResult = @sliResult + 
								CASE 
									WHEN @tCatName = 'Shop By Vendor' THEN '&nbsp;>&nbsp;<a href="http://www.katom.com/brands.A-C.1.html" title="Shop By Vendor">'
									ELSE '&nbsp;>&nbsp;<a href="' + @turl + '" title="' + @tCatName + '">'  
								END + @tCatName + '</a>'					
					   END
					ELSE
					   BEGIN
						SET @result = @result + 
							CASE 
								WHEN LEN(@result) > 0 THEN ' > ' 
								ELSE ''
							END + @tCatName
						SET @sliResult = @sliResult + '&nbsp;>&nbsp;<strong>' + @tCatName + '</strong></div>'
					   END
					FETCH NEXT FROM bread_cursor INTO @tCatName, @tCode, @tURL
				   END

				CLOSE bread_cursor
				DEALLOCATE bread_cursor
	        
				SET @counter = 0
				TRUNCATE TABLE categoryWorktable
			   END
		   END
		ELSE
		   BEGIN
			SET @result = @result + 
				CASE 
					WHEN LEN(@result) > 0 THEN ' > ' 
					ELSE ''
				END + @tCatName
			SET @sliResult = @sliResult + '&nbsp;>&nbsp;<strong>' + @catName + '</strong></div>'
			SET @url = 'http://www.katom.com/cat/'  + @tCode + '.html'
		   END
		
		UPDATE categories
		SET breadcrumb = @result, 
			sliBreadcrumb = @sliResult, 
			updateinfo = 0
		WHERE ID = @ID  	

		FETCH NEXT FROM catCursor INTO @ID, @parentid, @code, @catName, @url  
	   END
	   
	CLOSE catCursor
	DEALLOCATE catCursor	   
	
	--DETERMINING SISTER categories
	DECLARE @sisterCat VARCHAR(MAX), @sisterCatID VARCHAR(MAX)
	
	UPDATE categories SET sisterCat = null, sisterCatID = null

	DECLARE pCursor CURSOR FOR 
	SELECT parent_ID
	FROM categories
	WHERE ACTIVE = 1 AND PARENT_ID > 0
	GROUP BY PARENT_ID
	HAVING COUNT(*) > 1
	
	OPEN pCursor
	FETCH NEXT FROM pCursor INTO @parentID

	WHILE @@FETCH_STATUS = 0
	   BEGIN
		
		SET @sisterCat = ''
		SET @sisterCatID = ''
			   
		DECLARE dCursor CURSOR FOR 
		SELECT code, catName
		FROM categories
		WHERE active = 1 AND parent_ID = @parentID AND primaryCat = 1
		ORDER BY ID
		
		OPEN dCursor
		FETCH NEXT FROM dCursor INTO @code, @catName

		WHILE @@FETCH_STATUS = 0
		   BEGIN
		   
			IF LEN(@sisterCat) > 0 
			   BEGIN
				SET @sisterCat = @sisterCat + '|' + @catName
				SET @sisterCatID = @sisterCatID + '|' + @code
			   END
			ELSE 
			   BEGIN
				SET @sisterCat = @catName
				SET @sisterCatID = @code
			   END
			
			FETCH NEXT FROM dCursor INTO @code, @catName
		   END
		   
		UPDATE categories
		SET sisterCat = @sisterCat,
			sisterCatID = @sisterCatID
		WHERE PARENT_ID = @parentID
		
		CLOSE dCursor
		DEALLOCATE dCursor
		   
		FETCH NEXT FROM pCursor INTO @parentID
	   END

	CLOSE pCursor
	DEALLOCATE pCursor		
	
	--CLEAN UP BREADCRUMB URL FOR VENDORS
	DECLARE @replacingURL varchar(1000)

	SET @URL = ''
	SET @replacingURL = ''

	DECLARE catCursor CURSOR FOR 
	SELECT url 
	FROM categories 
	WHERE PARENT_ID = 5844 and active = 1 and CHARINDEX('restaurant-supplies.katom.com', url) = 0

	OPEN catCursor
	FETCH NEXT FROM catCursor INTO @url

	WHILE @@FETCH_STATUS = 0
	   BEGIN
	   
		SET @replacingURL = REPLACE(@url, '/vendor/', '/cat/')
		
		UPDATE categories
		SET breadcrumb = REPLACE(breadcrumb, REPLACE(@replacingURL, 'http://www.katom.com/', ''), REPLACE(@url, 'http://www.katom.com/', '')),
				slibreadcrumb = REPLACE(slibreadcrumb, @replacingURL, @url)
		WHERE PARENT_ID <> 5844 and active = 1 and vendor = 1
		
		FETCH NEXT FROM catCursor INTO @url
	   END

	CLOSE catCursor
	DEALLOCATE catCursor
	
	UPDATE categories
	SET breadcrumb = REPLACE(breadcrumb, '&nbsp;>&nbsp;<a href="cat/HDN.html" title="special">Special</a>', ''),
		slibreadcrumb = REPLACE(slibreadcrumb, '&nbsp;>&nbsp;<a href="cat/HDN.html" title="special">Special</a>', '')
		
	EXEC [dbo].[category_catxprod]
	
	--INSERT PRODUCT TO SPECIAL HOLIDAY CATEGORIES...JUST FOR THE HOLIDAY.  TURN OFF ON 12/31/2011
	EXEC [dbo].[category_holiday]	
	
	--CATEGORIZING PRODUCTS UNDER THEIR ASSOCIATED VENDOR CATEGORY IF THEY ARE NO CHILDREN 
	--CATEGORIES ASSIGNED TO THE MAIN PARENT
	--EXEC [dbo].[category_categorized_uncat_vendor_products]
	
	--ASSIGNING FEATURED PRODUCTS
	EXEC dbo.[category_featuredProducts]
	
	UPDATE categories SET url = REPLACE(url, '/cat/', '/holiday/') WHERE PARENT_ID = 10836
	
	UPDATE categories 
	SET breadcrumb = REPLACE(breadcrumb, '&nbsp;>&nbsp;<a href="holiday/holiday.html" title="Holiday">Holiday</a>', '') 
	WHERE superCatID = 10836 
	
	UPDATE categories 
	SET slibreadcrumb = REPLACE(slibreadcrumb, '&nbsp;>&nbsp;<a href="http://www.katom.com/cat/holiday.html" title="Holiday">Holiday</a>', '') 
		WHERE superCatID = 10836
				
	--CALCULATING SALES FOR 30 AND 60 DAYS
	DECLARE @sales30 DECIMAL(10, 2), @sales3060 DECIMAL(10, 2)

	TRUNCATE TABLE prodTemp

	UPDATE categories SET sales30 = 0, sales3060 = 0

	INSERT INTO prodTemp(Keywords, [UnitPrice])
	SELECT p.primaryCatCode,
		CAST(SUM(ISNULL(il.[Quantity], 0)*ISNULL(il.[Unit Price], 0)) as decimal(10, 2))	
	FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] ih
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] il
			ON ih.[No_]  = il.[Document No_]
		JOIN products p on p.CODE = il.[No_] COLLATE Latin1_General_CS_AS
	WHERE DATEDIFF(DAY, ih.[Posting Date], GETDATE()) < 31
		AND ih.[Source Code] = 'SALES'
		AND ih.[Customer Source] = 'I'
		AND il.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
	GROUP BY p.primaryCatCode
		
	UPDATE c
	SET c.sales30 = t.[UnitPrice]
	FROM categories c JOIN prodTemp t on c.CODE = t.Keywords

	INSERT INTO prodTemp(Keywords, [UnitPrice])
	SELECT p.primaryCatCode,
		CAST(SUM(ISNULL(il.[Quantity], 0)*ISNULL(il.[Unit Price], 0)) as decimal(10, 2))	
	FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] ih
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] il
			ON ih.[No_]  = il.[Document No_]
		JOIN products p on p.CODE = il.[No_] COLLATE Latin1_General_CS_AS
	WHERE DATEDIFF(DAY, ih.[Posting Date], GETDATE()) between 31 and 60
		AND ih.[Source Code] = 'SALES'
		AND ih.[Customer Source] = 'I'
		AND il.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
	GROUP BY p.primaryCatCode
		
	UPDATE c
	SET c.sales3060 = t.[UnitPrice]
	FROM categories c JOIN prodTemp t on c.CODE = t.Keywords

	DECLARE catCursor CURSOR FOR 
	SELECT ID FROM categories WHERE ACTIVE = 1 AND sales30 = 0 AND endleaf = 0 order by parent_id desc, id desc

	OPEN catCursor
	FETCH NEXT FROM catCursor INTO @id

	WHILE @@FETCH_STATUS = 0
	   BEGIN
		SELECT @sales30 = sum(isnull(sales30, 0)), 
			   @sales3060 = sum(isnull(sales3060, 0))
		FROM categories 
		WHERE PARENT_ID = @id
		
		UPDATE categories SET sales30 = @sales30, sales3060 = @sales3060 WHERE ID = @id

		FETCH NEXT FROM catCursor INTO @id
	   END

	CLOSE catCursor
	DEALLOCATE catCursor
		
	--ASSIGNING MFGID FOR ALL CAT WHERE PARENT_ID = 5844 (SHOP BY VENDOR)
	UPDATE c
	SET c.mfgid = m.mfgid
	FROM categories c JOIN mfg m ON c.CATNAME = m.mfgName
	WHERE c.mfgID IS NULL			
	
	--Bandaid - added 10/11/12 by MS
	UPDATE categories
	SET vendor = 0
	WHERE vendor = 1 AND parent_id <> 5844
	
	UPDATE jobHistory SET endDT = GETDATE() WHERE jobID = @jobID

END
GO
