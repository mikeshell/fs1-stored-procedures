USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[categories2_SP]    Script Date: 01/30/2013 16:26:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[categories2_SP]
AS
BEGIN
	DECLARE @jobID INT
	INSERT INTO jobHistory(jobName, startdT) values('CAT_UPD', GETDATE())
	SET @jobID = @@identity
	
	--DELETE ALL CATEGORIES WHERE IT DOESNT EXIST IN MIVA CAT TABLE
	--DELETE FROM categories WHERE CODE NOT IN (SELECT CODE FROM mivaCategories)

	--INACTIVATE ALL INACTIVE categories
	--UPDATE categories
	--SET active = 0
	--WHERE code NOT IN (SELECT code FROM mivacategories WHERE active = 1)
	--	AND active = 1
		
	--UPDATE c
	--SET c.active = m.active, c.updateinfo = 1
	--FROM categories c JOIN mivacategories m ON c.CODE = m.code
	--WHERE c.active <> m.active
	--	AND c.primaryCat = 1

	--INSERTING NEW categories
	--INSERT INTO categories(id, parent_id, code, catname, active, catOrder, updateinfo, primaryCat)
	--SELECT id, parent_id, code, [name], 1, disp_order, 1, 1
	--FROM mivacategories
	--WHERE code NOT IN (SELECT code FROM categories)
	--	AND active = 1

	--UPDATING CATID
	--UPDATE c
	--SET c.id = cm.id, c.updateinfo = 1
	--FROM categories c JOIN mivacategories cm ON c.code = cm.code
	--WHERE cm.active = 1 and c.id <> cm.id
	--	AND c.primaryCat = 1

	--UPDATING CATNAME
	--UPDATE c
	--SET c.catName = cm.Name, c.updateinfo = 1
	--FROM categories c JOIN mivacategories cm ON c.code = cm.code
	--WHERE cm.active = 1 and c.catName <> cm.Name
	--	AND c.primaryCat = 1

	--UPDATING DISPLAY ORDER
	--UPDATE c
	--SET c.catOrder = cm.disp_order, c.updateinfo = 1
	--FROM categories c JOIN mivacategories cm ON c.code = cm.code
	--WHERE cm.active = 1 and isnull(c.catOrder, -1) <> cm.disp_order
	--	AND c.primaryCat = 1

	--UPDATING PARENT ID
	--UPDATE c
	--SET c.PARENT_ID = cm.parent_id, c.updateinfo = 1
	--FROM categories c JOIN mivacategories cm ON c.code = cm.code
	--WHERE c.PARENT_ID <> cm.parent_id AND c.primaryCat = 1
	
	--RESET ALL COLUMNS
	UPDATE categories SET breadcrumb = null, supercat = null, endleaf = 0, sliBreadcrumb = null, URL = NULL, vendor = 0
			
	--CREATING VIRTUAL CAT INSTANCES...TO REVISE ONCE MIVA GOES AWAY
	--EXEC dbo.categoryVirtual_SP
	
	--CLEANING UP DUPLICATE CAT INSTANCES
	EXEC dbo.categoryDuplicateCleanup_SP
	
	--DELETE ALL CATEGORIES WHERE ID & PARENT_ID IS NOT IN MIVA TABLE
	--DELETE categories WHERE ID NOT IN (SELECT ID FROM mivaCategories)
	--DELETE categories WHERE PARENT_ID NOT IN (SELECT ID FROM categories) AND PARENT_ID > 0
	
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
		IF EXISTS(SELECT * FROM categories WHERE PARENT_ID = @ID and ACTIVE = 1)
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
			WHEN ID = 5844 THEN 'http://www.katom.com/brands.A-C.1.html'
			WHEN PARENT_ID = 8788 THEN 'http://www.katom.com/buyer/' + CODE + '.html'
			WHEN endleaf = 1 THEN 'http://restaurant-supplies.katom.com/cat/' + CODE + '.html'
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
		SET @result = '<div class="breadcrumb"><a href="http://www.katom.com">Katom</a>'
		SET @sliResult = '<div class="breadcrumb"><a href="http://www.katom.com">Katom</a>'
		
		IF @parentID > 0 
		   BEGIN
			INSERT INTO categoryWorktable VALUES(@counter, @catName, @code, 1, @url)
			SET @tParentID = @parentID		

			WHILE @tParentID > 0 
			   BEGIN
				IF EXISTS(SELECT parent_ID FROM categories WHERE ID = @tParentID AND ACTIVE = 1)
				   BEGIN
					SET @counter = @counter + 1
					
					SELECT @tParentID = parent_ID, @tCatName = catName, @tCode = code, @tActive = ACTIVE, @turl = url 
					FROM categories WHERE ID = @tParentID AND ACTIVE = 1 AND primaryCat = 1					
					
					INSERT INTO categoryWorktable VALUES(@counter, @tCatName, @tCode, @tActive, @turl)
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
									WHEN @tCatName = 'Shop By Vendor' THEN '&nbsp;>&nbsp;<a href="brands.A-C.1.html">'
									ELSE '&nbsp;>&nbsp;<a href="' + REPLACE(@turl, 'http://www.katom.com/', '') + '">' 
								END + @tCatName + '</a>'
						SET @sliResult = @sliResult + 
								CASE 
									WHEN @tCatName = 'Shop By Vendor' THEN '&nbsp;>&nbsp;<a href="http://www.katom.com/brands.A-C.1.html">'
									ELSE '&nbsp;>&nbsp;<a href="' + @turl + '">' 
								END + @tCatName + '</a>'					
					   END
					ELSE
					   BEGIN
						SET @result = @result + '&nbsp;>&nbsp;<h2>' + @tCatName + '</h2></div>'
						SET @sliResult = @sliResult + '&nbsp;>&nbsp;<h2>' + @tCatName + '</h2></div>'
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
			SET @result = @result + '&nbsp;>&nbsp;<h2>' + @catName + '</h2></div>'
			SET @sliResult = @sliResult + '&nbsp;>&nbsp;<h2>' + @catName + '</h2></div>'
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
	
	--CLEAN UP BREADCRUMB URL FOR SPECIAL CAT
	UPDATE categories
	SET url = 'http://www.katom.com/site/' + CODE + '.html'
	WHERE CODE in ('restaurant-equipment-leasing','demos-restaurant-buying-guide','patricia-chili-recipe',
					'WHICHFRYER','lodge-hotsellers','kitchenAid-hotsellers','anchor-hocking-hotsellers',
					'waring-hotsellers','star-hotsellers','john-boos-hotsellers','staub-recipes-dinner-idea')

	UPDATE categories
	SET url = 'http://www.katom.com/help/CATALOG.html'
	WHERE CODE = 'x-catalogrequest'

	UPDATE categories
	SET breadcrumb = REPLACE(breadcrumb, '&nbsp;>&nbsp;<a href="cat/HDN.html">Special</a>', ''),
		slibreadcrumb = REPLACE(slibreadcrumb, '&nbsp;>&nbsp;<a href="cat/HDN.html">Special</a>', '')
		
	EXEC [dbo].[category_catxprod]
	
	--COUNT THE NUMBER OF PRODUCTS PER CATEGORY
	EXEC dbo.[categories_numProd]
		
	UPDATE jobHistory SET endDT = GETDATE() WHERE jobID = @jobID

END
GO
