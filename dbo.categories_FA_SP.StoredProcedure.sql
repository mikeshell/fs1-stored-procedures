USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[categories_FA_SP]    Script Date: 01/30/2013 16:26:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[categories_FA_SP]
AS
BEGIN
	DECLARE @numProdPerCat INT

	--SET THE NUMBER OF PRODUCTS PER CATEGORY YOU WANT TO HAVE.  ALWAYS ADD 1 TO WHAT YOU WANT.
	--FOR EXAMPLE: IF YOU WANT TO HAVE 36 PRODUCTS PER CATEGORY THEN SET IT TO 36+1 = 37
	SET @numProdPerCat = 37

	TRUNCATE TABLE catxProd_FA
	TRUNCATE TABLE CATEGORIES_FA

	--ONLY COPY FA PRODUCTS CATEGORIZATION
	INSERT INTO catxProd_FA 
	SELECT DISTINCT cx.id, cx.catID, cx.catCode, cx.prodID, cx.prodCode, cx.primaryCat 
	FROM catxProd cx JOIN products p ON cx.prodcode = p.code 
	WHERE p.active = 1 AND isweb = 1 AND fa = 1

	--ONLY COPY MAIN AND ACTIVE CATEGORIES
	INSERT INTO CATEGORIES_FA ([ID], [PARENT_ID], [CODE], [CATNAME], [superCat], [endleaf], [catDescription], 
					[metaDescription], [sisterCat], [sisterCatID], [numProd], [ACTIVE], [primaryCat], 
					[numProdUpdated], [superCatID], [mfgID])
	SELECT id, parent_ID, code, catname, superCat, endleaf, catDescription, metaDescription, sisterCat, sisterCatID,
			numProd, active, primaryCat, numProdUpdated, superCatID, mfgID
	FROM CATEGORIES 
	WHERE supercatid NOT IN (5612, 5837, 8406, 10135, 10751, 10836) 
		AND active = 1
		AND primaryCat = 1

	--DELETING ALL ENDLEAF WHERE THERE IS NO PRODUCT
	DELETE FROM CATEGORIES_FA
	WHERE id IN (
				SELECT c.id
				FROM CATEGORIES_FA c LEFT JOIN catxProd_FA cx ON c.id = cx.catid
				WHERE endleaf = 1
				GROUP BY c.id
				HAVING COUNT(cx.prodCode) = 0
				)
		
	--RECOUNT NUMBER OF PRODUCTS PER CATEGORY FOR FA
	EXEC dbo.[categories_numProd_FA]

	--LOOP THROUGH CATEGORIES TO REASSIGNED PRODUCTS UP THE TREE IF THE NUMBER OF PRODUCTS PER PARENT CAT IS UNDER 37
	WHILE EXISTS(SELECT parent_id
				FROM CATEGORIES_FA c
				WHERE endleaf = 1 AND PARENT_ID NOT IN (0, 5844)
					AND (SELECT COUNT(*) FROM CATEGORIES_FA WHERE PARENT_ID = c.PARENT_ID AND endleaf = 0 ) < 1
				GROUP BY parent_id
				HAVING SUM(numProd) < @numProdPerCat)
	   BEGIN
		EXEC dbo.[categories_reAssignProdCat_FA] @numProdPerCat
		EXEC dbo.[categories_numProd_FA]
	   END
	   
	--CLEAN UP PROCESS
	DELETE cx 
	FROM catxprod_FA cx LEFT JOIN categories_FA c ON cx.catID = c.ID
	WHERE c.ID IS NULL
	EXEC dbo.[categories_numProd_FA]

	--SET URL
	UPDATE CATEGORIES_FA
	SET url = 'www.fabeqsupply.com/cat/' + LOWER(REPLACE(REPLACE(supercat, '/ ', ''), ' ', '-')) + '/' + code + '.html'

	--DETERMINING BREADCRUMB
	DECLARE @parentid INT, @catname VARCHAR(2000), @vendor BIT, @id INT, @code VARCHAR(255),
		@result VARCHAR(MAX), @tParentID INT, @tcatname VARCHAR(2000), 
		@tcode VARCHAR(1000), @tActive BIT, @turl VARCHAR(2000), @counter INT,
		@url VARCHAR(500)

	SET @counter = 0
	SET @tParentID = 0
		
	TRUNCATE TABLE categoryWorktable 

	DECLARE catCursor CURSOR FOR 
	SELECT ID, PARENT_ID, CODE, CATNAME, url
	FROM CATEGORIES_FA
	WHERE ACTIVE = 1 and primarycat = 1
	ORDER BY id asc

	OPEN catCursor
	FETCH NEXT FROM catCursor INTO @ID, @parentid, @code, @catName, @url

	WHILE @@FETCH_STATUS = 0
	   BEGIN
		SET @result = '<div class="breadcrumb"><a href="http://www.fabeqsupply.com" title="FAB EQ Supply">FAB EQ Supply</a>'
		
		IF @parentID > 0 
		   BEGIN
			INSERT INTO categoryWorktable VALUES(@counter, @catName, @code, 1, @url, null)
			SET @tParentID = @parentID		

			WHILE @tParentID > 0 
			   BEGIN
				IF EXISTS(SELECT parent_ID FROM categories_FA WHERE ID = @tParentID AND ACTIVE = 1)
				   BEGIN
					SET @counter = @counter + 1
					
					SELECT @tParentID = parent_ID, @tCatName = catName, @tCode = code, @tActive = ACTIVE, @turl = url 
					FROM categories_FA WHERE ID = @tParentID AND ACTIVE = 1 AND primaryCat = 1					
					
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
						SET @result = @result + '&nbsp;>&nbsp;<a href="' + @turl + '" title="' + @tCatName + '">' + @tCatName + '</a>'					
					   END
					ELSE
					   BEGIN
						SET @result = @result + '&nbsp;>&nbsp;<strong>' + @tCatName + '</strong></div>'
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
			SET @result = @result + '&nbsp;>&nbsp;<strong>' + @catName + '</strong></div>'
			--SET @url = 'http://www.katom.com/cat/'  + @tCode + '.html'
		   END
		
		UPDATE categories_FA
		SET breadcrumb = @result
		WHERE ID = @ID  	

		FETCH NEXT FROM catCursor INTO @ID, @parentid, @code, @catName, @url  
	   END
	   
	CLOSE catCursor
	DEALLOCATE catCursor	   
	
	--DETERMINING SISTER categories
	DECLARE @sisterCat VARCHAR(MAX), @sisterCatID VARCHAR(MAX)
	
	UPDATE categories_FA SET sisterCat = null, sisterCatID = null

	DECLARE pCursor CURSOR FOR 
	SELECT parent_ID
	FROM categories_FA
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
		FROM categories_FA
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
		   
		UPDATE categories_FA
		SET sisterCat = @sisterCat,
			sisterCatID = @sisterCatID
		WHERE PARENT_ID = @parentID
		
		CLOSE dCursor
		DEALLOCATE dCursor
		   
		FETCH NEXT FROM pCursor INTO @parentID
	   END

	CLOSE pCursor
	DEALLOCATE pCursor		
	
END
GO
