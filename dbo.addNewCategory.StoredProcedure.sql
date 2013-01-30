USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[addNewCategory]    Script Date: 01/30/2013 16:26:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[addNewCategory]
AS
BEGIN	
	DECLARE @ID INT, @code varchar(500), @parentCode varchar(500), @catName varchar(500), @parent_id INT, @isPrimary INT, @parentID INT

	SELECT @ID = MAX(id) FROM categories 

	DELETE FROM catTemp WHERE catCode IS NULL
	
	--REMOVE NEW SUBMITTED CAT THAT ALREADY EXIST
	DELETE ct 
	FROM catTemp ct join categories c on ct.catCode = c.CODE
	WHERE c.ACTIVE = 1
	
	UPDATE ct
	SET ct.parent_id = c.id 
	FROM catTemp ct JOIN categories c ON ct.[parentCode] = code AND ACTIVE = 1
	
	DECLARE catCursor CURSOR FOR 
	SELECT ltrim(rtrim(catCode)), ltrim(rtrim(parentCode)), ltrim(rtrim(catName)), primaryCat, parent_ID 
	FROM catTemp ORDER BY parentCode

	OPEN catCursor
	FETCH NEXT FROM catCursor INTO @code, @parentCode, @catName, @isPrimary, @parentID

	WHILE @@FETCH_STATUS = 0
	   BEGIN
		IF NOT EXISTS(SELECT * FROM categories WHERE CODE = @code AND primaryCat = @isPrimary and PARENT_ID = @parentID)
		   BEGIN   
			SET @ID = @ID + 1
			IF @parentCode = NULL 
			   BEGIN 
				  SET @parent_id = 0
			   END
			ELSE
			   BEGIN 
				IF @parentID = NULL
				   BEGIN
					SELECT @parentID = id FROM categories WHERE CODE = @parentCode
				   END
			   END
			
			INSERT INTO categories(ID, PARENT_ID, CODE, CATNAME, endleaf, primaryCat, ACTIVE)
			VALUES(@ID, @parentID, @code, @catName, 0, @isPrimary, 1)				   
		   END
		   
		FETCH NEXT FROM catCursor INTO @code, @parentCode, @catName, @isPrimary, @parentID
	   END

	CLOSE catCursor
	DEALLOCATE catCursor

	--FORCE AN ACTIVE CATEGORY TO WORK IF IT IS ASSIGNED TO THE SPECIAL CAT
	UPDATE c
	SET c.PARENT_ID = 10751 
	FROM catTemp ct JOIN categories c ON ct.[catCode] = c.CODE AND ct.parentCode = 'special'
	WHERE c.PARENT_ID IS NULL

	-- ASSIGN PARENT_ID IN CASE IT WAS NOT ASSIGNED THROUGH THE FIRST PASS
	DELETE FROM catTemp WHERE ISNUMERIC(parent_id) = 1
	
	UPDATE ct
	SET ct.parent_id = c.id 
	FROM catTemp ct JOIN categories c ON ct.[parentCode] = c.CODE AND ACTIVE = 1
	
	UPDATE c
	SET c.parent_id = ct.parent_id 
	FROM categories c JOIN catTemp ct on c.CODE = ct.catCode
	WHERE c.PARENT_ID IS NULL AND c.ACTIVE = 1 AND ct.parent_id IS NOT NULL
	
	TRUNCATE TABLE catTemp
	
	UPDATE categories SET ACTIVE = 0 WHERE ACTIVE = 1 AND parent_id IS NULL
	
	
	--REMOVE DUP CATEGORIES
	DECLARE catCursor CURSOR FOR 
	SELECT CODE
	FROM categories 
	GROUP BY CODE
	HAVING COUNT(*) > 1

	OPEN catCursor
	FETCH NEXT FROM catCursor INTO @code

	WHILE @@FETCH_STATUS = 0
	   BEGIN
		SELECT @id = ID FROM categories WHERE CODE = @code AND primaryCat = 1
		
		UPDATE categories 
		SET ID = @id 
		WHERE CODE = @code AND primaryCat = 0
		
		FETCH NEXT FROM catCursor INTO @code
	   END

	CLOSE catCursor
	DEALLOCATE catCursor	
	
	--ASSIGNING MFGID FOR ALL CAT WHERE PARENT_ID = 5844 (SHOP BY VENDOR)
	UPDATE c
	SET c.mfgid = m.mfgid
	FROM categories c JOIN mfg m ON c.CATNAME = m.mfgName
	WHERE c.mfgID IS NULL
END
GO
