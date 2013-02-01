USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[categoryDuplicateCleanup_SP]    Script Date: 02/01/2013 11:09:50 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[categoryDuplicateCleanup_SP]
AS
BEGIN
	DECLARE @id INT, @parent_ID INT, @code VARCHAR(500), @catCode VARCHAR(500), @uniqueID INT

	--REMOVING DUPLICATE ENTRY IN CATEGORIES TABLE
	DECLARE cCursor CURSOR FOR 
	SELECT ID, PARENT_ID
	FROM categories
	GROUP BY ID, PARENT_ID
	HAVING COUNT(*) > 1

	OPEN cCursor
	FETCH NEXT FROM cCursor INTO @id, @parent_ID

	WHILE @@FETCH_STATUS = 0
	   BEGIN
		SET @uniqueID = ''
		
		SELECT @uniqueID = unique_ID
		FROM categories
		WHERE ID = @id AND PARENT_ID = @parent_ID 
		ORDER BY active DESC
		
		DELETE FROM categories
		WHERE ID = @id AND PARENT_ID = @parent_ID AND unique_ID <> @uniqueID

		FETCH NEXT FROM cCursor INTO @id, @parent_ID
	   END

	CLOSE cCursor
	DEALLOCATE cCursor

	--REMOVING DUPLICATE ENTRY IN CATXPROD TABLE
	DECLARE cCursor CURSOR FOR 
	SELECT catcode, prodCode
	FROM catxprod
	GROUP BY catcode, prodCode
	HAVING COUNT(*) > 1

	OPEN cCursor
	FETCH NEXT FROM cCursor INTO @catCode, @code

	WHILE @@FETCH_STATUS = 0
	   BEGIN
		SET @uniqueID = ''
		
		SELECT @uniqueID = ID
		FROM catxprod
		WHERE catCode = @catCode AND prodCode = @code
		
		DELETE FROM catxprod
		WHERE catCode = @catCode AND prodCode = @code AND ID <> @uniqueID

		FETCH NEXT FROM cCursor INTO @catCode, @code
	   END

	CLOSE cCursor
	DEALLOCATE cCursor
END
GO
