USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[categories_reAssignProdCat_FA]    Script Date: 02/01/2013 11:09:50 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[categories_reAssignProdCat_FA]
	@numProdPerCat INT
AS
BEGIN
	DECLARE @parent_ID INT, @parentCode VARCHAR(100)

	DECLARE catCursor CURSOR FOR 
	SELECT parent_id
	FROM CATEGORIES_FA c
	WHERE endleaf = 1 AND PARENT_ID NOT IN (0, 5844)
		AND (SELECT COUNT(*) FROM CATEGORIES_FA WHERE PARENT_ID = c.PARENT_ID AND endleaf = 0 ) < 1
	GROUP BY parent_id
	HAVING SUM(numProd) < (@numProdPerCat+1)

	OPEN catCursor
	FETCH NEXT FROM catCursor INTO @parent_ID

	WHILE @@FETCH_STATUS = 0
	   BEGIN
		IF NOT EXISTS(SELECT id FROM categories_FA WHERE PARENT_ID = @parent_ID AND endleaf = 0)
		   BEGIN   
			SELECT @parentCode = code FROM categories_FA WHERE ID = @parent_ID
		   
			UPDATE cx
			SET cx.catID = @parent_ID, 
				cx.catCode = @parentCode
			FROM catxprod_FA cx JOIN categories_FA c ON cx.catID = c.ID
			WHERE c.PARENT_ID = @parent_ID
			
			UPDATE categories_FA SET endleaf = 1 WHERE ID = @parent_ID
			DELETE FROM categories_FA WHERE PARENT_ID = @parent_ID	
		   END
			
		FETCH NEXT FROM catCursor INTO @parent_ID
	   END

	CLOSE catCursor
	DEALLOCATE catCursor
END
GO
