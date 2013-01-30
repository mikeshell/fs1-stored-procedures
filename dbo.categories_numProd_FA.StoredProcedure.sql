USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[categories_numProd_FA]    Script Date: 01/30/2013 16:26:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[categories_numProd_FA]
AS
BEGIN
	DECLARE @counter TINYINT

	SET @counter = 1

	UPDATE CATEGORIES_FA set numProd = 0, numProdUpdated = 0

	TRUNCATE TABLE categoryWorktable

	--COUNT NUMBER OF PRODUCTS FOR ENDLEAF
	INSERT INTO categoryWorktable(tCatname, tCounter)
	SELECT c.id, COUNT(cx.prodid)
	FROM CATEGORIES_FA c LEFT JOIN catxProd_FA cx ON c.id = cx.catid
	WHERE endleaf = 1
	GROUP BY c.ID

	UPDATE c
	SET c.numProd = ct.tCounter, numProdUpdated = 1
	FROM CATEGORIES_FA c JOIN categoryWorktable ct ON c.id = ct.tCatname


	--THE PRODCOUNT IS NOT ACCURATE AS YOU MOVE UP THE TREE.  
	--IT'S DOUBLE COUNTING ENDLEAF PRODCOUNT IF A COMBINATION OF ENDLEAF AND NONE ENDLEAF CAT BELONG TO A PARENT CAT...
	WHILE EXISTS (
					SELECT PARENT_ID, superCat
					FROM CATEGORIES_FA
					WHERE ACTIVE = 1 AND numprod > 0 and numProdUpdated = @counter
					GROUP BY PARENT_ID, superCat
				 )
	   BEGIN
		TRUNCATE TABLE categoryWorktable

		INSERT INTO categoryWorktable(tCounter, tCode, tCatName, tSales)
		SELECT SUM(numprod), PARENT_ID, superCat, COUNT(id)
		FROM CATEGORIES_FA
		WHERE ACTIVE = 1 and numProd > 0 and numProdUpdated = @counter and PARENT_ID > 0
		GROUP BY PARENT_ID, superCat
		order by PARENT_ID	
		
		SET @counter = @counter + 1
			
		UPDATE c
		SET c.numProd = c.numProd + cw.tCounter, numProdUpdated = @counter
		FROM CATEGORIES_FA c join categoryWorktable cw on c.id = cw.tCode
	   END	   
	
	--DELETING ANY CATEGORIES WITH NO PRODUCTS 
	DELETE FROM CATEGORIES_FA WHERE numProd = 0
END
GO
