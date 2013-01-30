USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[categories_numProd]    Script Date: 01/30/2013 16:26:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[categories_numProd]
AS
BEGIN
	DECLARE @counter TINYINT
	
	SET @counter = 1

	UPDATE categories set numProd = 0, numProdUpdated = 0, sales30 = 0 

	TRUNCATE TABLE categoryWorktable

	INSERT INTO categoryWorktable
	SELECT COUNT(*), c.ID, c.CODE, c.ACTIVE, NULL, SUM(p.price*ISNULL(p.sales30, 0))
	FROM catxprod m JOIN categories c ON m.catcode = c.code
						join products p on p.code = m.prodCode
	WHERE c.ACTIVE = 1 and p.ACTIVE = 1 and c.primaryCat = 1
	group by c.ID, c.CODE, c.ACTIVE

	UPDATE c
	SET c.numProd = cw.tCounter, numProdUpdated = 1, c.sales30 = cw.tSales
	FROM categories c join categoryWorktable cw on c.CODE = cw.tCode

	WHILE EXISTS (
					SELECT PARENT_ID, superCat
					FROM categories
					WHERE ACTIVE = 1 AND numprod > 0 and numProdUpdated = @counter and PARENT_ID > 0
					GROUP BY PARENT_ID, superCat
				 )
	   BEGIN
		TRUNCATE TABLE categoryWorktable

		INSERT INTO categoryWorktable(tCounter, tCode, tCatName, tSales)
		SELECT SUM(numprod), PARENT_ID, superCat, SUM(sales30)
		FROM categories
		WHERE ACTIVE = 1 and numProd > 0 and numProdUpdated = @counter and PARENT_ID > 0
		GROUP BY PARENT_ID, superCat
		order by PARENT_ID	
		
		SET @counter = @counter + 1
			
		UPDATE c
		SET c.numProd = c.numProd + cw.tCounter, numProdUpdated = @counter, c.sales30 = cw.tSales
		FROM categories c join categoryWorktable cw on c.id = cw.tCode --and cw.tCatName = c.superCat	
	   END
END
GO
