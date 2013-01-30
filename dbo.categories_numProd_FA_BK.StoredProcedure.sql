USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[categories_numProd_FA_BK]    Script Date: 01/30/2013 16:26:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[categories_numProd_FA_BK]
AS
BEGIN
	DECLARE @counter TINYINT
	
	SET @counter = 1

	UPDATE FAcategories set numProd = 0, numProdUpdated = 0, sales30 = 0 

	TRUNCATE TABLE FAcategoryWorktable

	INSERT INTO FAcategoryWorktable
	SELECT COUNT(*), c.ID, c.CODE, c.ACTIVE, NULL, SUM(p.price*ISNULL(p.sales30, 0))
	FROM catxprod m JOIN categories c ON m.catcode = c.code
						join products p on p.code = m.prodCode
	WHERE c.ACTIVE = 1 and p.ACTIVE = 1 and c.primaryCat = 1 and p.fa = 1
	group by c.ID, c.CODE, c.ACTIVE

	UPDATE c
	SET c.numProd = cw.tCounter, numProdUpdated = 1, c.sales30 = cw.tSales
	FROM facategories c join FAcategoryWorktable cw on c.CODE = cw.tCode

	WHILE EXISTS (
					SELECT PARENT_ID, superCat
					FROM FAcategories
					WHERE ACTIVE = 1 AND numprod > 0 and numProdUpdated = @counter and PARENT_ID > 0 
					GROUP BY PARENT_ID, superCat
				 )
	   BEGIN
		TRUNCATE TABLE FAcategoryWorktable

		INSERT INTO FAcategoryWorktable(tCounter, tCode, tCatName, tSales)
		SELECT SUM(numprod), PARENT_ID, superCat, SUM(sales30)
		FROM FAcategories
		WHERE ACTIVE = 1 and numProd > 0 and numProdUpdated = @counter and PARENT_ID > 0
		GROUP BY PARENT_ID, superCat
		order by PARENT_ID	
		
		SET @counter = @counter + 1
			
		UPDATE c
		SET c.numProd = c.numProd + cw.tCounter, numProdUpdated = @counter, c.sales30 = cw.tSales
		FROM FAcategories c join FAcategoryWorktable cw on c.id = cw.tCode --and cw.tCatName = c.superCat	
	   END

		delete openquery (RDN, 'select * from fa_cat_prods')

		insert openquery(RDN, 'select catcode, numprods from fa_cat_prods')
		SELECT f.code, f.numprod FROM FAcategories f

END
GO
