USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[prodCatAssignment]    Script Date: 01/30/2013 16:26:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[prodCatAssignment]
AS
BEGIN
	update products 
	set categories = NULL, catCode = NULL

	UPDATE p
	SET p.categories = c.CATNAME,
		p.catCode = c.CODE
	from MIVACatXProd cx join categories c on cx.cat_id = c.ID
						 join products p on p.mivaProdID = cx.product_id	
	where product_id in 
		   (select product_id	
			from MIVACatXProd			 
			group by product_id
			having COUNT(*) = 1)

	DECLARE @prodCode VARCHAR(255), @catCode VARCHAR(255), @catName VARCHAR(255)

	DECLARE catCursor CURSOR FOR 
	select p.code, c.CODE, c.CATNAME
	from MIVACatXProd cx join categories c on cx.cat_id = c.ID
						 join products p on p.mivaProdID = cx.product_id	
	where product_id in 
		   (select product_id	
			from MIVACatXProd			 
			group by product_id
			having COUNT(*) > 1)
	order by p.CODE, c.id

	OPEN catCursor
	FETCH NEXT FROM catCursor INTO @prodCode, @catCode, @catName

	WHILE @@FETCH_STATUS = 0
	   BEGIN
		UPDATE products
		SET categories = 
			CASE 
				WHEN categories IS NULL THEN @catName
				ELSE categories + '|' + @catName
			END ,
			catCode = 
			CASE 
				WHEN catCode IS NULL THEN @catCode
				ELSE catCode + ', ' + @catCode
			END
		WHERE CODE = @prodCode
	   
		FETCH NEXT FROM catCursor INTO @prodCode, @catCode, @catName
	   END

	CLOSE catCursor
	DEALLOCATE catCursor
END
GO
