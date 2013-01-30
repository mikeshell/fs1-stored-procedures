USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[category_featuredProducts]    Script Date: 01/30/2013 16:26:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[category_featuredProducts]
AS
BEGIN
	DECLARE @catID INT, @cID INT, @code VARCHAR(50), @counter TINYINT, @endleaf INT, 
			@catCode VARCHAR(100), @sSQL VARCHAR(MAX), @superCat INT

	SET @sSQL = ''
		
	UPDATE categories SET featuredProd1 = NULL, featuredProd2 = NULL

	DECLARE catCursor CURSOR FOR 
	SELECT distinct id, code, endleaf, superCatID
	FROM categories
	WHERE ACTIVE = 1 

	OPEN catCursor
	FETCH NEXT FROM catCursor INTO @catID, @catCode, @endleaf, @superCat

	WHILE @@FETCH_STATUS = 0
	   BEGIN
		SET @counter = 1	
	   
		IF @endleaf = 1
		   BEGIN
			SET @sSQL = 'DECLARE pCursor CURSOR FOR 
						SELECT TOP 2 m.catid, p.code
						FROM catxprod m JOIN categories c ON m.catid = c.ID
											JOIN products p ON m.prodid = p.prodID
						WHERE c.ACTIVE = 1 AND p.ACTIVE = 1 AND m.catid = ' + CAST(@catID AS VARCHAR(10)) + 
						' AND c.primaryCat = 1 AND superCatID = ' + CAST(@superCat AS VARCHAR(10)) + ' ORDER BY price*SALES90 DESC, p.qtyonhand DESC'
		   END
		ELSE
		   BEGIN
			SET @sSQL = 'DECLARE pCursor CURSOR FOR 
						SELECT top 2 ' + CAST(@catID AS VARCHAR(10)) + ', code
						FROM products
						WHERE ProdID IN 
							(
							SELECT prodid
							FROM categories c JOIN catxprod m ON c.ID = m.catid
							WHERE breadcrumb LIKE ' + 
								CASE 
									WHEN @catCode = 'shop-by-vendor' THEN '''%brands.A-C.1.html%'''
									ELSE '''%' + @catCode + '.html%'''
								END  
						--	+ ' AND superCatID = ' + CAST(@superCat AS VARCHAR(10)) + ' AND endleaf = 1
							+ ' AND endleaf = 1
							)
						ORDER BY price*SALES90 DESC, qtyonhand DESC'
		   END
		   
		EXEC(@sSQL)
		   
		OPEN pCursor
		FETCH NEXT FROM pCursor INTO @cID, @code

		WHILE @@FETCH_STATUS = 0
		   BEGIN
			IF @counter = 1
			   BEGIN
				SET @counter = 2
				UPDATE categories SET featuredProd1 = @code WHERE ID = @cID
			   END
			ELSE
			   BEGIN
				SET @counter = 1
				UPDATE categories SET featuredProd2 = @code WHERE ID = @cID		   
			   END
			   
			FETCH NEXT FROM pCursor INTO  @cID, @code
		   END

		CLOSE pCursor
		DEALLOCATE pCursor	
		
		FETCH NEXT FROM catCursor INTO @catID, @catCode, @endleaf, @superCat
	   END

	CLOSE catCursor
	DEALLOCATE catCursor

END
GO
