USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[product_categorization_FULL_SP]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[product_categorization_FULL_SP]
AS
BEGIN
	DECLARE @pCode VARCHAR(1000), @cName VARCHAR(1000), @cCode VARCHAR(1000)

	UPDATE p
	SET p.catCode = NULL, p.categories = NULL
	FROM products p JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] i ON p.CODE = i.[No_] COLLATE Latin1_General_CS_AS

	DECLARE cCursor CURSOR FOR 
	SELECT prodCode, catCode
	FROM catxProd cx JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] i ON cx.prodCode = i.[No_] COLLATE Latin1_General_CS_AS
	GROUP BY prodCode, catCode
	ORDER BY prodCode

	OPEN cCursor
	FETCH NEXT FROM cCursor INTO @pCode, @cCode

	WHILE @@FETCH_STATUS = 0
	   BEGIN
		SELECT @cName = CATNAME FROM categories WHERE code = @cCode
		
		UPDATE products
		SET catCode = 
				CASE	
					WHEN catCode IS NULL THEN @cCode
					ELSE catCode + ', ' + @cCode
				END,
			categories = 
				CASE	
					WHEN categories IS NULL THEN @cName
					ELSE categories + '|' + @cName
				END
		WHERE CODE = @pCode	
	   
		FETCH NEXT FROM cCursor INTO @pCode, @cCode
	   END
	   
	CLOSE cCursor
	DEALLOCATE cCursor
END
GO
