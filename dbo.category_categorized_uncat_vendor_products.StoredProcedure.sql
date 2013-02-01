USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[category_categorized_uncat_vendor_products]    Script Date: 02/01/2013 11:09:50 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
-- Modified: 4-14-2011 by Mike S., Removed references to Merchant2 per Beau D.


CREATE PROCEDURE [dbo].[category_categorized_uncat_vendor_products]
AS
BEGIN
	DECLARE @id INT, @catName VARCHAR(500), @numProd INT, @children INT, @prodCat INT, @mfgid VARCHAR(3)

	DECLARE catCursor CURSOR FOR 
	SELECT ID, catname, mfgid,
		(SELECT COUNT(*) FROM products WHERE ACTIVE = 1 and mfgid = c.mfgID) numProd,
		(SELECT COUNT(*) FROM categories WHERE ACTIVE = 1 and PARENT_ID = c.ID) children,
		(SELECT COUNT(*) FROM products WHERE ACTIVE = 1 and CHARINDEX(isnull(c.catname, ''), categories) > 0) prodCat
	FROM categories c
	WHERE ACTIVE = 1 AND PARENT_ID = 5844
		AND (SELECT COUNT(*) FROM categories WHERE ACTIVE = 1 and PARENT_ID = c.ID) = 0

	OPEN catCursor
	FETCH NEXT FROM catCursor INTO @id, @catName, @mfgid, @numProd, @children, @prodCat

	WHILE @@FETCH_STATUS = 0
	   BEGIN
		
		UPDATE products
		SET categories = 
				CASE 
					WHEN LEN(ISNULL(categories, '')) = 0 THEN @catName
					ELSE categories + '|' + @catName
				END
		WHERE ACTIVE = 1 AND mfgID = @mfgid
			AND CHARINDEX(@catName, ISNULL(categories, '')) = 0   
	   
		FETCH NEXT FROM catCursor INTO @id, @catName, @mfgid, @numProd, @children, @prodCat
	   END

	CLOSE catCursor
	DEALLOCATE catCursor

END
GO
