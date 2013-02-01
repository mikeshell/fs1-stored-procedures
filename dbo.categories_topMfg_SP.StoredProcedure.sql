USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[categories_topMfg_SP]    Script Date: 02/01/2013 11:09:50 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[categories_topMfg_SP]
AS
BEGIN
	DECLARE @id INT, @code VARCHAR(255), @sSQL VARCHAR(MAX)

	TRUNCATE TABLE categories_topMfg
	DECLARE catCursor CURSOR FOR 
	SELECT id, code
	FROM categories 
	WHERE primaryCat = 1 AND ACTIVE = 1	AND PARENT_ID <> 5844 AND endleaf = 0
	ORDER BY PARENT_ID, id

	OPEN catCursor
	FETCH NEXT FROM catCursor INTO @id, @code

	WHILE @@FETCH_STATUS = 0
	   BEGIN
		TRUNCATE TABLE prodTemp
		
		SET @sSQL = 'INSERT INTO prodTemp([Mfg ID], [Product Name], [UnitPrice])
					 SELECT p.mfgid, p.mfgName, sum(p.sales30*price)
					 FROM products p JOIN categories c ON c.id = p.primaryCatID
					 WHERE breadcrumb LIKE ''%' + @code + '%'' OR categories LIKE ''%' + @code + '%'' 
					 GROUP BY p.mfgid, p.mfgName
					 ORDER BY 3 DESC'

		EXEC(@sSQL)
		
		INSERT INTO categories_topMfg(cid, mfgID, mfgName, insertDT)
		SELECT @id, [Mfg ID], [Product Name], GETDATE() FROM prodTemp ORDER BY UnitPrice DESC
		
		FETCH NEXT FROM catCursor INTO @id, @code
	   END

	CLOSE catCursor
	DEALLOCATE catCursor


END
GO
