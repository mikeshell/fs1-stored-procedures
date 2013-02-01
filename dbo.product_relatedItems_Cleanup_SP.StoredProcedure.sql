USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[product_relatedItems_Cleanup_SP]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[product_relatedItems_Cleanup_SP]
	@type VARCHAR(20)
AS
BEGIN
	DECLARE @code VARCHAR(255), @r VARCHAR(1000), @code2 VARCHAR(255), @r2 VARCHAR(1000), 
			@sSQL VARCHAR(MAX), @sSQL2 VARCHAR(MAX)

	SET @sSQL = ''
	SET @sSQL2 = ''
	
	IF @type = 'FULL'
	   BEGIN		
		DECLARE catCursor CURSOR FOR 
		SELECT code, REPLACE(relateditems, ', ', ''', ''')
		FROM products 
		WHERE ACTIVE = 1 AND LEN(ISNULL(relateditems, '')) > 0
		order by code
	   END
	ELSE
	   BEGIN
		DECLARE catCursor CURSOR FOR 
		SELECT code, REPLACE(relateditems, ', ', ''', ''')
		FROM products 
		WHERE ACTIVE = 1 AND LEN(ISNULL(relateditems, '')) > 0
			AND DATEDIFF(DAY, updatedt, GETDATE()) BETWEEN 0 AND 1
		order by code
	   END

	OPEN catCursor
	FETCH NEXT FROM catCursor INTO @code, @r

	WHILE @@FETCH_STATUS = 0
	   BEGIN
		SET @r2 = ''
		SET @sSQL = 'DECLARE pCursor CURSOR FOR
					 SELECT code FROM products WHERE ACTIVE = 1 AND isWeb = 1 AND CODE IN (''' + @r + ''')'
		EXEC(@sSQL)
		   
		OPEN pCursor
		FETCH NEXT FROM pCursor INTO @code2

		WHILE @@FETCH_STATUS = 0
		   BEGIN
			SET @r2 = @r2 + CASE WHEN LEN(@r2) > 0 THEN ', ' ELSE '' END + @code2
			FETCH NEXT FROM pCursor INTO  @code2
		   END
		
		CLOSE pCursor
		DEALLOCATE pCursor	

		UPDATE products
		SET relatedItems = @r2
		WHERE CODE = @code
		
		FETCH NEXT FROM catCursor INTO @code, @r
	   END

	CLOSE catCursor
	DEALLOCATE catCursor
END
GO
