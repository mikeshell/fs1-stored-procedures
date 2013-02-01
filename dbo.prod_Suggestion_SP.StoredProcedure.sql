USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[prod_Suggestion_SP]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[prod_Suggestion_SP]
AS
BEGIN
	--Delete all inactive products
	TRUNCATE TABLE prodSuggestionToRemove

	INSERT INTO prodSuggestionToRemove
	SELECT id FROM prod_suggestion WHERE suggested_Prod not in (SELECT code FROM ProductsMIVA WHERE active = 1)

	INSERT INTO prodSuggestionToRemove
	SELECT id FROM prod_suggestion 
	WHERE code not in (SELECT code FROM ProductsMIVA WHERE active = 1) 
			AND id not in (SELECT PSID FROM prodSuggestionToRemove)

	--Clearing out current suggested products for fresh index
	TRUNCATE TABLE prod_suggestionWorktable

	INSERT INTO prod_suggestionWorktable
	SELECT DISTINCT TOP 2500 code, updatedt FROM prod_suggestion 
	ORDER BY updateDT ASC

	insert into prodSuggestionToRemove
	select id from prod_suggestion 
	WHERE code in (select code from prod_suggestionWorktable)
		and id NOT IN (select psid from prodSuggestionToRemove)

	delete from prod_suggestion WHERE id in (select PSID from prodSuggestionToRemove)
	
	UPDATE products
	SET prodSuggestionDT = NULL
	WHERE CODE IN (SELECT TOP 500 code FROM products
	WHERE prodSuggestionDT is not null
	ORDER BY prodSuggestionDT ASC)

	DECLARE @Code varchar(50), @mpn varchar(50), @mfgid varchar(3), @mfgName varchar(100), 
			@prodName varchar(250), @count smallint, @primaryCat varchar(100), @sSQL varchar(1000)
		
	DECLARE catCursor CURSOR FOR 
	SELECT code, mfgID, CASE 
			WHEN LEN(ISNULL(mpn, '')) = 0 THEN RIGHT(CODE, LEN(CODE)-4)
			ELSE MPN
		   END [MPN], mfgname, [Name], primaryCatCode
	FROM PRODUCTS 
	WHERE CODE IN (SELECT CODE FROM PRODUCTSMIVA WHERE ACTIVE = 1)
			AND code NOT IN (select distinct code from prod_suggestion)
	ORDER BY mfgid

	OPEN catCursor
	FETCH NEXT FROM catCursor INTO @Code, @mfgid, @mpn, @mfgName, @prodName, @primaryCat

	WHILE @@FETCH_STATUS = 0
	   BEGIN
		SET @sSQL = 'INSERT INTO prod_suggestion
			SELECT TOP 20 ''' + @Code + ''', CODE, mpn, mfgname, NAME, price, BBPrice, [image], GETDATE(), 1
			FROM products '
	    
		IF (SELECT COUNT(*) FROM products WHERE FREETEXT(mpn, @mpn) AND mfgID = @mfgid AND CODE IN (SELECT CODE FROM ProductsMIVA WHERE active = 1) AND MPN <> @mpn) > 3
		   BEGIN
			SET @sSQL = @sSQL + 'WHERE FREETEXT(mpn, ''' + @mpn + ''')'				
		   END
		ELSE	
		   BEGIN
			SET @sSQL = @sSQL + 'WHERE FREETEXT(*, ''' + replace(@prodName, '''', '''''') + ''')'
		   END
		   
		SET @sSQL = @sSQL + ' AND mfgID = ''' + @mfgid + ''' 
					AND CODE IN (SELECT CODE FROM ProductsMIVA WHERE active = 1) 
					AND MPN <> ''' + @mpn + ''' 
					AND CODE NOT IN (SELECT CODE FROM products WHERE catCode like ''%' + @primaryCat + '%'')'
		exec(@sSQL)
					
		FETCH NEXT FROM catCursor INTO @Code, @mfgid, @mpn, @mfgName, @prodName, @primaryCat
	   END

	CLOSE catCursor
	DEALLOCATE catCursor
	
	UPDATE products
	SET prodSuggestionDT = GETDATE()
	WHERE prodSuggestionDT IS NULL AND CODE NOT IN (SELECT DISTINCT CODE FROM prod_suggestion)
END
GO
