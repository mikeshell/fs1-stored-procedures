USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[category_catxprod_cleanUp]    Script Date: 02/01/2013 11:09:50 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[category_catxprod_cleanUp]
AS
BEGIN
	--DELETE DUPLICATE CATXPROD ENTRY
	DECLARE @catid INT, @prodid INT, @primarycat BIT, @id INT

	DECLARE cCursor CURSOR FOR  	
	SELECT catid, prodid, primarycat
	FROM catxprod
	GROUP BY catid, prodid, primarycat
	HAVING COUNT(*) > 1
		
	OPEN cCursor
	FETCH NEXT FROM cCursor INTO @catid, @prodid, @primarycat

	WHILE @@FETCH_STATUS = 0
	   BEGIN
		SET @id = ''
		
		SELECT TOP 1 @id = id
		FROM catxprod
		WHERE catid = @catid AND prodid = @prodid AND primaryCat = @primarycat
		ORDER BY id ASC
		
		DELETE FROM catxprod
		WHERE catid = @catid 
			AND prodid = @prodid 
			AND primaryCat = @primarycat 
			AND id <> @id

		FETCH NEXT FROM cCursor INTO @catid, @prodid, @primarycat
	   END

	CLOSE cCursor
	DEALLOCATE cCursor
				
	--ASSIGNED PRIMARYCAT IF THEY ARE ONLY 1 CATEGORIZATION AND IF IT IS SET PRIMARYCAT = 0	
	--UTILIZING PRODUCTTMP TABLE TO PROCESS DATA
	--MIVAPRODID = PRODID
	TRUNCATE TABLE productTMP	

	INSERT INTO productTMP(mivaProdID)
	SELECT prodid
	FROM catxprod
	GROUP BY prodid
	HAVING COUNT(*) = 1

	DELETE t
	FROM productTMP t JOIN catxProd cx ON t.mivaProdID = cx.prodID
	WHERE cx.primaryCat = 1

	UPDATE cx
	SET cx.primaryCat = 1
	FROM productTMP t JOIN catxProd cx ON t.mivaProdID = cx.prodID

	TRUNCATE TABLE productTMP
END
GO
