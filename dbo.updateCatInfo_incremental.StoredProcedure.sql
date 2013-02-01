USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[updateCatInfo_incremental]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[updateCatInfo_incremental]
AS
BEGIN

DECLARE @jobID INT
INSERT INTO jobHistory(jobName, startdT) values('PROD_CAT_UPD_INCR', GETDATE())
SET @jobID = @@identity

UPDATE products SET categories = NULL, catCode = NULL, primaryCatID = NULL 
WHERE DATEDIFF(DAY, GETDATE(), updateDT) = 0

UPDATE p
SET p.primaryCatID = c.id
from fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Item Category Codes] ic
		join fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Category Codes] cc
			on ic.[Category Code] = cc.[code]
		join mivacategories c on c.code = cc.Description COLLATE Latin1_General_CS_AS
		join products p on p.code = ic.[Item No_] COLLATE Latin1_General_CS_AS
where [Primary Category] = 1 AND DATEDIFF(DAY, GETDATE(), p.updateDT) = 0

DECLARE @prodID varchar(200), @catCode varchar(200), @catName varchar(200)

DECLARE pCursor CURSOR FOR 
SELECT ic.[Item No_], c.code, c.[NAME] 
FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Item Category Codes] ic
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Category Codes] cc
			ON ic.[Category Code] = cc.[code]
		JOIN mivacategories c ON c.code = cc.Description COLLATE Latin1_General_CS_AS
		JOIN products p ON p.code = ic.[Item No_] COLLATE Latin1_General_CS_AS
WHERE DATEDIFF(DAY, GETDATE(), p.updateDT) = 0
ORDER BY 1

OPEN pCursor
FETCH NEXT FROM pCursor INTO @prodID, @catCode, @catName

WHILE @@FETCH_STATUS = 0
   BEGIN
		UPDATE products
		SET categories = 
				CASE 
					WHEN categories IS NULL THEN @catName
					ELSE categories + ' | ' + @catName
				END,
			catCode = 
				CASE 
					WHEN catCode IS NULL THEN @catCode
					ELSE catCode + ', ' + @catCode
				END
		WHERE code = @prodID
	FETCH NEXT FROM pCursor INTO @prodID, @catCode, @catName
   END

CLOSE pCursor
DEALLOCATE pCursor

UPDATE jobHistory SET endDT = GETDATE() WHERE jobID = @jobID
END
GO
