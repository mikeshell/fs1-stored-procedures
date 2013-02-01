USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[sitemap_SP]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[sitemap_SP]
AS
BEGIN

DECLARE @jobID INT
INSERT INTO jobHistory(jobName, startdT) values('SITEMAP_FEED', GETDATE())
SET @jobID = @@identity

DECLARE @code VARCHAR(100), @string VARCHAR(1000), @frequency VARCHAR(50), @date VARCHAR(16), @updateDT DATETIME, @counter INT, @page INT, @priority float
DECLARE @month VARCHAR(2), @day VARCHAR(2), @year VARCHAR(4), @type VARCHAR(20), @order INT, @target INT, @currentType VARCHAR(50), @url VARCHAR(255)

DECLARE @index VARCHAR(10), @count INT, @pageType VARCHAR(25), @dow INT, @pagenum INT

SET @counter = 0
SET @page = 1
SET @target = 50000
SET @currentType = 'David'
SET @dow = DATEPART(WEEKDAY, GETDATE())

--DELETING ENTRY FROM URLERROR IF IT IS OLDER THAN 7 DAYS
DELETE FROM urlError WHERE DATEDIFF(DAY, insertDT, GETDATE()) > 14

--IDENTIFYING THE NUMBER OF PAGES NEEDED FOR FULL PROD LIST
SELECT @pagenum = 
	CASE
	  WHEN COUNT(*)%50000 > 0 THEN 1 + COUNT(*)/50000
	  ELSE COUNT(*)/50000
	END
FROM products
WHERE active = 1 and isWeb = 1

TRUNCATE TABLE sitemap2

IF @dow BETWEEN 2 AND 6
   BEGIN
	SELECT @target = COUNT(*)/@pagenum + COUNT(*)%@pagenum FROM PRODUCTS WHERE active = 1 AND isWeb = 1 AND DATEDIFF(day, updateDT, GETDATE()) < 8			
	
	DECLARE sCursor CURSOR FOR 
	SELECT code, url, .5, 'CAT', 2 FROM categories WHERE active = 1 AND URL IS NOT NULL 
	UNION
	SELECT code, 'http://www.katom.com/' + code + '.html' url, 
			CASE
				WHEN DATEDIFF(DAY, updatedt, GETDATE()) < 30 THEN .9
				ELSE .7
			END, 'PROD', 1 
	FROM PRODUCTS WHERE active = 1 AND isWeb = 1 AND DATEDIFF(day, updateDT, GETDATE()) < 8
	UNION
	SELECT code, dbo.[asciiConverter](url), .9, 'OTHER', 3 FROM urlError
	ORDER BY 5, 3 desc
   END
ELSE
  BEGIN
	
	DECLARE sCursor CURSOR FOR 
	SELECT code, url, .5, 'CAT', 2 FROM categories WHERE active = 1 AND URL IS NOT NULL
	UNION
	SELECT code, 'http://www.katom.com/' + code + '.html' url, 
			CASE
				WHEN DATEDIFF(DAY, updatedt, GETDATE()) < 30 THEN .9
				ELSE .7
			END, 'PROD', 1 
	FROM PRODUCTS WHERE active = 1 AND isWeb = 1
	UNION
	SELECT code, dbo.[asciiConverter](url), .9, 'OTHER', 3 FROM urlError
	ORDER BY 5, 3 desc
  
  END
OPEN sCursor
FETCH NEXT FROM sCursor INTO @code, @url, @priority, @type, @order

WHILE @@FETCH_STATUS = 0
   BEGIN
	SET @string = '	<url>
		<loc>%CODE%</loc>
		<changefreq>%FREQUENCY%</changefreq>
		<lastmod>%DATE%</lastmod>
		<priority>%PRIORITY%</priority>
	</url>'
	SET @counter = @counter + 1
	IF (@currentType = 'David')
	   BEGIN
		SET @currentType = @type
	   END
	   
	IF @type = 'CAT' 
	   BEGIN
		SET @updateDT = DATEADD(DAY, -1, GETDATE())			
		SET @frequency = 'weekly'
	   END
	ELSE
	   BEGIN
		SELECT @updateDT = ISNULL(updateDT, DATEADD(DAY, -7, GETDATE())) FROM products WHERE code = @code
		SET @frequency = 'daily'
		
		IF DATEDIFF(DAY, @updateDT, getdate()) > 7
			SET @updateDT = DATEADD(DAY, -7, GETDATE())	
	   END
	
	SET @year = CAST(YEAR(@updateDT) AS VARCHAR(4))
		
	IF MONTH(@updateDT) < 10 
		SET @month = '0' + CAST(MONTH(@updateDT) AS VARCHAR(4))
	ELSE
		SET @month = CAST(MONTH(@updateDT) AS VARCHAR(4))
	
	IF DAY(@updateDT) < 10 
		SET @day = '0' + CAST(DAY(@updateDT) AS VARCHAR(4))
	ELSE
		SET @day = CAST(DAY(@updateDT) AS VARCHAR(4))	

	SET @date = @year + '-' + @month + '-' + @day

	SET @string = REPLACE(@string, '%CODE%', @url)
	SET @string = REPLACE(@string, '%FREQUENCY%', @frequency)
	SET @string = REPLACE(@string, '%DATE%', @date)
	SET @string = REPLACE(@string, '%PRIORITY%', @priority)
	
	/** 
	-- THIS IS TO ONLY LIMIT THE NUMBER OF LISTING PER FILE

	IF @type <> @currentType
		BEGIN
			SET @counter = 1
			SET @currentType = @type
			SET @page = 1			
		END
	ELSE 
		BEGIN
			IF @counter = @target 
			   BEGIN
				SET @counter = 1
				SET @page = @page + 1
			   END	
		END
	**/
	
	IF @type <> @currentType
		BEGIN
			SET @counter = 1
			SET @currentType = @type
			SET @page = 1			
		END
	ELSE 
	   BEGIN
		IF @type = 'PROD'
		   BEGIN
			IF @counter = @target 
			   BEGIN
				SET @counter = 1
				SET @page = @page + 1
			   END			
		   END 	   
	   END
	
	INSERT INTO sitemap2([content], pageNum, pagetype, priority) VALUES(@string, @page, @type, @order)

	FETCH NEXT FROM sCursor INTO @code, @url, @priority, @type, @order
   END

CLOSE sCursor
DEALLOCATE sCursor	

--REMOVING HENNY PENNY
DELETE FROM sitemap2 
WHERE pagetype = 'cat'
	AND [content] LIKE '%henny-penny%'

DELETE FROM sitemap2 
WHERE pagetype = 'Prod'
	AND [content] LIKE '%www.katom.com/540-%'

UPDATE jobHistory SET endDT = GETDATE() WHERE jobID = @jobID

--SET PRIORITY TO 7 FOR FIRST SITEMAP PAGE
UPDATE sitemap2
SET [Content] = REPLACE([Content], '<priority>0.6</priority>', '<priority>0.7</priority>')
WHERE id < 5000

--SET PRIORITY TO 8 FOR ALL TRUE PAGES
UPDATE sitemap2
SET [Content] = REPLACE([Content], '<priority>0.6</priority>', '<priority>0.8</priority>')
WHERE [Content] LIKE '%http://www.katom.com/598-%'


--INSERTING OTHER PAGES
SET @updateDT = GETDATE()

TRUNCATE TABLE sitemapWorktable

INSERT INTO sitemapWorktable
SELECT LEFT([name], 1), 0, 'products'
FROM Products
WHERE active = 1 AND dbo.isAlphaNumberic(LEFT([name], 1)) = 1 AND isWeb = 1 AND LEN(LEFT([name], 1)) > 0
GROUP BY LEFT([name], 1)
UNION
SELECT LEFT(CATNAME, 1), COUNT(*), 'categories'
FROM categories
WHERE active = 1 AND isnumeric(LEFT(CATNAME, 1)) = 0 AND LEN(LEFT(CATNAME, 1)) > 0
GROUP BY LEFT(CATNAME, 1)
ORDER BY 3, 1

--SPECIAL CASE FOR CATEGORIES
INSERT INTO sitemapWorktable VALUES('0-9', (SELECT COUNT(*) FROM categories WHERE ACTIVE = 1 AND ISNUMERIC(LEFT(CATNAME, 1)) = 1), 'categories')

--SPECIAL CASE FOR BRANDS
INSERT INTO sitemapWorktable VALUES('0-9', (SELECT COUNT(*) FROM categories WHERE ACTIVE = 1 AND vendor = 1 AND PARENT_ID = 5844 AND isnumeric(LEFT(CATNAME, 1)) = 1), 'brANDs')
INSERT INTO sitemapWorktable VALUES('A-C', (SELECT COUNT(*) FROM categories WHERE ACTIVE = 1 AND vendor = 1 AND PARENT_ID = 5844 AND LEFT(CATNAME, 1) BETWEEN 'A' AND 'C'), 'brANDs')
INSERT INTO sitemapWorktable VALUES('D-F', (SELECT COUNT(*) FROM categories WHERE ACTIVE = 1 AND vendor = 1 AND PARENT_ID = 5844 AND LEFT(CATNAME, 1) BETWEEN 'D' AND 'F'), 'brANDs')
INSERT INTO sitemapWorktable VALUES('G-I', (SELECT COUNT(*) FROM categories WHERE ACTIVE = 1 AND vendor = 1 AND PARENT_ID = 5844 AND LEFT(CATNAME, 1) BETWEEN 'G' AND 'I'), 'brANDs')
INSERT INTO sitemapWorktable VALUES('J-L', (SELECT COUNT(*) FROM categories WHERE ACTIVE = 1 AND vendor = 1 AND PARENT_ID = 5844 AND LEFT(CATNAME, 1) BETWEEN 'J' AND 'L'), 'brANDs')
INSERT INTO sitemapWorktable VALUES('M-O', (SELECT COUNT(*) FROM categories WHERE ACTIVE = 1 AND vendor = 1 AND PARENT_ID = 5844 AND LEFT(CATNAME, 1) BETWEEN 'M' AND 'O'), 'brANDs')
INSERT INTO sitemapWorktable VALUES('P-R', (SELECT COUNT(*) FROM categories WHERE ACTIVE = 1 AND vendor = 1 AND PARENT_ID = 5844 AND LEFT(CATNAME, 1) BETWEEN 'P' AND 'R'), 'brANDs')
INSERT INTO sitemapWorktable VALUES('S-U', (SELECT COUNT(*) FROM categories WHERE ACTIVE = 1 AND vendor = 1 AND PARENT_ID = 5844 AND LEFT(CATNAME, 1) BETWEEN 'S' AND 'U'), 'brANDs')
INSERT INTO sitemapWorktable VALUES('V-Z', (SELECT COUNT(*) FROM categories WHERE ACTIVE = 1 AND vendor = 1 AND PARENT_ID = 5844 AND LEFT(CATNAME, 1) BETWEEN 'V' AND 'Z'), 'brANDs')

/**
DECLARE catCursor CURSOR FOR 
select LEFT([name], 1), COUNT(*)
FROM Products
WHERE active = 1 AND isWeb = 1
group by LEFT([name], 1)
order by 1

OPEN catCursor
FETCH NEXT FROM catCursor INTO @index, @count

WHILE @@FETCH_STATUS = 0
   BEGIN
	
	IF dbo.isAlphaNumberic(@index) = 0
		UPDATE sitemapWorktable SET countTT = countTT + @count WHERE myIndex = '0'
		
	UPDATE sitemapWorktable SET countTT = countTT + @count WHERE myIndex = @index AND pageType = 'products'	
		
	FETCH NEXT FROM catCursor INTO @index, @count
   END

CLOSE catCursor
DEALLOCATE catCursor

DECLARE catCursor CURSOR FOR 
SELECT myIndex,
	CASE
		WHEN countTT%100 > 0 THEN (countTT/100)+1
		ELSE countTT/100
	END, pageType
FROM sitemapWorktable
WHERE countTT > 0
ORDER BY 3, 1

OPEN catCursor
FETCH NEXT FROM catCursor INTO @index, @count, @pageType

WHILE @@FETCH_STATUS = 0
   BEGIN
	SET @string = '	<url>
		<loc>%CODE%</loc>
		<changefreq>%FREQUENCY%</changefreq>
		<lastmod>%DATE%</lastmod>
		<priority>%PRIORITY%</priority>
	</url>'
	SET @year = CAST(YEAR(@updateDT) AS VARCHAR(4))
		
	IF MONTH(@updateDT) < 10 
		SET @month = '0' + CAST(MONTH(@updateDT) AS VARCHAR(4))
	ELSE
		SET @month = CAST(MONTH(@updateDT) AS VARCHAR(4))
	
	IF DAY(@updateDT) < 10 
		SET @day = '0' + CAST(DAY(@updateDT) AS VARCHAR(4))
	ELSE
		SET @day = CAST(DAY(@updateDT) AS VARCHAR(4))	

	SET @date = @year + '-' + @month + '-' + @day
	
	WHILE (@count > 0)
	   BEGIN
		SET @url = 'http://www.katom.com/' + @pageType + '.' + @index + '.' + cast(@count as VARCHAR(10)) + '.html'
		SET @count = @count - 1
		
		SET @string = REPLACE(@string, '%CODE%', @url)
		SET @string = REPLACE(@string, '%FREQUENCY%', 'daily')
		SET @string = REPLACE(@string, '%DATE%', @date)
		SET @string = REPLACE(@string, '%PRIORITY%', '.6')
		
		INSERT INTO sitemap2([content], pageNum, pagetype, priority) VALUES(@string, 1, 'OTHER', 3)
		
		SET @string = '	<url>
		<loc>%CODE%</loc>
		<changefreq>%FREQUENCY%</changefreq>
		<lastmod>%DATE%</lastmod>
		<priority>%PRIORITY%</priority>
	</url>'
	   END
		
    
	FETCH NEXT FROM catCursor INTO @index, @count, @pageType
   END

CLOSE catCursor
DEALLOCATE catCursor
**/
DELETE FROM sitemap2 WHERE content LIKE '%599-%'

END
GO
