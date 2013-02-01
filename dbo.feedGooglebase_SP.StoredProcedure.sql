USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[feedGooglebase_SP]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[feedGooglebase_SP]
AS
BEGIN

/**
DECLARE @jobID INT
INSERT INTO jobHistory(jobName, startdT) values('GOOGLEBASE_NEW_FEED', GETDATE())
SET @jobID = @@identity

TRUNCATE TABLE feed_googlebase

INSERT INTO feed_googlebase ([id], [title], [description], [google_product_category], [link], [image_link], 
						     [condition], [price], [sale_price], [brand], [mpn], [shipping], [shipping_weight], 
						     [adWords_grouping], [adwords_labels], [adwords_redirect], [product_type], [adwords_publish], 
						     [expiration_date], [Code])
SELECT LOWER(REPLACE([dbo].[RemoveNonAlphaNumericCharacters](m.mfgShortName), ' ', '') + '-' + RIGHT(p.code, LEN(p.code)-4)) id,
	ISNULL(m.mfgShortName, '') + ' ' + ISNULL(p.mpn, '') + ' ' + ISNULL(p.name, '') [title],
	CASE 
		WHEN LEN(ISNULL(extendedDescription, '')) = 0 THEN CAST(p.prodDesc AS VARCHAR(MAX))
		ELSE extendedDescription
	END + ' (' + '%CODE%' + ')' [description],	
	dbo.cleaner(ISNULL(ce.google, 'Home & Garden > Kitchen & Dining')) [google_product_category],
	'http://www.katom.com/' + LTRIM(RTRIM(p.code)) + '.html?CID=GoogleBase3&amp;utm_source=googlebase3&amp;utm_medium=CSE&amp;utm_campaign=CSE&amp;zmam=29342707&amp;zmas=1&amp;zmac=1&amp;zmap=' + p.code [link],
	'http://' + p.mfgid + '.katomcdn.com/' + LOWER(p.image) + '_large.jpg'  [image_link],
	'New' Condition,
	p.listPrice price,		
	CASE
		WHEN ISNULL(p.map, 0) = 1 THEN 
			CASE 
				WHEN ISNULL(p.feedMap, 0) = 1 THEN 
					CASE
						WHEN ISNULL(p.MAP_Program, 'K') = 'K' THEN CAST(p.price AS DECIMAL(10, 2))
						ELSE CAST(p.map_price AS DECIMAL(10, 2))
					END
				ELSE CAST(p.price AS DECIMAL(10, 2))
			END
		ELSE CAST(p.price AS DECIMAL(10, 2))
	END [sale_price],
	dbo.cleaner(ISNULL(m.mfgName, '')) [brand],
	REPLACE(dbo.[googleBaseMPNcleaner](p.mpn), '&', '-') mpn,
	CASE p.freeshipping
		WHEN '1' THEN 1 
		WHEN 'T' THEN 1 
		ELSE 0 
	END [shipping],
	p.weight [weight],
	dbo.cleaner(ISNULL(c.superCat, 'uncat')) [Adwords_Grouping], 
	dbo.cleaner(ISNULL(c.CATNAME, 'uncat')) [adwords_labels],
	'http://www.katom.com/' + LTRIM(RTRIM(p.code)) + '?utm_source=googlebase3&amp;utm_medium=Adwords&amp;utm_campaign=CSE&amp;CID=GoogleBase3&amp;zmam=29342707&amp;zmas=1&amp;zmac=32&amp;zmap=' + p.code [adwords_redirect],
	ISNULL(dbo.cleaner(c.breadcrumb), 'Restaurant Supplies') [product_type],
	'true' [adwords_publish],	
	CAST(MONTH(DATEADD(DAY, 14, GETDATE())) AS VARCHAR(2)) + '/' + 
	CAST(DAY(DATEADD(DAY, 14, GETDATE())) AS VARCHAR(2)) + '/' + 
	CAST(YEAR(DATEADD(DAY, 14, GETDATE())) AS VARCHAR(4)) [expiration_date],
	p.CODE
FROM products p JOIN mfg m ON p.mfgID = m.mfgID
				LEFT JOIN categories c ON c.CODE = p.primaryCatCode AND c.primaryCat = 1
				LEFT JOIN cseCatMapping ce ON ce.catCode = p.primaryCatCode
WHERE p.ACTIVE = 1 AND p.isWeb = 1 AND p.mfgid NOT IN ('599')

-- SHRINKING TITLE TO 70 CHARACTERS & MOVING ORIGINAL TITLE TO PROD DESC
UPDATE feed_googlebase 
SET [description] = title + '. ' + [description] ,
	title = LTRIM(RTRIM(LEFT(title, 70))), 
	titleChange = 1 
WHERE LEN(title) > 70

WHILE EXISTS(SELECT id FROM feed_googlebase WHERE titlechange = 1 AND LEN(title) > 70 AND dbo.isAlphaNumberic(RIGHT(title, 1)) = 0 )	
	BEGIN
		UPDATE feed_googlebase 
		SET title = RTRIM(LEFT(title, LEN(title)-1))
		WHERE LEN(title) > 70 
			AND dbo.isAlphaNumberic(RIGHT(title, 1) ) = 0
			AND titlechange = 1 
	END

UPDATE feed_googlebase SET [description] = dbo.[googleBasecleaner]([description]) WHERE [description] like '%<%'

UPDATE feed_googlebase SET [description] = REPLACE([description], '%CODE%', brand + ' ' + MPN), title = dbo.cleaner(title)

UPDATE feed_googlebase SET [description] = dbo.cleaner([description])

UPDATE jobHistory SET endDT = GETDATE() WHERE jobID = @jobID
**/

SELECT id, title, [description], google_product_category, link, image_link, condition, 
		price, sale_price, brand, MPN, shipping, shipping_weight, adwords_grouping, adwords_labels, 
		adwords_redirect, product_type, adwords_publish, expiration_date
FROM feed_googlebase
ORDER BY code 

END
GO
