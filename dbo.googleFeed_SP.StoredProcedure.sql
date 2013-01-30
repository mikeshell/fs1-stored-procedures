USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[googleFeed_SP]    Script Date: 01/30/2013 16:26:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[googleFeed_SP]
AS
BEGIN

DECLARE @jobID INT
INSERT INTO jobHistory(jobName, startdT) values('GOOGLEBASE_FEED', GETDATE())
SET @jobID = @@identity

TRUNCATE TABLE googleBaseFeed

INSERT INTO googleBaseFeed
SELECT DISTINCT
	'New' [condition],
	CASE cast(p.prodDesc as varchar(max)) 
			WHEN NULL THEN ''
			WHEN '' THEN ''
			WHEN '#NAME?' THEN ''
			ELSE cast(p.prodDesc as varchar(max))
	END + ' ( ' + UPPER(dbo.cleaner(isnull(m.mfgName, ''))) + ' - ' + '%CODE%' + ' ) ' [description],
	REPLACE(REPLACE(isnull(m.mfgShortName, ''), ' ', '-'), '.', '') 
		+ '-' + REPLACE(dbo.[googleBaseMPNcleaner](p.mpn), '&', '-') [id],
	'http://www.katom.com/' + LTRIM(RTRIM(p.code)) + '.html' [link],
	cast(p.price as decimal(10, 2)) [price],
	dbo.cleaner(isnull(m.mfgShortName, '')) + ' ' + 
		CASE 
			WHEN CHARINDEX(' ', mpn) > 0 THEN dbo.[googleBaseMPNcleaner](LEFT(p.mpn, CHARINDEX(' ', mpn)-1))
			ELSE dbo.[googleBaseMPNcleaner](replace(p.mpn, '&', '-'))
		END + ' - ' + dbo.cleaner(p.Name) [title],
	dbo.cleaner(isnull(m.mfgName, '')) [brand],
	'http://' + p.mfgid + '.katomcdn.com/' + LOWER(p.image) + '_large.jpg'  [image_link],
	dbo.cleaner(REPLACE(REPLACE(isnull(m.mfgShortName, ''), ' ', '-'), '.', ''))
		+ '-' + REPLACE(dbo.[googleBaseMPNcleaner](p.mpn), '&', '-') [mpn],
	'' [product_type],
	p.weight [weight],
	cast(month(dateadd(day, 14, getdate())) as varchar(2)) + '/' + 
	cast(day(dateadd(day, 14, getdate())) as varchar(2)) + '/' + 
	cast(year(dateadd(day, 14, getdate())) as varchar(4)) [expiration_date],
	CASE p.freeshipping
		WHEN '1' THEN 1 
		WHEN 'T' THEN 1 
		ELSE 0 
	END [shipping],
	dbo.cleaner(p.keywords) [keywords],
	p.code, p.upc, 
	dbo.cleaner(isnull(c.superCat, 'uncat')) [Adwords_Grouping], 
	dbo.cleaner(ISNULL(c.CATNAME, 'uncat')) [adwords_labels],
	dbo.cleaner(isnull(c2.google, 'Home & Garden > Kitchen & Dining')) [google_product_category],
	'true' [adwords_publish],
	'' [adwords_redirect]	
FROM products p left join mfg m on p.mfgid = m.mfgid
				left join categories c on c.CODE = p.primaryCatCode AND c.primaryCat = 1
				left join cseCatMapping c2 on c.CODE = c2.catCode
WHERE p.active = 1 
	AND ISNULL(p.price, 0) > 0 
	AND p.feedMAP <> 3 
	AND ISNULL(p.MAP_Program, 'A') <> 'X'
	AND ISNULL(webactive, 0) = 1
	AND (CHARINDEX('-', p.mpn) > 0 
			OR CHARINDEX('.', p.mpn) > 0 
			OR CHARINDEX(' ', p.mpn) > 0 
			OR CHARINDEX('&', p.mpn) > 0 
			OR CHARINDEX('/', p.mpn) > 0)
UNION
SELECT DISTINCT
	'New' [condition],
	CASE cast(p.prodDesc as varchar(max)) 
			WHEN NULL THEN ''
			WHEN '' THEN ''
			WHEN '#NAME?' THEN ''
			ELSE cast(p.prodDesc as varchar(max))
	END + ' ( ' + UPPER(dbo.cleaner(isnull(m.mfgName, ''))) + ' - ' + '%CODE%' + ' ) ' [description],
	cast(p.mfgid as varchar(10)) + '-' + [dbo].[googleBaseMPNcleaner](p.mpn) [id],
	'http://www.katom.com/' + LTRIM(RTRIM(p.code)) + '.html' [link],
	cast(p.price as decimal(10, 2)) [price],
	dbo.cleaner(isnull(m.mfgShortName, '')) + ' ' + 
		CASE 
			WHEN CHARINDEX(' ', mpn) > 0 THEN dbo.[googleBaseMPNcleaner](LEFT(p.mpn, CHARINDEX(' ', mpn)-1))
			ELSE dbo.[googleBaseMPNcleaner](replace(p.mpn, '&', '-'))
		END + ' - ' + dbo.cleaner(p.Name) [title],
	dbo.cleaner(isnull(m.mfgName, '')) [brand],
	'http://' + p.mfgid + '.katomcdn.com/' + LOWER(p.image) + '_large.jpg' [image_link],
	cast(p.mfgid as varchar(10)) + '-' + [dbo].[googleBaseMPNcleaner](p.mpn) [mpn],
	'' [product_type],
	p.weight [weight],
	cast(month(dateadd(day, 14, getdate())) as varchar(2)) + '/' + 
	cast(day(dateadd(day, 14, getdate())) as varchar(2)) + '/' + 
	cast(year(dateadd(day, 14, getdate())) as varchar(4)) [expiration_date],
	CASE p.freeshipping
		WHEN '1' THEN 1 
		WHEN 'T' THEN 1 
		ELSE 0 
	END [shipping],
	dbo.cleaner(p.keywords) [keywords],
	p.code, p.upc, 
	dbo.cleaner(isnull(c.superCat, 'uncat')) [Adwords_Grouping], 
	dbo.cleaner(ISNULL(c.CATNAME, 'uncat')) [adwords_labels],
	dbo.cleaner(isnull(c2.google, 'Home & Garden > Kitchen & Dining')) [google_product_category],
	'true' [adwords_publish],
	'' [adwords_redirect]	
FROM products p left join mfg m on p.mfgid = m.mfgid
				left join categories c on c.CODE = p.primaryCatCode AND c.primaryCat = 1
				left join cseCatMapping c2 on c.CODE = c2.catCode
WHERE p.active = 1 
	AND ISNULL(p.price, 0) > 0 
	AND p.feedMAP <> 3 
	AND ISNULL(p.MAP_Program, 'A') <> 'X'
	AND ISNULL(webactive, 0) = 1
	AND (CHARINDEX('-', p.mpn) > 0 
			OR CHARINDEX('.', p.mpn) > 0 
			OR CHARINDEX(' ', p.mpn) > 0 
			OR CHARINDEX('&', p.mpn) > 0 
			OR CHARINDEX('/', p.mpn) > 0)
UNION
SELECT DISTINCT
	'New' [condition],
	CASE cast(p.prodDesc as varchar(max)) 
			WHEN NULL THEN ''
			WHEN '' THEN ''
			WHEN '#NAME?' THEN ''
			ELSE cast(p.prodDesc as varchar(max))
	END + ' ( ' + UPPER(dbo.cleaner(isnull(m.mfgName, ''))) + ' - ' + '%CODE%' + ' ) ' [description],
	dbo.[googleBaseMPNcleaner](REPLACE(cp.[mpn], '&', '-')) [id],
	'http://www.katom.com/' + LTRIM(RTRIM(p.code)) + '.html' [link],
	cast(p.price as decimal(10, 2)) [price],
	dbo.cleaner(isnull(m.mfgShortName, '')) + ' ' + 
		CASE 
			WHEN CHARINDEX(' ', p.mpn) > 0 THEN dbo.[googleBaseMPNcleaner](LEFT(p.mpn, CHARINDEX(' ', p.mpn)-1))
			ELSE dbo.[googleBaseMPNcleaner](replace(p.mpn, '&', '-'))
		END + ' - ' + dbo.cleaner(p.Name) [title],
	dbo.cleaner(isnull(m.mfgName, '')) [brand],
	'http://' + p.mfgid + '.katomcdn.com/' + LOWER(p.image) + '_large.jpg' [image_link],
	dbo.[googleBaseMPNcleaner](REPLACE(cp.[mpn], '&', '-')) [mpn],
	'' [product_type],
	p.weight [weight],
	cast(month(dateadd(day, 14, getdate())) as varchar(2)) + '/' + 
	cast(day(dateadd(day, 14, getdate())) as varchar(2)) + '/' + 
	cast(year(dateadd(day, 14, getdate())) as varchar(4)) [expiration_date],
	CASE p.freeshipping
		WHEN '1' THEN 1 
		WHEN 'T' THEN 1 
		ELSE 0 
	END [shipping],
	dbo.cleaner(p.keywords) [keywords],
	p.code, p.upc, 
	dbo.cleaner(isnull(c.superCat, 'uncat')) [Adwords_Grouping], 
	dbo.cleaner(ISNULL(c.CATNAME, 'uncat')) [adwords_labels],
	dbo.cleaner(isnull(c2.google, 'Home & Garden > Kitchen & Dining')) [google_product_category],
	'true' [adwords_publish],
	'' [adwords_redirect]
FROM products p left join mfg m on p.mfgid = m.mfgid
				left join categories c on c.CODE = p.primaryCatCode AND c.primaryCat = 1
				left join cseCatMapping c2 on c.CODE = c2.catCode
				join competitorRelatedProduct cp on cp.code = p.code
WHERE p.active = 1 
	AND ISNULL(p.price, 0) > 0 
	AND p.feedMAP <> 3 
	AND ISNULL(p.MAP_Program, 'A') <> 'X'
	AND ISNULL(webactive, 0) = 1

-- CLEAN UP BAD CONTENTS
DELETE FROM googlebasefeed WHERE [description] IS NULL
DELETE FROM googleBaseFeed WHERE id IS NULL
DELETE FROM googleBaseFeed WHERE link LIKE '%599-%'
DELETE FROM googleBaseFeed WHERE CHARINDEX(' ', link) > 0

UPDATE googleBaseFeed 
SET id = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(id, '(', ''), ')', ''), '/', ''), '&', ''), ' ', '-')

UPDATE googleBaseFeed 
SET [description] = Replace([description], dbo.returnHTMLTagWrapper([description], '<iframe'), '')
WHERE CHARINDEX('<iframe', [description]) > 0

UPDATE googleBaseFeed 
SET [description] = dbo.cleaner(REPLACE([description], '%CODE%', [mpn]))
WHERE CHARINDEX('%CODE%', [description]) > 0

UPDATE googleBaseFeed 
SET [description] = RTRIM(REPLACE(REPLACE([description], '&AMP', '&amp;'), ';;', ';')) 

WHILE exists(SELECT * FROM googleBaseFeed WHERE CHARINDEX('  ', title) > 0)
   BEGIN
	UPDATE googleBaseFeed SET title = REPLACE(title, '  ', ' ') WHERE CHARINDEX('  ', title) > 0
   END

WHILE exists(SELECT * FROM googleBaseFeed WHERE CHARINDEX('  ', description) > 0)
   BEGIN
    UPDATE googleBaseFeed SET description = REPLACE(description, '  ', ' ') where CHARINDEX('  ', description) > 0
   END

--ADDING DUP PROD WITH DIFF VARIATION OF PROD ID

UPDATE googleBaseFeed
SET id = RTRIM(id) + ' ' + REPLACE(
		REPLACE(REPLACE(REPLACE(link, 'http://www.katom.com/', ''), '.html', ''), '-', ''),
		REPLACE(REPLACE(id, '-', ''), ' ', ''),
		'')
WHERE id IN 
	(
	SELECT id FROM googlebasefeed
	GROUP BY id
	HAVING COUNT(*) > 1
	)

DELETE FROM googleBaseFeed 
WHERE id IN 
	(
	SELECT id FROM googlebasefeed
	GROUP BY id
	HAVING COUNT(*) > 1
	)

--MAP POLICY
UPDATE g
SET g.price = 		
		CASE
			WHEN ISNULL(p.map, 0) = 1 THEN 
				CASE 
					WHEN ISNULL(p.feedMap, 0) = 1 THEN 
						CASE
							WHEN ISNULL(p.MAP_Program, 'K') = 'K' THEN cast(p.price as decimal(10, 2))
							ELSE CAST(p.map_price AS DECIMAL(10, 2))
						END
					ELSE cast(p.price as decimal(10, 2))
				END
			--WHEN ISNULL(P.clearance, 0) = 1 THEN cast(p.salesPrice as decimal(10, 2))
			ELSE cast(p.price as decimal(10, 2))
		END
FROM googleBaseFeed g join products p on g.Code = p.code

UPDATE googleBaseFeed
SET  description = REPLACE(REPLACE(replace(description, '21-30 day lead time.', ''), '14-21 day lead time.', ''), '(3 WEEK SHIP)', '')
where CHARINDEX('day lead time.', description) > 0

--POPULATING PRODUCT_TYPE
UPDATE g
SET g.product_type = ISNULL(dbo.breadcrumb(p.primaryCatCode), 'Restaurant Supplies')
FROM googleBaseFeed g JOIN products p ON g.Code = p.CODE

UPDATE googleBaseFeed
SET product_type = dbo.cleaner(product_type)

--REMOVING ALL PRODUCTS WITH BAD PRODUCT CODE
DELETE FROM  googleBaseFeed
WHERE dbo.isAlphaNumberic(REPLACE(REPLACE(REPLACE(link, 'http://www.katom.com/', ''), '.html', ''), '-', '')) = 0

--ADD TAGGING TO NATURAL AND REDIRECT URL
UPDATE googleBaseFeed
SET link = link + '?CID=GoogleBase2&amp;utm_source=googlebase2&amp;utm_medium=CSE&amp;utm_campaign=CSE&amp;zmam=29342707&amp;zmas=1&amp;zmac=1&amp;zmap=' + code,
	adwords_redirect = link + '?utm_source=googlebase2&amp;utm_medium=Adwords&amp;utm_campaign=CSE&amp;CID=googlebase2&amp;zmam=29342707&amp;zmas=1&amp;zmac=32&amp;zmap=' + code

--delete from googleBaseFeed where link like '%http://www.katom.com/598-T49.html%'
delete from googleBaseFeed where id = 'TRUE-Refrigeration-T-49'

UPDATE jobHistory SET endDT = GETDATE() WHERE jobID = @jobID
END
GO
