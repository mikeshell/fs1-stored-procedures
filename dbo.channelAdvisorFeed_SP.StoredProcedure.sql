USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[channelAdvisorFeed_SP]    Script Date: 02/01/2013 11:09:50 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[channelAdvisorFeed_SP]
AS
BEGIN
	DECLARE @jobID INT
	INSERT INTO jobHistory(jobName, startdT) values('CA_FEED', GETDATE())
	SET @jobID = @@identity

	TRUNCATE TABLE CAFeed

	INSERT INTO CAFeed
	SELECT 'http://www.katom.com/' + ltrim(rtrim(p.CODE)) + '.html' [ActionURL], ISNULL(p.mfgName, '') + ' ' + p.mpn + ' - ' + dbo.cleaner(p.[Name]),
		cast(dbo.cleaner(p.[prodDesc]) as varchar(max)) + ' ( ' + isnull(p.mfgName, '') + ' - ' + p.mpn  + ' )',
		'http://' + p.mfgid + '.katomcdn.com/' + LOWER(p.image) + '_large.jpg', 
		cast(p.listprice as decimal(10, 2)),
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
			WHEN ISNULL(P.clearance, 0) = 1 THEN cast(p.salesPrice as decimal(10, 2))
			ELSE cast(p.price as decimal(10, 2))
		END, ltrim(rtrim(p.code)), '1', ISNULL(p.mfgName, ''), NULL [MerchantCategory], p.mfgName, p.mpn,
		CASE isnull(p.freeShipping, 0)
			WHEN 1 THEN 'Free Shipping! Call 1-800-541-8683 for great customer service!'
			ELSE 'Call 1-800-541-8683 for great customer service!'
		END,
		CASE 
			WHEN  p.freightOnly = 0 AND (p.prodL*p.prodH*p.prodW)/166 > p.[Weight] THEN CAST((prodL*prodH*prodW)/166 AS DECIMAL(10, 2))+5
			ELSE cast(p.weight as decimal(10, 2))
		END, 0, p.keywords, p.prodH, p.prodL, p.prodW, 'http://www.katom.com/pdfspecs/' + p.code + '.pdf', NULL [custom5],
		cast(p.cost as decimal(10, 2)), p.freeshipping, p.[Name], NULL [custom8], NULL [custom9], NULL [custom10],
		CASE
			WHEN CHARINDEX('.jpg', LOWER(p.image)) > 0 THEN 'http://www.katom.com/largeproducts/' + p.mfgid + '/' + LOWER(p.image)
			ELSE 'http://www.katom.com/largeproducts/' + p.mfgid + '/' + LOWER(p.image) + '.jpg'
		END, NULL [custom12], prodID, GETDATE(), 0, 1, 1, 
		CASE 
			WHEN p.mfgID = '598' THEN CAST((p.price-.8*p.true_cost)*p.sales30 AS DECIMAL(10, 2))
			ELSE CAST((p.price-p.true_cost)*p.sales30 AS DECIMAL(10, 2))
		END, NULL [profitData], 1, 1, 1, 1, 1, NULL, NULL, NULL, NULL, NULL
	FROM products p
	WHERE p.active = 1 AND ISNULL(p.price, 0) > 0  AND p.isWeb = 1
	ORDER BY p.code 

	--POPULATING THE MERCHANTCATEGORY WITH THE REFINABLE CATEGORY IF IT EXISTS ELSE WITH THE PRIMARY CAT INFO
	UPDATE c
	SET c.MerchantCategory = 
			CASE 
				WHEN LEN(ISNULL(i.[Refinable Value], '')) > 0 THEN i.[Refinable Value]
				ELSE c2.catName
			END,
		c.[custom12] = i.[Refinable Value]
	FROM CAFeed c join products p on c.Model = p.CODE
				left join categories c2 on p.primaryCatCode = c2.code and c2.primarycat = 1
				LEFT JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item Refinables] i on i.[Item No_] = c.Model COLLATE SQL_Latin1_General_CP1_CI_AS AND i.[Refinable No_] = 'CATEGORY'
			
	--MARK AN ITEM TO BE RESIDENTIAL BASED ON THE REFINABLE VALUE OF NAV (NOT USE ANYMORE - DATA IS JUNK)
	UPDATE c
	SET c.[custom5] = i.[Refinable Value]
	FROM CAFeed c JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item Refinables] i on i.[Item No_] = c.Model COLLATE SQL_Latin1_General_CP1_CI_AS AND i.[Refinable No_] = 'RESIDENTIAL'

	--POPULATE CUSTOM10 WITH THE TYPE VALUE FROM NAVISION REFINABLE
	UPDATE c
	SET c.[custom10] = i.[Refinable Value]
	FROM CAFeed c JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item Refinables] i on i.[Item No_] = c.Model COLLATE SQL_Latin1_General_CP1_CI_AS AND i.[Refinable No_] = 'TYPE'

	UPDATE c
	SET c.[custom10] = i.[Refinable Value]
	FROM CAFeed c JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item Refinables] i on i.[Item No_] = c.Model COLLATE SQL_Latin1_General_CP1_CI_AS AND i.[Refinable No_] = 'COLOR'
	WHERE c.[custom10] IS NULL
		
	--SETTING NEW LISTPRICE IF LISTPRICE < PRICE
	UPDATE cafeed
	SET [RegularPrice] = CAST(2*[CurrentPrice] AS DECIMAL(10, 2))
	WHERE CAST([RegularPrice] AS DECIMAL(10, 2)) < CAST([CurrentPrice] AS DECIMAL(10, 2))

	--TAKING CARE OF DUPLICATE MPN
	DECLARE @MPN varchar(100), @mfgID varchar(100), @tmp varchar(100), @code varchar(100)
	
	DECLARE mCursor CURSOR FOR 
	select Left(Model, 3), ManufacturerModel
	from cafeed
	group by Left(Model, 3), ManufacturerModel 
	having count(*) > 1

	OPEN mCursor
	FETCH NEXT FROM mCursor INTO @mfgID, @MPN

	WHILE @@FETCH_STATUS = 0
	   BEGIN
			DECLARE pCursor CURSOR FOR 
			select Model
			from cafeed
			Where Left(Model, 3) = @mfgID and ManufacturerModel = @MPN

			OPEN pCursor
			FETCH NEXT FROM pCursor INTO @code

			WHILE @@FETCH_STATUS = 0
			   BEGIN
				SET @tmp = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@MPN, ')', ''), '(', ''), '-', ''), ' ', ''), '/', ''), '.', '')
				
				UPDATE cafeed
				SET ManufacturerModel = @MPN + ' ' + REPLACE(RIGHT(@CODE, LEN(@CODE)-4), @TMP, '')
				WHERE MODEL = @CODE
	
				SET @tmp = ''
				FETCH NEXT FROM pCursor INTO @code
			   END

			CLOSE pCursor
			DEALLOCATE pCursor	

		FETCH NEXT FROM mCursor INTO @mfgID, @MPN
	   END

	CLOSE mCursor
	DEALLOCATE mCursor
	
	UPDATE cafeed
	SET [OfferDescription] = REPLACE([OfferDescription], '°', '')
	WHERE CHARINDEX('°', [OfferDescription]) > 0

	UPDATE cafeed
	SET [OfferDescription] = REPLACE([OfferDescription], 'Ö', '')
	WHERE CHARINDEX('Ö', [OfferDescription]) > 0
	
	UPDATE cafeed
	SET [OfferDescription] = REPLACE([OfferDescription], '«', '')	
	WHERE CHARINDEX('«', [OfferDescription]) > 0
	
	UPDATE cafeed
	SET [OfferDescription] = REPLACE([OfferDescription], '¦', '')	
	WHERE CHARINDEX('¦', [OfferDescription]) > 0
	
	UPDATE cafeed
	SET [OfferDescription] = REPLACE([OfferDescription], 'û', '')	
	WHERE CHARINDEX('û', [OfferDescription]) > 0
	
	UPDATE cafeed
	SET [OfferDescription] = REPLACE([OfferDescription], '½', '')	
	WHERE CHARINDEX('½', [OfferDescription]) > 0
	
	UPDATE cafeed
	SET [OfferDescription] = REPLACE([OfferDescription], '¼', '')	
	WHERE CHARINDEX('¼', [OfferDescription]) > 0
	
	UPDATE cafeed
	SET [OfferDescription] = REPLACE([OfferDescription], '  ', '')	
	WHERE CHARINDEX('  ', [OfferDescription]) > 0
	
	UPDATE cafeed
	SET [custom7] = REPLACE([custom7], char(10), '')	
	WHERE CHARINDEX(char(10), [custom7]) > 0
		
	UPDATE cafeed
	SET [custom7] = REPLACE([custom7], char(13), '')	
	WHERE CHARINDEX(char(10), [custom3]) > 0	
	
	delete from cafeed where LEN(ISNULL(offername, '')) = 0
	
	delete from CAFeed where Model LIKE '%599-%'
	
	--REMOVE ALL ITEMS THAT ARE BACKORDERS
	UPDATE CAFeed
	SET listproduct = 0
	WHERE Model IN (select [No_] COLLATE Latin1_General_CS_AS from backOrder_item where DATEDIFF(day, [OldestOrderdt], getdate()) > 15 and price < 250)

	--USE ANALYTICS TO MANAGE
	--UPDATE CAFeed
	--SET apex = 1
	--WHERE Model in 
	--	(			
	--	SELECT p.code
	--	FROM categories c JOIN products p ON c.CODE = p.primaryCatCode
	--	WHERE (c.superCat IN ('Residential', 'Janitorial')
	--			AND c.ACTIVE = 1 and p.active = 1
	--			AND price < 500)
	--			OR price < 250				
	--	 )
		
	--MARK AN ITEMT O BE LISTED AT PRICE + .01 FOR AMAZON ONLY
	UPDATE CAFeed
	SET amazon = 0 
	where model in (select code from products where active = 1 and isnull(feedMAP, 0) = 1 and isnull(MAP_Program, 'K') = 'E' and MAP = 1)
	
	UPDATE c
	SET c.map_price = CAST(p.map_price AS DECIMAL(10, 2))
	FROM products p JOIN cafeed c ON p.CODE = c.Model
	WHERE active = 1 
		AND ISNULL(MAP, 0) = 1
		AND ISNULL(map_program, '') = 'A'
		AND ISNULL(feedMAP, 0) = 0

	--REMOVING PRODUCTS THAT WE HAVENT SOLD IN THE LAST 24 MONTH
	UPDATE CAFeed
	SET listproduct = 0
	WHERE Model IN (SELECT CODE 
					FROM products 
					WHERE sales_last_24MO = 0 
						AND DATEDIFF(MONTH, addedDT, GETDATE()) > 11
					)
	--REMOVING PRODUCTS FROM CSE IF WE ARE SPENDING MORE THAN WE ARE MAKING MONEY
	EXEC [dbo].[cseClick_Stat]
	
	--DISABLING ANY PRODUCTS UNDER $50	
	UPDATE CAFeed
	SET shopzilla = 0, nextag = 0, amazon = 0, pricegrabber = 0, shopping = 0, become = 0
	WHERE CurrentPrice < 25

	--DISABLING PRODUCTS FROM PAID CSE CHANNELS
	UPDATE c
	SET c.nextag = 0
	FROM CAFeed c JOIN cseclick cc ON c.Model = cc.sku
	WHERE cc.Channel_Name LIKE '%NexTag%'

	--DISABLING PRODUCTS FROM AMAZON
	UPDATE c
	SET c.amazon = 0
	FROM CAFeed c JOIN cseclick cc ON c.Model = cc.sku
	WHERE cc.Channel_Name LIKE '%Amazon%'

	--DISABLING PRODUCTS FROM SHOPZILLA
	UPDATE c
	SET c.shopzilla = 0
	FROM CAFeed c JOIN cseclick cc ON c.Model = cc.sku
	WHERE cc.Channel_Name LIKE '%Shopzilla%'
	
	/**			
	UPDATE CAFeed
	SET shopzilla = 0
	WHERE LEFT(model, 3) IN ('017', '095', '284', '261', '075', '158', '166', '229', '002', '290', '225', '037', '383')
	**/	
	--DISABLING PRODUCTS FROM PRICEGRABBER
	UPDATE c
	SET c.pricegrabber = 0
	FROM CAFeed c JOIN cseclick cc ON c.Model = cc.sku
	WHERE cc.Channel_Name LIKE '%PriceGrabber%'
		
	--DISABLING PRODUCTS FROM SHOPPING.COM
	UPDATE c
	SET c.shopping = 0
	FROM CAFeed c JOIN cseclick cc ON c.Model = cc.sku
	WHERE Channel_Name LIKE '%Shopping%'
		
	--DISABLING PRODUCTS FROM BECOME.COM
	UPDATE c
	SET c.become = 0
	FROM CAFeed c JOIN cseclick cc ON c.Model = cc.sku
	WHERE cc.Channel_Name LIKE '%Become%'
	
	--REMOVE ALL PRODUCTS WHERE FEED MAP = 3
	DELETE FROM CAFeed WHERE model IN
	(SELECT code FROM products WHERE active = 1 AND feedMAP = 3)
	
	--REMOVE ALL PRODUCTS WHERE MAP PROGRAM = X
	DELETE FROM CAFeed WHERE model IN
	(SELECT code FROM products WHERE active = 1 AND MAP_Program = 'X')
	
	-- GOOGLE CATEGORIES
	UPDATE c
	SET c.googleCat = cse.google,
		c.amazonCat = cse.amazon	
	FROM CAFeed c JOIN products p on c.Model = p.CODE			
				JOIN cseCatMapping cse ON cse.catCode = p.primaryCatCode	
	
	UPDATE CAFeed 
	SET googleCat = 'Home & Garden > Kitchen & Dining'
	WHERE googleCat IS NULL

	UPDATE CAFeed 
	SET amazonCat = 289814
	WHERE amazonCat IS NULL
	
	UPDATE g
	SET g.product_type = ISNULL(dbo.breadcrumb(p.primaryCatCode), 'Restaurant Supplies'),
		g.adwords_label = ISNULL(p.primaryCatCode, 'uncat')
	FROM CAFeed g JOIN products p ON g.model = p.CODE
	
	UPDATE c
	SET c.adwords_label = ISNULL(c2.catname, 'uncat'), 
		c.adwords_grouping = ISNULL(c2.superCat, 'uncat')
	FROM cafeed c JOIN categories c2 on c.adwords_label = c2.CODE AND c2.primaryCat = 1 
				
	UPDATE CAFeed
	SET product_type = dbo.cleaner(product_type),
		adwords_label = dbo.cleaner(adwords_label),
		adwords_grouping = dbo.cleaner(adwords_grouping)
		
	UPDATE CAFeed 
	SET adwords_grouping = 'uncat'
	WHERE adwords_grouping IS NULL
	
	UPDATE CAFeed SET MerchantCategory = 'Kitchen' WHERE MerchantCategory IS NULL
	
	--REMOVING ALL PRODUCTS FROM MAJOR CSE WITH 14+ DAYS LEAD TIME
	UPDATE c
	SET c.amazon = 0,
		c.shopping = 0,
		c.shopzilla = 0,
		c.nextag = 0,
		c.pricegrabber = 0,
		c.become = 0
	FROM products p JOIN CAFeed c ON p.CODE = c.Model
	WHERE active = 1 AND isWeb = 1
	AND price < 100
	AND avgLeadTime > 13
	AND qtyOnHand < 1
	
	UPDATE c
	SET c.amazon = 0,
		c.shopping = 0,
		c.shopzilla = 0,
		c.nextag = 0,
		c.pricegrabber = 0,
		c.become = 0
	FROM products p JOIN CAFeed c ON p.CODE = c.Model
	WHERE active = 1 AND isWeb = 1
	AND price < 100
	AND avgLeadTime is null 
	AND qtyOnHand < 1
	and leadTime_mfg like '%14+%'
	
	UPDATE jobHistory SET endDT = GETDATE() WHERE jobID = @jobID
END
GO
