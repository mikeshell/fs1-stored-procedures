USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[product_Pricing_Update_SP]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[product_Pricing_Update_SP]
AS
BEGIN
DECLARE @jobID INT
INSERT INTO jobHistory(jobName, startdT) values('PROD_PRICING', GETDATE())
SET @jobID = @@identity

INSERT INTO products_Price_Worktable(code, uom, updatedt)
SELECT [No_], [Base Unit Of Measure], GETDATE()
FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item]
WHERE [No_] NOT IN (SELECT code COLLATE SQL_Latin1_General_CP1_CI_AS FROM products_Price_Worktable)

--UPDATE DISCOUNT GROUP
UPDATE pt
SET pt.discountGroup = i.[Item Disc_ Group]
FROM products_Price_Worktable pt JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] i
		ON pt.code = i.[No_] COLLATE Latin1_General_CS_AS
WHERE ISNULL(pt.discountGroup, '') <> i.[Item Disc_ Group] COLLATE Latin1_General_CS_AS

--UPDATE UOM
UPDATE pt
SET pt.UOM = i.[Base Unit Of Measure]
FROM products_Price_Worktable pt JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] i
		ON pt.code = i.[No_] COLLATE Latin1_General_CS_AS
WHERE ISNULL(pt.UOM, '') <> i.[Base Unit Of Measure] COLLATE Latin1_General_CS_AS

--UPDATING MANUAL PRICING FLAG
UPDATE pt
SET pt.manualPricingBB = p.[Manual Pricing]
FROM products_Price_Worktable pt JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Price] p 
		ON pt.code = p.[Item No_] COLLATE SQL_Latin1_General_CP1_CI_AS
	   AND pt.uom = p.[Unit of Measure Code] COLLATE SQL_Latin1_General_CP1_CI_AS
WHERE [Sales Type] = 1
	AND [Sales Code] = 'BB'
	AND pt.manualPricingBB <> p.[Manual Pricing]
	AND GETDATE() BETWEEN REPLACE([Starting Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, -1, GETDATE())) AND
			REPLACE([Ending Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, 1, GETDATE()))  

UPDATE pt
SET pt.manualPricingRealCost = p.[Manual Pricing]
FROM products_Price_Worktable pt JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Purchase Price] p 
		ON pt.code = p.[Item No_] COLLATE SQL_Latin1_General_CP1_CI_AS
	   AND pt.uom = p.[Unit of Measure Code] COLLATE SQL_Latin1_General_CP1_CI_AS
WHERE pt.manualPricingRealCost <> p.[Manual Pricing]
	AND GETDATE() BETWEEN REPLACE([Starting Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, -1, GETDATE())) AND
			REPLACE([Ending Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, 1, GETDATE())) 

UPDATE pt
SET pt.manualPricingCost = p.[Manual Pricing]
FROM products_Price_Worktable pt JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Price] p 
		ON pt.code = p.[Item No_] COLLATE SQL_Latin1_General_CP1_CI_AS
	   AND pt.uom = p.[Unit of Measure Code] COLLATE SQL_Latin1_General_CP1_CI_AS
WHERE [Sales Type] = 1
	AND [Sales Code] = 'STD_COST'
	AND pt.manualPricingCost <> p.[Manual Pricing]
	AND GETDATE() BETWEEN REPLACE([Starting Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, -1, GETDATE())) AND
			REPLACE([Ending Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, 1, GETDATE())) 

UPDATE pt
SET pt.manualPricingKAT = p.[Manual Pricing]
FROM products_Price_Worktable pt JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Price] p 
		ON pt.code = p.[Item No_] COLLATE SQL_Latin1_General_CP1_CI_AS
	   AND pt.uom = p.[Unit of Measure Code] COLLATE SQL_Latin1_General_CP1_CI_AS
WHERE [Sales Type] = 1
	AND [Sales Code] = 'KATOM'
	AND pt.manualPricingKAT <> p.[Manual Pricing]
	AND GETDATE() BETWEEN REPLACE([Starting Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, -1, GETDATE())) AND
			REPLACE([Ending Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, 1, GETDATE())) 

UPDATE pt
SET pt.manualPricingMAP = p.[Manual Pricing]
FROM products_Price_Worktable pt JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Price] p 
		ON pt.code = p.[Item No_] COLLATE SQL_Latin1_General_CP1_CI_AS
	   AND pt.uom = p.[Unit of Measure Code] COLLATE SQL_Latin1_General_CP1_CI_AS
WHERE [Sales Type] = 1
	AND [Sales Code] = 'MAP'
	AND pt.manualPricingMAP <> p.[Manual Pricing]
	AND GETDATE() BETWEEN REPLACE([Starting Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, -1, GETDATE())) AND
			REPLACE([Ending Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, 1, GETDATE())) 

UPDATE pt
SET pt.manualPricingGSA = p.[Manual Pricing]
FROM products_Price_Worktable pt JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Price] p 
		ON pt.code = p.[Item No_] COLLATE SQL_Latin1_General_CP1_CI_AS
	   AND pt.uom = p.[Unit of Measure Code] COLLATE SQL_Latin1_General_CP1_CI_AS
WHERE [Sales Type] = 1
	AND [Sales Code] = 'GSA'
	AND pt.manualPricingGSA <> p.[Manual Pricing]
	AND GETDATE() BETWEEN REPLACE([Starting Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, -1, GETDATE())) AND
			REPLACE([Ending Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, 1, GETDATE())) 

UPDATE pt
SET pt.manualPricingFA = p.[Manual Pricing]
FROM products_Price_Worktable pt JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Price] p 
		ON pt.code = p.[Item No_] COLLATE SQL_Latin1_General_CP1_CI_AS
	   AND pt.uom = p.[Unit of Measure Code] COLLATE SQL_Latin1_General_CP1_CI_AS
WHERE [Sales Type] = 1
	AND [Sales Code] = 'FROSTY'
	AND pt.manualPricingFA <> p.[Manual Pricing]
	AND GETDATE() BETWEEN REPLACE([Starting Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, -1, GETDATE())) AND
			REPLACE([Ending Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, 1, GETDATE())) 			
					
UPDATE pt
SET pt.manualPricingLP = p.[Manual Pricing]
FROM products_Price_Worktable pt JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Price] p 
		ON pt.code = p.[Item No_] COLLATE SQL_Latin1_General_CP1_CI_AS
	   AND pt.uom = p.[Unit of Measure Code] COLLATE SQL_Latin1_General_CP1_CI_AS
WHERE [Sales Type] = 2
	AND pt.manualPricingLP <> p.[Manual Pricing]
	AND GETDATE() BETWEEN REPLACE([Starting Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, -1, GETDATE())) AND
			REPLACE([Ending Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, 1, GETDATE())) 

--UPDATING COST
UPDATE pt
SET pt.cost = CAST(p.[Unit Price] AS DECIMAL(10, 4))
FROM products_Price_Worktable pt JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Price] p 
		ON pt.code = p.[Item No_] COLLATE SQL_Latin1_General_CP1_CI_AS
	   AND pt.uom = p.[Unit of Measure Code] COLLATE SQL_Latin1_General_CP1_CI_AS
WHERE [Sales Type] = 1
	AND [Sales Code] = 'STD_COST'
	AND CAST(p.[Unit Price] AS DECIMAL(10, 4)) <> CAST(ISNULL(pt.cost, 0) AS DECIMAL(10, 4))
	AND GETDATE() BETWEEN REPLACE([Starting Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, -1, GETDATE())) AND
			REPLACE([Ending Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, 1, GETDATE()))  
			
UPDATE pt
SET pt.realCost = CAST(p.[Direct Unit Cost] AS DECIMAL(10, 4))
FROM products_Price_Worktable pt JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Purchase Price] p 
		ON pt.code = p.[Item No_] COLLATE SQL_Latin1_General_CP1_CI_AS
	   AND pt.uom = p.[Unit of Measure Code] COLLATE SQL_Latin1_General_CP1_CI_AS
WHERE CAST(p.[Direct Unit Cost] AS DECIMAL(10, 4)) <> CAST(ISNULL(pt.realCost, -1) AS DECIMAL(10, 4))
	AND GETDATE() BETWEEN REPLACE([Starting Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, -1, GETDATE())) AND
			REPLACE([Ending Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, 1, GETDATE())) 
					
			
--UPDATING BB SELLING PRICE			
UPDATE pt
SET pt.bbprice = CAST(p.[Unit Price] AS DECIMAL(10, 2))
FROM products_Price_Worktable pt JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Price] p 
		ON pt.code = p.[Item No_] COLLATE SQL_Latin1_General_CP1_CI_AS
	   AND pt.uom = p.[Unit of Measure Code] COLLATE SQL_Latin1_General_CP1_CI_AS
WHERE [Sales Type] = 1
	AND [Sales Code] = 'BB'
	AND CAST(p.[Unit Price] AS DECIMAL(10, 2)) <> CAST(ISNULL(pt.bbprice, 0) AS DECIMAL(10, 2))
	AND GETDATE() BETWEEN REPLACE([Starting Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, -1, GETDATE())) AND
			REPLACE([Ending Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, 1, GETDATE()))

--UPDATING KATOM PRICE
UPDATE pt
SET pt.price = CAST(p.[Unit Price] AS DECIMAL(10, 2))
FROM products_Price_Worktable pt JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Price] p 
		ON pt.code = p.[Item No_] COLLATE SQL_Latin1_General_CP1_CI_AS
	   AND pt.uom = p.[Unit of Measure Code] COLLATE SQL_Latin1_General_CP1_CI_AS
WHERE [Sales Type] = 1
	AND [Sales Code] = 'KATOM'
	AND CAST(p.[Unit Price] AS DECIMAL(10, 2)) <> CAST(ISNULL(pt.price, 0) AS DECIMAL(10, 2))
	AND GETDATE() BETWEEN REPLACE([Starting Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, -1, GETDATE())) AND
			REPLACE([Ending Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, 1, GETDATE()))  

--UPDATING LIST PRICE			
UPDATE pt
SET pt.listprice = CAST(p.[Unit Price] AS DECIMAL(10, 2))
FROM products_Price_Worktable pt JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Price] p 
		ON pt.code = p.[Item No_] COLLATE SQL_Latin1_General_CP1_CI_AS
	   AND pt.uom = p.[Unit of Measure Code] COLLATE SQL_Latin1_General_CP1_CI_AS
WHERE [Sales Type] = 2
	AND LEN([Sales Code]) = 0
	AND CAST(p.[Unit Price] AS DECIMAL(10, 2)) <> CAST(ISNULL(pt.listprice, 0) AS DECIMAL(10, 2))
	AND GETDATE() BETWEEN REPLACE([Starting Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, -1, GETDATE())) AND
			REPLACE([Ending Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, 1, GETDATE()))  
			
--UPDATING MAP PRICE	
UPDATE pt
SET pt.mapPrice = CAST(p.[Unit Price] AS DECIMAL(10, 2))
FROM products_Price_Worktable pt JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Price] p 
		ON pt.code = p.[Item No_] COLLATE SQL_Latin1_General_CP1_CI_AS
	   AND pt.uom = p.[Unit of Measure Code] COLLATE SQL_Latin1_General_CP1_CI_AS
WHERE [Sales Type] = 1
	AND [Sales Code] = 'MAP'
	AND CAST(p.[Unit Price] AS DECIMAL(10, 2)) <> CAST(ISNULL(pt.mapPrice, 0) AS DECIMAL(10, 2))
	AND GETDATE() BETWEEN REPLACE([Starting Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, -1, GETDATE())) AND
			REPLACE([Ending Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, 1, GETDATE()))  
			
--UPDATING GSA PRICE	
UPDATE pt
SET pt.gsaPrice = CAST(p.[Unit Price] AS DECIMAL(10, 2))
FROM products_Price_Worktable pt JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Price] p 
		ON pt.code = p.[Item No_] COLLATE SQL_Latin1_General_CP1_CI_AS
	   AND pt.uom = p.[Unit of Measure Code] COLLATE SQL_Latin1_General_CP1_CI_AS
WHERE [Sales Type] = 1
	AND [Sales Code] = 'GSA'
	AND CAST(p.[Unit Price] AS DECIMAL(10, 2)) <> CAST(ISNULL(pt.mapPrice, 0) AS DECIMAL(10, 2))
	AND GETDATE() BETWEEN REPLACE([Starting Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, -1, GETDATE())) AND
			REPLACE([Ending Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, 1, GETDATE()))  

	
--UPDATING FROSTY ACRES PRICE	
UPDATE pt
SET pt.faPrice = CAST(p.[Unit Price] AS DECIMAL(10, 2))
FROM products_Price_Worktable pt JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Price] p 
		ON pt.code = p.[Item No_] COLLATE SQL_Latin1_General_CP1_CI_AS
	   AND pt.uom = p.[Unit of Measure Code] COLLATE SQL_Latin1_General_CP1_CI_AS
WHERE [Sales Type] = 1
	AND [Sales Code] = 'FROSTY'
	AND CAST(p.[Unit Price] AS DECIMAL(10, 2)) <> CAST(ISNULL(pt.mapPrice, 0) AS DECIMAL(10, 2))
	AND GETDATE() BETWEEN REPLACE([Starting Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, -1, GETDATE())) AND
			REPLACE([Ending Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, 1, GETDATE()))  



--UPDATE PRICING ON THE PRODUCTS TABLE
UPDATE p
SET p.cost = pw.cost,
	p.updateDT = GETDATE()	
FROM products_Price_Worktable pw JOIN products p on p.CODE = pw.code
WHERE CAST(ISNULL(p.cost, 0) AS DECIMAL(10, 2)) <> CAST(ISNULL(pw.cost, 0) AS DECIMAL(10, 2))

UPDATE p
SET p.price = pw.price,
	p.updateDT = GETDATE()	
FROM products_Price_Worktable pw JOIN products p on p.CODE = pw.code
WHERE CAST(ISNULL(p.price, 0) AS DECIMAL(10, 2)) <> CAST(ISNULL(pw.price, 0) AS DECIMAL(10, 2))

UPDATE p
SET p.BBPrice = pw.bbprice,
	p.updateDT = GETDATE()	
FROM products_Price_Worktable pw JOIN products p on p.CODE = pw.code
WHERE CAST(ISNULL(p.BBPrice, 0) AS DECIMAL(10, 2)) <> CAST(ISNULL(pw.bbprice, 0) AS DECIMAL(10, 2))

UPDATE p
SET p.gsaPrice = pw.gsaPrice,
	p.updateDT = GETDATE()	
FROM products_Price_Worktable pw JOIN products p on p.CODE = pw.code
WHERE CAST(ISNULL(p.gsaPrice, 0) AS DECIMAL(10, 2)) <> CAST(ISNULL(pw.gsaPrice, 0) AS DECIMAL(10, 2))

UPDATE p
SET p.fa_Price= pw.faPrice,
	p.updateDT = GETDATE()	
FROM products_Price_Worktable pw JOIN products p on p.CODE = pw.code
WHERE CAST(ISNULL(p.fa_Price, 0) AS DECIMAL(10, 2)) <> CAST(ISNULL(pw.faPrice, 0) AS DECIMAL(10, 2))

UPDATE p
SET p.MAP_Price = pw.mapPrice,
	p.updateDT = GETDATE()	
FROM products_Price_Worktable pw JOIN products p on p.CODE = pw.code
WHERE CAST(ISNULL(p.MAP_Price, 0) AS DECIMAL(10, 2)) <> CAST(ISNULL(pw.mapPrice, 0) AS DECIMAL(10, 2))

UPDATE p
SET p.listPrice = pw.listprice,
	p.updateDT = GETDATE()	
FROM products_Price_Worktable pw JOIN products p on p.CODE = pw.code
WHERE CAST(ISNULL(p.listPrice, 0) AS DECIMAL(10, 2)) <> CAST(ISNULL(pw.listPrice, 0) AS DECIMAL(10, 2))

UPDATE p
SET p.true_Cost = CAST(pw.realCost AS DECIMAL(10, 2)),
	p.updateDT = GETDATE()
FROM products_Price_Worktable pw JOIN products p on p.CODE = pw.code
WHERE CAST(ISNULL(p.true_Cost, 0) AS DECIMAL(10, 2)) <> CAST(ISNULL(pw.realCost, 0) AS DECIMAL(10, 2))

--UPDATING DISCOUNT GROUP
UPDATE p
SET p.icdiscountGrp = pp.discountGroup
FROM products p join  products_Price_Worktable pp on p.CODE = pp.code
WHERE ISNULL(icdiscountGrp, '') <> discountGroup

--SET ALL SALES PRODUCTS
UPDATE p
SET p.clearance = 1
FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Price] sp 
	LEFT JOIN products p on sp.[Item No_] = p.CODE COLLATE SQL_Latin1_General_CP1_CI_AS
WHERE [Sales Type] = 1
	AND [Manual Price Reason Code] = 'SALE_KATOM' 
	AND [Sales Code] = 'KATOM'
	AND GETDATE() BETWEEN REPLACE([Starting Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, -1, GETDATE())) AND
			REPLACE([Ending Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, 1, GETDATE()))
			
UPDATE jobHistory SET endDT = GETDATE() WHERE jobID = @jobID
END
GO
