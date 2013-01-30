USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[order_cache_SP]    Script Date: 01/30/2013 16:26:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[order_cache_SP]
AS
	BEGIN	
		--INSERTING NEW WEB ORDER
	INSERT INTO orderInfo_cache(webOrderID, orderDT, customerName, shiptozip, shiptocity, shiptostate, webShippingCharge)
	SELECT customerpo, batchtimestamp, shiptocustomername, shiptozip, shiptocity, shiptostate, c.amount webEstimate
	FROM order_hdr o JOIN order_charges c ON c.order_id = o.customerpo
	WHERE DATEDIFF(HOUR, batchtimestamp, GETDATE()) < 2
		--YEAR(batchtimestamp) = 2011
		AND c.type = 'SHIPPING'
		AND customerpo NOT IN (SELECT webOrderID FROM orderInfo_cache)
	GROUP BY customerpo, batchtimestamp, shiptocustomername, shiptozip, shiptocity, shiptostate, c.amount

	--MATCHING OPEN ORDER TO WEB ORDER 
	UPDATE c
	SET c.NAVOrderID = ih.[No_],
		c.orderDT = ih.[Order Date],
		c.salesPerson = ih.[Salesperson Code],
		c.orderMedium = ih.[Order Medium],
		c.paymentType = ih.[Payment Terms Code],
		c.orderStatus = ih.[Order Status],
		c.printed = 
			CASE 
				WHEN ih.[No_ Printed] > 0 THEN 1
				ELSE 0
			END		
	FROM orderInfo_cache c JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Header] ih
				ON c.weborderID = ih.[Web Order No_] COLLATE Latin1_General_CS_AS
	WHERE c.NAVOrderID IS NULL		 

	--MATCHING INVOICED ORDER TO WEB ORDER	 
	UPDATE c
	SET c.invoiceID = ih.[No_],
		c.invoiceDT = ih.[Posting Date],
		c.NAVOrderID = 
			CASE 
				WHEN c.NAVOrderID IS NULL THEN ih.[Order No_] COLLATE Latin1_General_CS_AS
				ELSE c.NAVOrderID
			END,
		c.orderDT = 
			CASE 
				WHEN c.orderDT IS NULL THEN ih.[Shipment Date]	
				ELSE c.orderDT
			END,
		c.shipped = 
			CASE 
				WHEN LEN(ih.[Package Tracking No_]) > 0 THEN 1
				ELSE 0
			END
	FROM orderInfo_cache c JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] ih
				ON c.weborderID = ih.[Web Order No_] COLLATE Latin1_General_CS_AS
	WHERE c.invoiceID IS NULL

	----INSERTING MISSING ORDERS TO THE ORDER RPT
	--INSERT INTO orderInfo_cache(webOrderID, NAVOrderID, orderDT, InvoiceID, invoiceDT, customerName, ShipToZip, shipToCity, shipToState, 
	--	webShippingCharge, NAVShippingCharge, actuallShippingCharge, CostTT, salesTT, accessorialCharge, itemCount, salesPerson, 
	--	orderMedium, paymentType, printed, shipped, orderStatus, cancelled, ccDeclined)
	--SELECT [Web Order No_], [Order No_], [Shipment Date], [No_], [Posting Date], [Ship-to Name], 
	--	[Ship-to Post Code], [Ship-to City], [Ship-to County], NULL, NULL, NULL, NULL, NULL, NULL, NULL, 
	--	[Salesperson Code], [Catalog Version Code], [Payment Terms Code],		
	--		1, 
	--		CASE 
	--			WHEN LEN([Package Tracking No_]) > 0 THEN 1
	--			ELSE 0
	--		END, 'Invoiced', 0, 0
	--FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] 
	--WHERE [No_] NOT IN (SELECT ISNULL(invoiceID, '') COLLATE SQL_Latin1_General_CP1_CI_AS FROM orderInfo_cache)
	--	AND YEAR([Shipment Date]) = 2011

	--MARKING CANCEL ORDER
	UPDATE c
	SET c.cancelled = 1 
	FROM orderInfo_cache c JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] il
				ON c.invoiceID = il.[Document No_] COLLATE Latin1_General_CS_AS
	WHERE il.[Description] = 'Deleted Document' AND c.cancelled = 0

	--INSERTING OPEN ORDERS FOR ORDERS THAT WERE NOT IMPORTED FROM WEB
	INSERT INTO orderInfo_cache(webOrderID, NAVOrderID, orderDT, InvoiceID, invoiceDT, customerName, ShipToZip, shipToCity, shipToState, 
		webShippingCharge, NAVShippingCharge, actuallShippingCharge, CostTT, salesTT, accessorialCharge, itemCount, salesPerson, 
		orderMedium, paymentType, printed, shipped, orderStatus, cancelled, ccDeclined)
	SELECT [Web Order No_], [No_], [Order Date], NULL, NULL, [Ship-to Name], [Ship-to Post Code], [Ship-to City], [Ship-to County], 
			NULL, NULL, NULL, NULL, NULL, NULL, NULL, [Salesperson Code], [Order Medium], [Payment Terms Code],		
			CASE 
				WHEN [No_ Printed] > 0 THEN 1
				ELSE 0
			END, 0, [Order Status], 0, 0
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Header] 
	WHERE [Document Type] = 1
			AND [No_] NOT IN (SELECT ISNULL(NAVOrderID, '') COLLATE SQL_Latin1_General_CP1_CI_AS FROM orderInfo_cache)
		 

	--CALCULATING ACTUAL CHARGED IN NAV
	TRUNCATE TABLE prodTemp

	INSERT INTO prodTemp ([No_], UnitPrice)
	SELECT c.invoiceID, SUM([Quantity]*[Unit Price]) 
	FROM orderInfo_cache c JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] il
				ON c.invoiceID = il.[Document No_] COLLATE Latin1_General_CS_AS		
	WHERE [No_] IN ('100-FREIGHT', '420')
	GROUP BY c.invoiceID

	UPDATE c
	SET NAVShippingCharge = p.UnitPrice
	FROM orderInfo_cache c JOIN prodTemp p ON c.invoiceID = p.[No_] COLLATE Latin1_General_CS_AS	
	WHERE NAVShippingCharge IS NULL

	--CALCULATING PRODUCT MARGIN FROM OPEN ORDER
	TRUNCATE TABLE prodTemp

	INSERT INTO prodTemp ([No_], UnitPrice, UnitPrice2, UnitPrice1)
	SELECT c.NAVOrderID, SUM([Quantity]*[Unit Price]) salesTT, SUM([Quantity]*[realCost]) salesTT, COUNT(*) 
	FROM orderInfo_cache c JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Line] il
				ON c.NAVOrderID = il.[Document No_] COLLATE Latin1_General_CS_AS
						   JOIN products_Price_Worktable p ON p.code = il.[No_] COLLATE Latin1_General_CS_AS		
	WHERE LEFT([No_], 3) NOT IN ('100-FREIGHT', '420')
	GROUP BY c.NAVOrderID

	UPDATE c
	SET salesTT = p.UnitPrice, costTT = p.UnitPrice2, itemCount = CAST(UnitPrice1 AS SMALLINT)
	FROM orderInfo_cache c JOIN prodTemp p ON c.NAVOrderID = p.[No_] COLLATE Latin1_General_CS_AS	
	WHERE costTT IS NULL

	--CALCULATING PRODUCT MARGIN FROM INVOICED ORDER
	TRUNCATE TABLE prodTemp

	INSERT INTO prodTemp ([No_], UnitPrice, UnitPrice2, UnitPrice1)
	SELECT c.invoiceID, SUM(il.[Quantity]*il.[Unit Price]), SUM(il.[Quantity]*p.[Direct Unit Cost]), COUNT(*) 
	FROM orderInfo_cache c JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] il
				ON c.invoiceID = il.[Document No_] COLLATE Latin1_General_CS_AS
						   JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Purchase Price] p ON p.[Item No_] = il.[No_]		
	WHERE LEFT([No_], 3) <> '100'
		AND c.invoiceDT BETWEEN REPLACE([Starting Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, -1, GETDATE())) AND
				REPLACE([Ending Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, 1, GETDATE()))
	GROUP BY c.invoiceID

	UPDATE c
	SET salesTT = p.UnitPrice, costTT = p.UnitPrice2, itemCount = CAST(UnitPrice1 AS SMALLINT)
	FROM orderInfo_cache c JOIN prodTemp p ON c.invoiceID = p.[No_] COLLATE Latin1_General_CS_AS	
	WHERE CAST(ISNULL(costTT, -1) AS DECIMAL(10, 2)) <>  CAST(p.UnitPrice2 AS DECIMAL(10, 2))

	--TEMP FIX, DELETE WHEN LIVE 
	UPDATE c
	SET c.salesPerson = ih.[Salesperson Code],
		c.paymentType = ih.[Payment Terms Code],
		c.orderMedium = [Catalog Version Code],
		c.orderStatus = 'Invoiced',
		c.printed = 1
	FROM orderInfo_cache c JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] ih
				ON c.weborderID = ih.[Web Order No_] COLLATE Latin1_General_CS_AS
	WHERE c.salesPerson IS NULL
END
GO
