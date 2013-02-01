USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[product_leadTime_SP]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[product_leadTime_SP]
AS
BEGIN
	-- SETTING LEAD TIME FOR EACH PRODUCTS 
	TRUNCATE TABLE prodTemp

	INSERT INTO prodTemp([No_], [UnitPrice])
	SELECT il.[No_],
			CASE 
				WHEN ih.[Shipment Method Code] = 'PICK UP' THEN DATEDIFF(DAY, ih.[Order Date], ih.[Posting Date])
				WHEN LEN(ISNULL(u.[Pickup Date], '')) > 0 THEN DATEDIFF(DAY, ih.[Order Date], u.[Pickup Date])
				WHEN t.[Ship Date] IS NOT NULL THEN DATEDIFF(DAY, ih.[Order Date], t.[Ship Date])
				ELSE DATEDIFF(DAY, ih.[Order Date], ih.[Posting Date])
			END leadTime
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] ih
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] il ON  ih.[No_] = il.[Document No_]
		LEFT JOIN upsWorldShip u ON u.[Tracking Number] = ih.[Package Tracking No_] COLLATE Latin1_General_CS_AS
		LEFT JOIN trackingInfo t ON t.[Tracking Number] = ih.[Package Tracking No_] COLLATE Latin1_General_CS_AS
	WHERE DATEDIFF(DAY, ih.[Order Date], GETDATE()) BETWEEN 3 AND 91
		AND DATEDIFF(DAY, ih.[Posting Date], GETDATE()) > 0
		AND DATEDIFF(DAY, ih.[Order Date], ih.[Posting Date]) > -1
		AND [Source Code] <> 'DELETE'
		AND LEN(il.[No_]) > 3
		AND il.[Quantity] > 0
		AND LEFT(il.[No_], 3) <> '100'
	ORDER BY 1

	INSERT INTO prodTemp([No_], [UnitPrice2])
	SELECT [No_], AVG([UnitPrice])
	FROM prodTemp
	GROUP BY [No_]

	DELETE FROM prodTemp WHERE [UnitPrice2] IS NULL
	
	--RESET PROD LEADTIME TO NULL IF THE LAST UPDATE IS OVER 30 DAYS
	UPDATE products
	SET leadTime = NULL
	WHERE active = 1 
		AND DATEDIFF(DAY, leadTimeLastUpdate, GETDATE()) > 30
		AND leadTime IS NOT NULL

	UPDATE p
	SET p.avgLeadTime = [UnitPrice2],	
		p.leadTime = 
			CASE
			 WHEN qtyOnHand > 0 THEN 'Typically ships within <span class="bold"> 1 - 2 business days</span> '
			 WHEN [unitPrice2] < 3 THEN 'Typically ships within <span class="bold"> 1 - 2 business days</span> '
			 WHEN [unitPrice2] BETWEEN 3 AND 6.99 THEN 'Typically ships within <span class="bold"> 3 - 6 business days</span> ' 
			 WHEN [unitPrice2] BETWEEN 7 AND 13.99 THEN 'Typically ships within <span class="bold"> 7 - 13 business days</span> ' 
			 ELSE 'Extended Lead Time (<span class="bold">14+ days</span>)'
			END,
		p.leadTimeLastUpdate = GETDATE()	
	FROM products p join prodTemp pt on p.CODE = pt.[No_]

	-- PULLING FULFILLMENT TIME FOR SHIP DIRECT
	DELETE FROM purchase_order_history WHERE DATEDIFF(DAY, orderDT, GETDATE()) > 91
	DELETE FROM purchase_order_history WHERE confirmed = 0

	INSERT INTO purchase_order_history
	SELECT p.[No_], p.[Buy-from Vendor No_], i.[Order Date], 
			CASE
				WHEN CHARINDEX('|', p.[Vendor Authorization No_]) > 0 THEN 
					CASE
						WHEN ISDATE(RIGHT(p.[Vendor Authorization No_], LEN(p.[Vendor Authorization No_])-CHARINDEX('|', p.[Vendor Authorization No_]))) = 1
								THEN RIGHT(p.[Vendor Authorization No_], LEN(p.[Vendor Authorization No_])-CHARINDEX('|', p.[Vendor Authorization No_]) )
						ELSE p.[Order Date]
					END
				ELSE p.[Order Date]
			END, 
			CASE
				WHEN CHARINDEX('|', p.[Vendor Authorization No_]) > 0 THEN 
					CASE
						WHEN ISDATE(RIGHT(p.[Vendor Authorization No_], LEN(p.[Vendor Authorization No_])-CHARINDEX('|', p.[Vendor Authorization No_]))) = 1 THEN 1 
						ELSE 0
					END
				ELSE 0
			END
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Purchase Header] p
			JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] i ON p.[sales Order No_] = i.[Order No_]
	WHERE p.[Document Type] = 1 AND [Vendor Authorization No_] <> 'CANCEL'
		AND p.[Ship-to Name] NOT IN ('WAREHOUSE FACILITY')
		AND DATEDIFF(DAY, i.[Order Date], GETDATE()) BETWEEN 3 AND 91
		AND DATEDIFF(DAY, i.[Posting Date], GETDATE()) > 0
		AND p.[No_] NOT IN (SELECT poNum FROM purchase_order_history)
		
	INSERT INTO purchase_order_history
	SELECT p.[Order No_], p.[Buy-from Vendor No_], i.[Order Date], i.[Posting Date], 0
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Purch_ Inv_ Header] p
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] i ON p.[sales Order No_] = i.[Order No_] 
	WHERE p.[Source Code] NOT IN ('DELETE') AND p.[Ship-to Name] NOT IN ('WAREHOUSE FACILITY')
		AND DATEDIFF(DAY, i.[Order Date], GETDATE()) BETWEEN 3 AND 91
		AND DATEDIFF(DAY, i.[Posting Date], GETDATE()) > 0
		AND p.[Order No_] NOT IN (SELECT poNum FROM purchase_order_history)

	DELETE FROM purchase_order_history WHERE orderDT = '1900-01-01 00:00:00.000' OR shippedDT = '1900-01-01 00:00:00.000'
	DELETE FROM purchase_order_history WHERE DATEDIFF(DAY, orderDT, shippedDT) > 180 -- ASSUMING BAD DATA
	DELETE FROM purchase_order_history WHERE DATEDIFF(DAY, orderDT, shippedDT) < 0 -- ASSUMING BAD DATA

	TRUNCATE TABLE prodTemp

	INSERT INTO prodTemp([No_], [UnitPrice])
	SELECT mfgID, 
		CASE 
			WHEN STDEV(DATEDIFF(DAY, orderDT, shippedDT)) IS NULL THEN AVG(DATEDIFF(DAY, orderDT, shippedDT))
			ELSE STDEV(DATEDIFF(DAY, orderDT, shippedDT))
		END
	FROM purchase_order_history
	GROUP BY mfgid

	UPDATE m
	SET m.avgLeadTime = [unitPrice],
		m.leadTime = 
			CASE
			 WHEN [unitPrice] < 3 THEN 'Typically ships within <span class="bold"> 1 - 2 business days</span> '
			 WHEN [unitPrice] BETWEEN 3 AND 6.99 THEN 'Typically ships within <span class="bold"> 3 - 6 business days</span> '
			 WHEN [unitPrice] BETWEEN 7 AND 13.99 THEN 'Typically ships within <span class="bold"> 7 - 13 business days</span> '
			 ELSE 'Extended Lead Time (<span class="bold">14+ days</span>)'
			END,
		m.leadTimeLastUpdate = GETDATE()	
	FROM mfg m join prodTemp pt on m.mfgID = pt.[No_]
	
	
	--CALCULATING PURCHASING CYCLE
	DECLARE @vendorID VARCHAR(3), @date DATETIME, @vendID VARCHAR(3), @previousDT DATETIME

	TRUNCATE TABLE prodTemp
		
	DECLARE catCursor CURSOR FOR 
	SELECT [Buy-from Vendor No_]
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Purch_ Inv_ Header] p
	WHERE p.[Source Code] NOT IN ('DELETE') 
		AND p.[Ship-to Name] IN ('WAREHOUSE FACILITY')
		AND LEN([Buy-from Vendor No_]) = 3
		AND DATEDIFF(DAY, [Posting Date], GETDATE()) < 91
	GROUP BY [Buy-from Vendor No_]
	HAVING COUNT(*) > 2

	OPEN catCursor
	FETCH NEXT FROM catCursor INTO @vendorID

	WHILE @@FETCH_STATUS = 0
	   BEGIN
		SET @previousDT = '1/1/2000'
		
		DECLARE cCursor CURSOR FOR 
		SELECT DISTINCT [Buy-from Vendor No_], [Order Date]
		FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Purch_ Inv_ Header] p
		WHERE p.[Source Code] NOT IN ('DELETE') 
			AND p.[Ship-to Name] IN ('WAREHOUSE FACILITY')
			AND [Buy-from Vendor No_] = @vendorID
			AND DATEDIFF(DAY, [Order Date], GETDATE()) < 91
		ORDER BY [Order Date] ASC
		
		OPEN cCursor
		FETCH NEXT FROM cCursor INTO @vendID, @date
		
		WHILE @@FETCH_STATUS = 0
		   BEGIN
			IF @previousDT <> '1/1/2000'
			   BEGIN
				INSERT INTO prodTemp([No_], UnitPrice) VALUES(@vendID, DATEDIFF(DAY, @previousDT, @date))
			   END
			
			SET @previousDT = @date	
	   
			FETCH NEXT FROM cCursor INTO @vendID, @date
		   END

		CLOSE cCursor
		DEALLOCATE cCursor	
	   
		FETCH NEXT FROM catCursor INTO @vendorID
	   END

	CLOSE catCursor
	DEALLOCATE catCursor

	INSERT INTO prodTemp([No_], UnitPrice2)
	SELECT [No_], AVG([UnitPrice])
	FROM prodTemp p JOIN mfg m on p.[No_] = m.mfgID
	GROUP BY [No_], shipDirect, leadTime

	UPDATE m
	SET m.avgPurchasingCycle = UnitPrice2,
		m.purchasingCycle = 
			CASE
			 WHEN p.UnitPrice2 < 3 THEN 'Typically ships within <span class="bold"> 1 - 2 business days</span> '
			 WHEN p.UnitPrice2 BETWEEN 3 AND 6.99 THEN 'Typically ships within <span class="bold"> 3 - 6 busines days</span> '
			 WHEN p.UnitPrice2 BETWEEN 7 AND 13.99 THEN 'Typically ships within <span class="bold"> 7 - 13 business days</span> '
			 ELSE 'Extended Lead Time (<span class="bold">14+ days</span>)'
			END,
		m.purchasingCycleDT = GETDATE()
	FROM prodTemp p JOIN mfg m on p.[No_] = m.mfgID
	WHERE p.UnitPrice2 IS NOT NULL
	
	--ADDING MFG LEADTIME INFO INTO THE PRODUCTS TABLE
	UPDATE p
	SET p.leadTime_mfg =
		CASE 
			WHEN p.specialOrder = 1 THEN  m.purchasingCycle
			WHEN m.shipDirect = 1 AND m.dropShipCapable = 1 THEN m.leadTime
			WHEN m.shipDirect = 0 AND m.dropShipCapable = 1 THEN
				CASE
					WHEN p.cost > m.dropShipMin THEN m.leadTime
					ELSE m.purchasingCycle
				END
			ELSE m.purchasingCycle
		END	
	FROM mfg m JOIN products p on m.mfgID = p.mfgID 
	where p.active = 1
	
	-- IF A VENDOR LEADTIME EXIST IN THE VENDOR TABLE.  OVERWRITE ALL MFG LEADTIME
	UPDATE p
	SET p.leadTime_mfg =	
		CASE
		 WHEN CAST(REPLACE([Lead Time Calculation], '', '') AS DECIMAL(10, 2)) < 3 THEN 'Typically ships within <span class="bold"> 1 - 2 business days</span> '
		 WHEN CAST(REPLACE([Lead Time Calculation], '', '') AS DECIMAL(10, 2)) BETWEEN 3 AND 6.99 THEN 'Typically ships within <span class="bold"> 3 - 6 business days</span> ' 
		 WHEN CAST(REPLACE([Lead Time Calculation], '', '') AS DECIMAL(10, 2)) BETWEEN 7 AND 13.99 THEN 'Typically ships within <span class="bold"> 7 - 13 business days</span> ' 
		 ELSE 'Extended Lead Time (<span class="bold">14+ days</span>)'
		END
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Vendor] v 
		JOIN products p ON v.[No_] = p.mfgid COLLATE SQL_Latin1_General_CP1_CI_AS
	WHERE LEN([Lead Time Calculation]) > 0
	
	-- OVERWRITE ALL LEADTIME WITH LEADTIME PROVIDED BY MFG
	UPDATE p
	SET p.leadTime = 
				CASE p2.leadtimeID
					WHEN 1 THEN 'Typically ships within <span class="bold"> 1 - 2 business days</span> '
					WHEN 2 THEN 'Typically ships within <span class="bold"> 3 - 6 business days</span> '
					WHEN 3 THEN 'Typically ships within <span class="bold"> 7 - 13 business days</span> '
					ELSE 'Extended Lead Time (<span class="bold">14+ days</span>)'
				END
	FROM products p JOIN product_leadtime_tmp p2 ON p.CODE = p2.code
	
	UPDATE products 
	SET leadTime = 'Typically ships within <span class="bold"> 1 - 2 business days</span>'
	WHERE qtyOnHand > 0
	
	-- TEMPORARY FIX FOR LIBBEY, SET ALL LIBBEY ITEMS NOT IN STOCK TO 14+ EFFECTIVE 9/28/2012 RUN FOR 30 DAYS
	UPDATE products
	SET leadTime = 'Extended Lead Time (<span class="bold">14+ days</span>)'
	WHERE qtyOnHand = 0
		AND mfgID = 634
		AND DATEDIFF(DAY, '9/28/2012', GETDATE()) < 60;
	
	
END
GO
