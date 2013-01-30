USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[openOrderRpt]    Script Date: 01/30/2013 16:26:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[openOrderRpt]
AS
BEGIN
	--POPULATING OPEN PO DATA
	TRUNCATE TABLE openporpt

	INSERT INTO openporpt
	SELECT ph.[Order Date] PODT, 
		DATEDIFF(DAY, ph.[Order Date], GETDATE()) numDays,
		pl.[Document No_] PONum, pl.[No_] prodNum, ph.[Buy-from Vendor Name], 
		pl.[Quantity] qtyOrder, pl.[Qty_ to Invoice] qtyReceived, 
		pl.[Quantity Invoiced] qtyInvoiced, 
		CASE 
			WHEN DATEDIFF(DAY, pl.[Expected Receipt Date], ph.[Order Date]) = 0 THEN NULL
			ELSE pl.[Expected Receipt Date]
		END  ETA,
		COUNT(ISNULL(sl.[No_], 0)) numOfOrder, SUM(ISNULL(sl.[Quantity], 0)) qtyCustomerOrder
	FROM  fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Purchase Line] pl
			LEFT JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Purchase Header] ph ON pl.[Document No_] = ph.[No_]
			LEFT JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Line] sl ON sl.[No_] = pl.[No_]
	WHERE pl.[Document Type] = 1
			AND ph.[Document Type] = 1
			AND pl.[Quantity] <> (pl.[Quantity Invoiced] + pl.[Qty_ to Invoice])
			AND DATEDIFF(DAY, ph.[Order Date], GETDATE()) > 3
	GROUP BY ph.[Order Date], pl.[Document No_], ph.[Buy-from Vendor Name], pl.[No_], pl.[Quantity],pl.[Qty_ to Invoice], pl.[Quantity Invoiced], 
			pl.[Expected Receipt Date]
	order by  ph.[Buy-from Vendor Name], pl.[Document No_]
			
	DELETE FROM comment 
	WHERE documentNum NOT IN (SELECT [No_] COLLATE SQL_Latin1_General_CP1_CI_AS
								FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Purchase Header])
			AND documentType = 'PO'
			
	--POPULATING OPEN ORDER DATA
	TRUNCATE TABLE openOrder
	
	INSERT INTO openOrder
	select  
		  CAST(h.[No_] AS NVARCHAR(255)) orderNum, 
		  CAST(h.[Bill-to Name] AS NVARCHAR(255)) [Name], 
		  CAST(h.[Bill-to Address] AS NVARCHAR(255)) [Address], 
		  CAST(h.[Bill-to City] AS NVARCHAR(255)) [City], 
		  h.[Order Date] orderDT, 
		  CAST(h.[Payment Terms Code] AS NVARCHAR(255)) PaymentType, 
		  h.[Due Date] dueDT, 
		  CAST(h.[Shipment Method Code] AS NVARCHAR(255)) [ShipmentType],  
		  CASE h.[Salesperson Code]
				WHEN 'CHRISTY' THEN CAST('LIBBY' AS NVARCHAR(255))
				ELSE CAST(h.[Salesperson Code] AS NVARCHAR(255))
		  END [Salesperson], 
		  CAST(SUM(ISNULL(il.[Quantity], 0)*ISNULL(il.[Unit Price], 0)) as decimal(10, 2)) salesTT,
		  COUNT(il.[No_]) NumItems,
		  CASE 
			WHEN h.[No_ Printed] = 0 THEN 0
			ELSE 1
		  END ticketPrinted,
		  [Order Status] orderStatus, [Credit Card Info Sent] CCSent, [Credit Approval Obtained] CCAprObtained,
		  LEFT([Ship-to Post Code], 5) zipCode,
		  CASE	
			WHEN LEFT([Ship-to Post Code], 5) IN (SELECT zipcode COLLATE SQL_Latin1_General_CP1_CI_AS FROM zipcodetable) THEN 0
			ELSE 1
		 END INTL,
		 CASE h.[Customer Source]
			WHEN 'G' THEN 'GSA'
			WHEN 'I' THEN 'Internet'
			WHEN 'E' THEN 'Ebay'
			WHEN 'S' THEN 'Oustide Sales'
			WHEN 'C' THEN 'Catalog'
			WHEN 'B' THEN 'Bid'
			WHEN 'U' THEN 'Uncategorized'
		END [CustomerSource], 0, 0
	from fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Header] h
				LEFT JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Line] il ON h.[No_]  = il.[Document No_]
	where --charindex('-', h.[No_]) = 0
			h.[Document Type] = 1
			AND il.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
			AND LEN(il.[No_]) > 0
			/**
			AND (LEFT(il.[No_], 3) NOT IN (select mfgid collate SQL_Latin1_General_CP1_CI_AS from mfg where Active = 1 and shipDirect = 1)	
				OR il.[No_] IN (
						select [No_]
						from fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Item]
						where [Maximum Inventory] > 0
						))
			**/
			AND h.[Order Status] <> 'CANCEL'
	group by h.[No_], h.[Bill-to Name], h.[Bill-to Address], h.[Bill-to City], h.[Order Date], 
				 h.[Payment Terms Code], h.[Due Date], h.[Shipment Method Code],  h.[Salesperson Code],
				 h.[No_ Printed], [Order Status], [Credit Card Info Sent], [Credit Approval Obtained], [Ship-to Post Code], h.[Customer Source]
	order by salesTT desc, h.[Order Date] asc
	
	UPDATE openOrder 
	SET shipDirect = 1
	WHERE orderNum IN (SELECT DISTINCT ih.[No_]
						FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Header] ih
							JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Line] il
								ON ih.[No_]  = il.[Document No_]
						WHERE LEN(ISNULL(ih.[Customer Source], '')) > 0
							AND ih.[Document Type] = 1
							AND il.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
							AND LEN(il.[No_]) > 0
							AND ih.[Order Status] NOT IN ('Bring In Here')
							AND LEFT(il.[No_], 3) IN (select mfgid collate SQL_Latin1_General_CP1_CI_AS from mfg where Active = 1 and shipDirect = 1)
							AND il.[No_] NOT IN (
										select [No_]
										from fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item]
										where [Maximum Inventory] > 0
										))
	UPDATE openOrder SET shipDirect = 0 WHERE (orderStatus = 'International' OR INTL = 1) AND shipDirect = 1
	
	--MARK AN ORDER AS READY2SHIP									
	DECLARE @orderNum varchar(20), @numItems smallint, @numitemsInstock smallint

	DECLARE catCursor CURSOR FOR 
	select orderNum, numitems 
	from openOrder
	where shipDirect = 0
		AND PaymentType = 'CC'
		AND ticketPrinted = 1
		AND CCAprObtained = 1
		
	OPEN catCursor
	FETCH NEXT FROM catCursor INTO @orderNum, @numItems

	WHILE @@FETCH_STATUS = 0
	   BEGIN
		TRUNCATE TABLE openOrderW1
		
		INSERT INTO openOrderW1
		SELECT l.[Document No_]
		FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Line] l LEFT JOIN products p on p.CODE = l.[No_] collate Latin1_General_CS_AS
		WHERE [No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
			AND LEN([No_]) > 0
			AND [Document No_] = @orderNum
		GROUP BY l.[Document No_], l.[No_], p.[thresholdMin], p.[qtyonhand]
		HAVING SUM(l.[Quantity]) <= qtyOnHand	
		
		IF EXISTS(SELECT * FROM openOrderW1)
		   BEGIN
			SELECT @numitemsInstock = COUNT(*) FROM openOrderW1
			TRUNCATE TABLE openOrderW1
			
			IF ISNULL(@numitemsInstock, 0) = @numItems
			   BEGIN
				UPDATE openOrder SET ReadyToShip = 1 WHERE orderNum = @orderNum
			   END
		   END
		   
		FETCH NEXT FROM catCursor INTO @orderNum, @numItems
	   END

	CLOSE catCursor
	DEALLOCATE catCursor
END
GO
