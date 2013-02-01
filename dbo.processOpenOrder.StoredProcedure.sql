USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[processOpenOrder]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[processOpenOrder]
	@action tinyint
AS
BEGIN

IF @action = 1 
   BEGIN
	/**
	delete from openSalesWorktable
	where cast(isnull(navOrderID, '') as varchar(50)) not in (select [No_] from fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Header])
	**/
	
	DELETE FROM openOrderWorktable WHERE [shipDate] is null and Notes is null and processor is null
	
	update os
	set os.shipDT = ow.[shipDate],
		  os.notes = ow.Notes,
		  os.processor = ow.processor,
		  os.lastupdateDT = GETDATE()
	from openSalesWorktable os join openOrderWorktable ow on os.navOrderID = ow.[Nav Order ID]

	insert into openSalesWorktable
	select [Nav Order ID], shipDaTe, Notes, processor, GETDATE()
	from openOrderWorktable
	where isnull([Nav Order ID], 0) not in (select isnull(navOrderID, '') from openSalesWorktable)
		and [Nav Order ID] is not null
	
	DELETE FROM [openSalesWorktable] WHERE shipDT is null and Notes is null
   END
ELSE IF @action = 2 -- ALL OPEN SALES ORDERS THAT IS NOT INTL, FAILED CC OR SHIP DIRECT
   BEGIN
	select  
		  CAST(h.[No_] AS NVARCHAR(255)) [No_], 
		  CAST(h.[Bill-to Name] AS NVARCHAR(255)) [Name], 
		  CAST(h.[Bill-to Address] AS NVARCHAR(255)) [Address], 
		  CAST(h.[Bill-to City] AS NVARCHAR(255)) [City], 
		  h.[Order Date], 
		  CAST(h.[Payment Terms Code] AS NVARCHAR(255)) [Payment Terms Code], 
		  h.[Due Date], 
		  CAST(h.[Shipment Method Code] AS NVARCHAR(255)) [Shipment Method Code],  
		  CASE h.[Salesperson Code]
				WHEN 'CHRISTY' THEN CAST('LIBBY' AS NVARCHAR(255))
				ELSE CAST(h.[Salesperson Code] AS NVARCHAR(255))
		  END [Salesperson Code], 
		  CAST(SUM(ISNULL(il.[Quantity], 0)*ISNULL(il.[Unit Price], 0)) as decimal(10, 2)) salesTT,
		  w.shipDT, CAST(ISNULL(w.notes, '') AS NVARCHAR(255)) [notes],
		  CASE h.[No_ Printed]
				WHEN 0 THEN 'NO'
				ELSE 'YES'
		  END ticketPrinted,
		  w.processor
	from fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Header] h
				LEFT JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Line] il
					  ON h.[No_]  = il.[Document No_]
				LEFT JOIN openSalesWorktable w ON CAST(w.navOrderID AS NVARCHAR(25)) = h.[No_] COLLATE Latin1_General_CS_AS
	where charindex('-', h.[No_]) = 0
	 --     and datediff(day, h.[Order Date], getdate()) =0
			and DATEDIFF(DAY, GETDATE(), ISNULL(w.shipDT, GETDATE())) < 1
			and h.[Document Type] = 1
			AND LEFT([Bill-to Post Code], 5) IN (SELECT zipcode COLLATE SQL_Latin1_General_CP1_CI_AS FROM zipcodetable)
			AND il.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
			AND LEN(il.[No_]) > 0
			AND (LEFT(il.[No_], 3) NOT IN (select mfgid collate SQL_Latin1_General_CP1_CI_AS from mfg where Active = 1 and shipDirect = 1)	
				OR il.[No_] IN (
						select [No_]
						from fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Item]
						where [Maximum Inventory] > 0
						))
	group by h.[No_], h.[Bill-to Name], h.[Bill-to Address], h.[Bill-to City], h.[Order Date], 
				 h.[Payment Terms Code], h.[Due Date], h.[Shipment Method Code],  h.[Salesperson Code],
				 w.shipDT, w.notes, h.[No_ Printed], w.processor      
	order by w.shipDT desc, salesTT desc, h.[Order Date] asc
   END
ELSE IF @action = 3 --SHIP DIRECT ORDERS
   BEGIN
	select  
		  CAST(h.[No_] AS NVARCHAR(255)) [No_], 
		  CAST(h.[Bill-to Name] AS NVARCHAR(255)) [Name], 
		  CAST(h.[Bill-to Address] AS NVARCHAR(255)) [Address], 
		  CAST(h.[Bill-to City] AS NVARCHAR(255)) [City], 
		  h.[Order Date], 
		  CAST(h.[Payment Terms Code] AS NVARCHAR(255)) [Payment Terms Code], 
		  h.[Due Date], 
		  CAST(h.[Shipment Method Code] AS NVARCHAR(255)) [Shipment Method Code],  
		  CASE h.[Salesperson Code]
				WHEN 'CHRISTY' THEN CAST('LIBBY' AS NVARCHAR(255))
				ELSE CAST(h.[Salesperson Code] AS NVARCHAR(255))
		  END [Salesperson Code], 
		  CAST(SUM(ISNULL(il.[Quantity], 0)*ISNULL(il.[Unit Price], 0)) as decimal(10, 2)) salesTT,
		  w.shipDT, CAST(ISNULL(w.notes, '') AS NVARCHAR(255)) [notes],
		  CASE h.[No_ Printed]
				WHEN 0 THEN 'NO'
				ELSE 'YES'
		  END ticketPrinted,
		  w.processor
	from fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Header] h
				LEFT JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Line] il
					  ON h.[No_]  = il.[Document No_]
				LEFT JOIN openSalesWorktable w ON CAST(w.navOrderID AS NVARCHAR(25)) = h.[No_] COLLATE Latin1_General_CS_AS
	where datediff(day, h.[Order Date], getdate()) > 0
			AND DATEDIFF(DAY, GETDATE(), ISNULL(w.shipDT, GETDATE())) < 1
			AND h.[Document Type] = 1
			AND LEFT([Bill-to Post Code], 5) IN (SELECT zipcode COLLATE SQL_Latin1_General_CP1_CI_AS FROM zipcodetable)
			AND LEFT(il.[No_], 3) IN (select mfgid collate SQL_Latin1_General_CP1_CI_AS from mfg where Active = 1 and shipDirect = 1)
			AND il.[No_] NOT IN (
						select [No_]
						from fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Item]
						where [Maximum Inventory] > 0
						)
	group by h.[No_], h.[Bill-to Name], h.[Bill-to Address], h.[Bill-to City], h.[Order Date], 
				 h.[Payment Terms Code], h.[Due Date], h.[Shipment Method Code],  h.[Salesperson Code],
				 w.shipDT, w.notes, h.[No_ Printed], w.processor      
	UNION
	select  
		  CAST(h.[No_] AS NVARCHAR(255)) [No_], 
		  CAST(h.[Bill-to Name] AS NVARCHAR(255)) [Name], 
		  CAST(h.[Bill-to Address] AS NVARCHAR(255)) [Address], 
		  CAST(h.[Bill-to City] AS NVARCHAR(255)) [City], 
		  h.[Order Date], 
		  CAST(h.[Payment Terms Code] AS NVARCHAR(255)) [Payment Terms Code], 
		  h.[Due Date], 
		  CAST(h.[Shipment Method Code] AS NVARCHAR(255)) [Shipment Method Code],  
		  CASE h.[Salesperson Code]
				WHEN 'CHRISTY' THEN CAST('LIBBY' AS NVARCHAR(255))
				ELSE CAST(h.[Salesperson Code] AS NVARCHAR(255))
		  END [Salesperson Code], 
		  CAST(SUM(ISNULL(il.[Quantity], 0)*ISNULL(il.[Unit Price], 0)) as decimal(10, 2)) salesTT,
		  w.shipDT, CAST(ISNULL(w.notes, '') AS NVARCHAR(255)) [notes],
		  CASE h.[No_ Printed]
				WHEN 0 THEN 'NO'
				ELSE 'YES'
		  END ticketPrinted,
		  w.processor
	from fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Header] h
				LEFT JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Line] il
					  ON h.[No_]  = il.[Document No_]
				LEFT JOIN openSalesWorktable w ON CAST(w.navOrderID AS NVARCHAR(25)) = h.[No_] COLLATE Latin1_General_CS_AS
	where datediff(day, h.[Order Date], getdate()) > 0
		  AND DATEDIFF(DAY, GETDATE(), ISNULL(w.shipDT, GETDATE())) < 1
		  AND h.[Document Type] = 1
		  AND (il.[Description] like '%ship%' and il.[Description] like '%direct%')
		  AND LEFT([Bill-to Post Code], 5) IN (SELECT zipcode COLLATE SQL_Latin1_General_CP1_CI_AS FROM zipcodetable)
	group by h.[No_], h.[Bill-to Name], h.[Bill-to Address], h.[Bill-to City], h.[Order Date], 
				 h.[Payment Terms Code], h.[Due Date], h.[Shipment Method Code],  h.[Salesperson Code],
				 w.shipDT, w.notes, h.[No_ Printed], w.processor  	
	order by w.shipDT desc, salesTT desc, h.[Order Date] asc	
   END
ELSE IF @action = 4 --OPEN POs
   BEGIN
	SELECT ph.[Order Date] PODT, 
		pl.[Document No_] PONum, pl.[No_] prodNum, pl.[Quantity] qtyOrder, pl.[Qty_ to Invoice] qtyReceived, 
		pl.[Quantity Invoiced] qtyInvoiced, 
		CASE 
			WHEN DATEDIFF(DAY, pl.[Expected Receipt Date], ph.[Order Date]) = 0 THEN NULL
			ELSE pl.[Expected Receipt Date]
		END  ETA,
		COUNT(sl.[No_]) numOfOrder, ISNULL(SUM(sl.[Quantity]), 0) qtyCustomerOrder,	c.backorder, c.notes
	FROM  fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Purchase Line] pl
			LEFT JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Purchase Header] ph ON pl.[Document No_] = ph.[No_]
			LEFT JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Line] sl ON sl.[No_] = pl.[No_]
			left join Comment c on c.prodNum = pl.[No_] collate Latin1_General_CS_AS and c.documentNum = ph.[No_] collate Latin1_General_CS_AS and c.documentType = 'PO'
	WHERE pl.[Document Type] = 1
			AND ph.[Document Type] = 1
			AND pl.[Quantity] <> (pl.[Quantity Invoiced] + pl.[Qty_ to Invoice])
			AND DATEDIFF(DAY, ph.[Order Date], GETDATE()) > 3
	GROUP BY ph.[Order Date], pl.[Document No_], pl.[No_], pl.[Quantity],pl.[Qty_ to Invoice], pl.[Quantity Invoiced], 
			pl.[Expected Receipt Date], c.backorder, c.notes
	order by 1 asc
   END
ELSE IF @action = 5 --PROCESS OPEN PO SS
   BEGIN
	UPDATE c
	SET c.backOrder = cw.backOrder,
		c.notes = cw.notes,
		c.updateDT = GETDATE(),
		c.userID = cw.userID 
	FROM commentWorktable cw JOIN comment c on cw.prodNum = c.prodNum and cw.documentNum = c.documentNum
	WHERE c.documentType = 'PO' 
	
	DELETE FROM commentWorktable
	WHERE id IN (SELECT c.id 
				 FROM commentWorktable cw JOIN comment c on cw.prodNum = c.prodNum and cw.documentNum = c.documentNum
				 WHERE c.documentType = 'PO')
				 
	INSERT INTO comment
	SELECT prodNum, documentNum, backorder, notes, 'PO', GETDATE(), userID
	FROM commentWorktable
	WHERE LEN(ISNULL(backorder, '')) > 0 OR LEN(ISNULL(notes, '')) > 0
	
	DELETE FROM comment WHERE backorder is null and notes is null
	
	TRUNCATE TABLE commentWorktable
	
   END
END
GO
