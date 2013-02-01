USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[NAVInvoiceUpdate]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[NAVInvoiceUpdate]
AS
BEGIN
	--INSERT MISSING INVOICE 
	INSERT INTO NAVInvoiceHeader
	SELECT
		[No_], [Sell-to Customer No_], 
		[Name], [Address], [City], [Contact],  
		[Ship-to Name], [Ship-to Address], [Ship-to Address 2], [Ship-to City], [Ship-to Contact], 
		[Order Date], [Posting Date], [Shipment Date], 
		[Posting Description], [Payment Terms Code], 
		[Due Date], [Shipment Method Code], 
		[Project Code], [Cust__Item Disc_ Gr_], [Salesperson Code], 
		[Order No_], [No_ Printed], 
		[Sell-to Customer Name], [Sell-to Address], [Sell-to Address 2], [Sell-to City], [Sell-to Contact], 
		[ZIP Code], [State], [Country Code], 
		[Sell-to ZIP Code], [Sell-to State], [Sell-to Country Code], [Ship-to ZIP Code], 
		[Ship-to State], [Document Date], [Shipping Agent Code], [Package Tracking No_],  
		[User ID], [Source Code], [Ship-to UPS Zone], [Phone No_], [Fax No_], [E-Mail]
		[Customer Source], getdate()
	FROM fs1.z_SANDBOX.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] WITH(NOLOCK)
	WHERE DATEDIFF(DAY, [Order Date], GETDATE()) < 31
			AND [No_] NOT IN (SELECT [InvoiceID] from NAVInvoiceHeader)

	--UPDATE TRACKING INFORMATION
	UPDATE i
	SET i.trackingCode = i2.[Package Tracking No_]
	FROM NAVInvoiceHeader i join fs1.z_SANDBOX.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] i2 WITH(NOLOCK)
							on i.invoiceID = i2.[No_]
	WHERE LEN(ISNULL([Package Tracking No_], '')) > 0
			AND i.trackingCode <> i2.[Package Tracking No_]

	-- INSERT INVOICE LINE
	SELECT
		[Document No_] invoiceID, [Line No_] lineID, [Sell-to Customer No_] selltoCustomerID, 
		[Type], [No_] prodID, [Quantity Disc_ Code] qtyDiscCode, [Shipment Date] shipmentDT, 
		[Unit of Measure] uom, [Quantity] qty, [Unit Price] unitPrice, [Unit Cost ($)] unitCost, 
		[Tax %] tax, [Amount] subtotal, [Amount Including Tax] subtotalWithTax, [Gross Weight] grossWeight, 
		[Net Weight] netWeight, [Project Code] porjectCode, [Price Group Code] priceGroupCode, 
		[Drop Shipment] dropship, [Tax Area Code] taxAreaCode, [Tax Base Amount] taxAmount,
		[Quantity Ordered] qtyOrdered, [Quantity Back Ordered] qtyBackOrdered
	INTO #tmp
	FROM fs1.z_SANDBOX.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] WITH(NOLOCK)
	WHERE DATEDIFF(DAY, [Shipment Date], GETDATE()) < 31
	ORDER BY [Shipment Date] DESC

	DELETE FROM t
	FROM #tmp t join NAVInvoiceLine i on t.invoiceID = i.invoiceID
									  and t.lineID = i.lineID
									  and t.selltoCustomerID = i.selltoCustomerID

	INSERT INTO NAVInvoiceLine
	SELECT * FROM #tmp

	DROP TABLE #tmp
END
GO
