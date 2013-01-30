USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[NAVSalesOrderUpdate]    Script Date: 01/30/2013 16:26:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[NAVSalesOrderUpdate]
AS
BEGIN
	-- INSERT SALES HEADER TABLE
	TRUNCATE TABLE NAVSalesHeader

	INSERT INTO NAVSalesHeader
	SELECT 
		CASE [Document Type]
			WHEN 0 THEN 'Quote'
			WHEN 1 THEN 'Open Order'
			WHEN 2 THEN 'Open Invoice'
			WHEN 3 THEN 'Open Blanket Order'
		END docType, 
		[No_] orderID, [Sell-to Customer No_] selltoCustomerID, 
		[Name], [Address] [address], [Address 2] address2, [City], [Contact], [Your Reference] reference, 
		[Ship-to Name] shiptoName, [Ship-to Address] shiptoAddress, [Ship-to Address 2] shiptoAddress2, 
		[Ship-to City] shiptoCity, [Ship-to Contact] shiptoContact, [Order Date] orderDT, 
		[Posting Date] postingDT, [Shipment Date] shipmentDT, 
		[Posting Description] postingDesc, [Payment Terms Code] paymentTermsCode, 
		[Due Date] dueDT, [Shipment Method Code] shipmentMethod, [Cust__Item Disc_ Gr_] custDiscGroup, 
		[Salesperson Code] salesperson, [Sell-to Customer Name] selltoCustomerName, 
		[Sell-to Address] selltoAddress, [Sell-to Address 2] selltoAddress2, [Sell-to City] selltoCity, 
		[Sell-to Contact] selltoContact, [ZIP Code] zipcode, [State] states, [Sell-to ZIP Code] selltoZipcode, 
		[Sell-to State] selltoState, [Ship-to ZIP Code] shiptoZipcode, [Ship-to State] shiptoState, 
		[Document Date] documentDT, [Shipping Agent Code] shippingType, [Package Tracking No_] trackingNum, 
		[Tax Area Code] taxAreaCode, [Ship-to UPS Zone] upsZone, [New Customer] newCustomer, 
		[Phone No_] phoneNum, [E-Mail], 
		CASE [Order Medium]
			WHEN 'FAX' THEN 'Fax'
			WHEN 'W' THEN 'Walk-In'
			WHEN 'E' THEN 'Email'
			WHEN 'F' THEN 'Fax'
			WHEN 'P' THEN 'Phone'
			WHEN 'G' THEN 'GSA'
			WHEN 'B' THEN 'Bid'
			WHEN 'I' THEN 'Miva'
			WHEN 'NA' THEN ''
		END orderMedium
	FROM fs1.z_SANDBOX.dbo.[B&B Equipment & Supply Inc_$Sales Header] WITH(NOLOCK)
	ORDER BY [Order Date] DESC

	--INSERT SALES LINE TABLE
	TRUNCATE TABLE NAVSalesLine

	INSERT INTO NAVSalesLine
	SELECT 
		CASE [Document Type]
			WHEN 0 THEN 'Quote'
			WHEN 1 THEN 'Open Order'
			WHEN 2 THEN 'Open Invoice'
			WHEN 3 THEN 'Open Blanket Order'
		END docType,
		[Document No_], [Line No_], [Sell-to Customer No_], [No_], 
		[Shipment Date], [Description], [Unit of Measure], 
		[Quantity], [Unit Price], [Unit Cost ($)], [Tax %], 
		[Amount], [Amount Including Tax], [Gross Weight], 
		[Net Weight], [Project Code], [Est_ Freight]
	FROM fs1.z_SANDBOX.dbo.[B&B Equipment & Supply Inc_$Sales Line] WITH(NOLOCK)
	ORDER BY [Shipment Date] DESC
END
GO
