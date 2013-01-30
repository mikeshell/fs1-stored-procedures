USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[NAVCreditMemoUpdate]    Script Date: 01/30/2013 16:26:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[NAVCreditMemoUpdate]
AS
BEGIN
	--INSERT CREDIT MEMO HEADER
	INSERT INTO NAVCreditMemoHeader
	SELECT
		[No_], [Sell-to Customer No_], [Name], [Address], [Address 2], [City], 
		[Contact], [Your Reference], [Ship-to Code], [Ship-to Name], [Ship-to Address], 
		[Ship-to Address 2], [Ship-to City], [Ship-to Contact], [Posting Date], 
		[Shipment Date], [Posting Description], [Payment Terms Code], [Due Date], 
		[Shipment Method Code], [Project Code], [Invoice Disc_ Code], [Cust__Item Disc_ Gr_],
		[Salesperson Code], [On Hold], [Applies-to Doc_ Type], [Applies-to Doc_ No_], 
		[Sell-to Customer Name], [Sell-to Address], [Sell-to City], [Sell-to Contact], 
		[ZIP Code], [State], [Sell-to ZIP Code], [Sell-to State], [Ship-to ZIP Code], 
		[Ship-to State], [Document Date], [User ID] userID, [Source Code], [Tax Area Code], 
		[Ship-to UPS Zone], [Phone No_], [E-Mail], [Credit Approval Obtained]
	FROM fs1.z_SANDBOX.dbo.[B&B Equipment & Supply Inc_$Sales Cr_Memo Header] WITH(NOLOCK)
	WHERE DATEDIFF(DAY, [Document Date], GETDATE()) < 31 
		  AND [No_] NOT IN (SELECT creditMemoID FROM NAVCreditMemoHeader)
	ORDER BY [No_] DESC

	--INSERT CREDIT MEMO LINE
	SELECT
		[Document No_] creditMemoID, [Line No_] lineID, [Sell-to Customer No_] selltoCustoemerID, 
		[Type], [No_] prodID, [Shipment Date] shipmentDT, [Description] prodDesc, [Unit of Measure] uom, 
		[Quantity] qty, [Unit Price] unitPrice, [Unit Cost ($)] unitCost, [Tax %] tax, [Quantity Disc_ %], 
		[Amount] subtotal, [Amount Including Tax] subtotalwithTax, [Gross Weight] grossWeight, 
		[Net Weight] netWeight, [Project Code] projectCode, [Tax Area Code] taxAreaCode,
		[Tax Group Code] taxGroupCode, [Tax Base Amount] taxValue
	INTO #tmp
	FROM fs1.z_SANDBOX.dbo.[B&B Equipment & Supply Inc_$Sales Cr_Memo Line] WITH(NOLOCK)
	WHERE DATEDIFF(DAY, [Shipment Date], GETDATE()) < 31

	DELETE FROM t
	FROM #tmp t JOIN NAVCreditMemoLine c ON t.creditMemoID = c.creditMemoID
										 AND t.lineID = c.lineID 
										 AND t.selltoCustoemerID = c.selltoCustoemerID

	INSERT INTO NAVCreditMemoLine SELECT * FROM #tmp

	DROP TABLE #tmp
END
GO
