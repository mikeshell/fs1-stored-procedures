USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[openPORpt_SP]    Script Date: 01/30/2013 16:26:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[openPORpt_SP]
	@pageNum int, @pageSize int, @filterBy varchar(50)

AS
BEGIN
	DECLARE @startRowIndex INT, @recTT INT
	
	select vendor, COUNT(*) orderTT from openporpt group by vendor order by vendor
	
	
	IF @pageNum = 1
		SET @startRowIndex = @pageNum
	ELSE
		SET @startRowIndex = (@pageSize*(@pageNum-1)) + 1
	
	
	IF @filterBy = 'all'
	   BEGIN		
		SELECT @recTT = COUNT(*) FROM openporpt
		
		SELECT *, @recTT recTT FROM (select o.*, c.notes, c.id, ROW_NUMBER() OVER (order by o.poNum) AS RowNum 
		from openporpt o LEFT JOIN comment c ON o.prodNum = c.prodNum COLLATE SQL_Latin1_General_CP1_CI_AS
				and o.PONum = c.documentNum COLLATE SQL_Latin1_General_CP1_CI_AS and c.documentType = 'PO') AS openPO
		WHERE RowNum BETWEEN @startRowIndex AND (@pageNum*@pageSize)
		ORDER BY 1 ASC
	   END
	ELSE
	   BEGIN		
		SELECT o.*, ISNULL(c.notes, '') notes, c.id
		FROM openporpt o LEFT JOIN comment c ON o.prodNum = c.prodNum COLLATE SQL_Latin1_General_CP1_CI_AS
				and o.PONum = c.documentNum COLLATE SQL_Latin1_General_CP1_CI_AS and c.documentType = 'PO'
		WHERE vendor = @filterBy
		ORDER BY 1, 4 ASC
	   END
	/**	
	SELECT @recTT = COUNT(*) 
	FROM  fs1.katomdev.dbo.[B&B Equipment & Supply Inc_$Purchase Line] pl
			LEFT JOIN fs1.katomdev.dbo.[B&B Equipment & Supply Inc_$Purchase Header] ph ON pl.[Document No_] = ph.[No_]
			LEFT JOIN fs1.katomdev.dbo.[B&B Equipment & Supply Inc_$Sales Line] sl ON sl.[No_] = pl.[No_]
			left join Comment c on c.prodNum = pl.[No_] collate Latin1_General_CS_AS and c.documentNum = ph.[No_] collate Latin1_General_CS_AS and c.documentType = 'PO'
	WHERE pl.[Document Type] = 1
			AND ph.[Document Type] = 1
			AND pl.[Quantity] <> (pl.[Quantity Invoiced] + pl.[Qty_ to Invoice])
			AND DATEDIFF(DAY, ph.[Order Date], GETDATE()) > 3
	GROUP BY ph.[Order Date], pl.[Document No_], pl.[No_], pl.[Quantity],pl.[Qty_ to Invoice], pl.[Quantity Invoiced], 
			pl.[Expected Receipt Date], c.backorder, c.notes) AS openPO
	
	SELECT *, @recTT recTT FROM (
		SELECT ph.[Order Date] PODT, 
			DATEDIFF(DAY, ph.[Order Date], GETDATE()) numDays,
			pl.[Document No_] PONum, pl.[No_] prodNum, pl.[Quantity] qtyOrder, pl.[Qty_ to Invoice] qtyReceived, 
			pl.[Quantity Invoiced] qtyInvoiced, 
			CASE 
				WHEN DATEDIFF(DAY, pl.[Expected Receipt Date], ph.[Order Date]) = 0 THEN NULL
				ELSE pl.[Expected Receipt Date]
			END  ETA,
			COUNT(sl.[No_]) numOfOrder, ISNULL(SUM(sl.[Quantity]), 0) qtyCustomerOrder,	c.backorder, c.notes, 
			ROW_NUMBER() OVER (order by ph.[Order Date]) AS RowNum
		FROM  fs1.katomdev.dbo.[B&B Equipment & Supply Inc_$Purchase Line] pl
				LEFT JOIN fs1.katomdev.dbo.[B&B Equipment & Supply Inc_$Purchase Header] ph ON pl.[Document No_] = ph.[No_]
				LEFT JOIN fs1.katomdev.dbo.[B&B Equipment & Supply Inc_$Sales Line] sl ON sl.[No_] = pl.[No_]
				left join Comment c on c.prodNum = pl.[No_] collate Latin1_General_CS_AS and c.documentNum = ph.[No_] collate Latin1_General_CS_AS and c.documentType = 'PO'
		WHERE pl.[Document Type] = 1
				AND ph.[Document Type] = 1
				AND pl.[Quantity] <> (pl.[Quantity Invoiced] + pl.[Qty_ to Invoice])
				AND DATEDIFF(DAY, ph.[Order Date], GETDATE()) > 3
		GROUP BY ph.[Order Date], pl.[Document No_], pl.[No_], pl.[Quantity],pl.[Qty_ to Invoice], pl.[Quantity Invoiced], 
				pl.[Expected Receipt Date], c.backorder, c.notes) AS openPO
	WHERE RowNum BETWEEN @startRowIndex AND (@pageNum*@pageSize)
    ORDER BY 1 ASC
    **/

END
GO
