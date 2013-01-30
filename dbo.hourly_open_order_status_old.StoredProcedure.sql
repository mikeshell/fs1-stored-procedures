USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[hourly_open_order_status_old]    Script Date: 01/30/2013 16:26:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[hourly_open_order_status_old]
AS
BEGIN
	DECLARE @salesTT_SD MONEY, @orderTT_SD INT, @salesTT MONEY, @orderTT INT,
		@salesTT_SD_Miva MONEY, @orderTT_SD_Miva INT, @salesTT_Miva MONEY, @orderTT_Miva INT,
		@salesTT_SD_Printed MONEY, @orderTT_SD_Printed INT, @salesTT_Printed MONEY, @orderTT_Printed INT,
		@salesTT_SD_CCDeclined MONEY, @orderTT_SD_CCDeclined INT, @salesTT_CCDeclined MONEY, @orderTT_CCDeclined INT,
		@salesTT_PO MONEY, @orderTT_PO INT,
		@salesTT_SD_INV MONEY, @orderTT_SD_INV INT, @salesTT_INV MONEY, @orderTT_INV INT, @today DATETIME
	
	DECLARE @body1 VARCHAR(MAX), @email1 varchar(500), @subject1 varchar(150), @fontColor varchar(50)
	
	SET @today = GETDATE()
	SET @body1 = '<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"
	"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
	<html xmlns="http://www.w3.org/1999/xhtml">
	<head>
	<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
	<title>E-Mail</title>

	<style type="text/css">
	body,td,th {font-family: Arial, Helvetica, sans-serif;font-size: 12px; vertical-align: top; }
	</style>

	</head>
	<body>
	<H2>Internet Open Orders as of ' + CONVERT(VARCHAR(20), @today, 0) + '</H2>
	<table border="0" cellspacing="0" cellpadding="3" style="border:solid 1px #000000;">
	  <tr style="background-color:#446fb7; color:#FFFFFF; font-weight: bold;">
		   <td width="75">&nbsp;</td>
		<td style="text-align:center; width: 100px;">Orders</td>
		<td style="text-align:center; width: 100px;">from MIVA</td>
		<td style="text-align:center; width: 100px;">Printed</td>
		<td style="text-align:center; width: 100px;">PO Created</td>
		<td style="text-align:center; width: 100px;">CC Declined</td>
		<td style="text-align:center; width: 100px;">Invoiced</td>
	  </tr>'
	  		
	--Ship Direct
	SELECT 
		--@salesTT_SD = CAST(SUM(ISNULL(il.[Quantity], 0)*ISNULL(il.[Unit Price], 0)) as decimal(10, 2)),
		@salesTT_SD = CAST(ISNULL(SUM(il.[Line Amount]), 0) as decimal(10, 2)),
		@orderTT_SD = COUNT(distinct ISNULL(x.[No_], 0))		
	FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Header] ih
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Line] il
			ON ih.[No_]  = il.[Document No_]
		JOIN (SELECT DISTINCT [No_] FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Header]) as X 
			ON X.[No_] = ih.[No_]
	WHERE DATEDIFF(DAY, [Order Date], @today) = 0
		AND LEN(ISNULL(ih.[Customer Source], '')) > 0
		AND ih.[Document Type] = 1
		AND ih.[Customer Source] = 'I'
		AND il.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
		AND LEN(il.[No_]) > 0
		AND LEFT(il.[No_], 3) IN (select mfgid collate SQL_Latin1_General_CP1_CI_AS from mfg where Active = 1 and shipDirect = 1)
		AND il.[No_] NOT IN (
					select [No_]
					from fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Item]
					where [Maximum Inventory] > 0
					)
		
	--non-ship Direct
	SELECT 
		--@salesTT =CAST(SUM(ISNULL(il.[Quantity], 0)*ISNULL(il.[Unit Price], 0)) as decimal(10, 2)),
		@salesTT = CAST(ISNULL(SUM(il.[Line Amount]), 0) as decimal(10, 2)),
		@orderTT = COUNT(distinct ISNULL(x.[No_], 0))		
	FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Header] ih
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Line] il
			ON ih.[No_]  = il.[Document No_]
		JOIN (SELECT DISTINCT [No_] FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Header]) as X 
			ON X.[No_] = ih.[No_]
	WHERE DATEDIFF(DAY, [Order Date], @today) = 0
		AND LEN(ISNULL(ih.[Customer Source], '')) > 0
		AND ih.[Document Type] = 1
		AND ih.[Customer Source] = 'I'
		AND il.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
		AND LEN(il.[No_]) > 0
		AND (LEFT(il.[No_], 3) NOT IN (select mfgid collate SQL_Latin1_General_CP1_CI_AS from mfg where Active = 1 and shipDirect = 1)	
			OR il.[No_] IN (
					select [No_]
					from fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Item]
					where [Maximum Inventory] > 0
					))
	
	--miva import - Ship Direct orders
	select @salesTT_SD_Miva = ISNULL(SUM(price*quantity), 0), 
		   @orderTT_SD_Miva = COUNT(distinct customerpo) 
	from order_hdr o JOIN order_items i ON o.customerpo = i.order_id 
	where DATEDIFF(day, ordertimestamp, @today) = 0
		AND LEFT(i.code, 3) IN (select mfgid from mfg where Active = 1 and shipDirect = 1)		
		AND i.code NOT IN (
					select [No_] collate SQL_Latin1_General_CP1_CI_AS
					from fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Item]
					where [Maximum Inventory] > 0
					)
							
	--miva import - non ship direct orders
	select @salesTT_Miva = ISNULL(SUM(price*quantity), 0), 
		   @orderTT_Miva = COUNT(distinct customerpo) 
	from order_hdr o JOIN order_items i ON o.customerpo = i.order_id 
	where DATEDIFF(day, ordertimestamp, @today) = 0
		AND (LEFT(i.code, 3) NOT IN (select mfgid from mfg where Active = 1 and shipDirect = 1)
			OR i.code IN (
					select [No_] collate SQL_Latin1_General_CP1_CI_AS
					from fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Item]
					where [Maximum Inventory] > 0
					))
					
	--printed ticket - Ship Direct
	SELECT 
		--@salesTT_SD_Printed = CAST(SUM(ISNULL(il.[Quantity], 0)*ISNULL(il.[Unit Price], 0)) as decimal(10, 2)),
		@salesTT_SD_Printed = CAST(ISNULL(SUM(il.[Line Amount]), 0) as decimal(10, 2)),
		@orderTT_SD_Printed = COUNT(distinct ISNULL(x.[No_], 0))		
	FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Header] ih
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Line] il
			ON ih.[No_]  = il.[Document No_]
		JOIN (SELECT DISTINCT [No_] FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Header]) as X 
			ON X.[No_] = ih.[No_]
	WHERE DATEDIFF(DAY, [Order Date], @today) = 0
		AND LEN(ISNULL(ih.[Customer Source], '')) > 0
		AND ih.[Document Type] = 1
		AND ih.[Customer Source] = 'I'
		AND ih.[No_ Printed] > 0
		AND il.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
		AND LEFT(il.[No_], 3) IN (select mfgid collate SQL_Latin1_General_CP1_CI_AS from mfg where Active = 1 and shipDirect = 1)		
		AND LEN(il.[No_]) > 0
		AND il.[No_] NOT IN (
					select [No_]
					from fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Item]
					where [Maximum Inventory] > 0
					)
					
	--printed ticket
	SELECT 
		--@salesTT_Printed = CAST(SUM(ISNULL(il.[Quantity], 0)*ISNULL(il.[Unit Price], 0)) as decimal(10, 2)),
		@salesTT_Printed = CAST(ISNULL(SUM(il.[Line Amount]), 0) as decimal(10, 2)),
		@orderTT_Printed = COUNT(distinct ISNULL(x.[No_], 0))		
	FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Header] ih
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Line] il
			ON ih.[No_]  = il.[Document No_]
		JOIN (SELECT DISTINCT [No_] FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Header]) as X 
			ON X.[No_] = ih.[No_]
	WHERE DATEDIFF(DAY, [Order Date], @today) = 0
		AND LEN(ISNULL(ih.[Customer Source], '')) > 0
		AND ih.[Document Type] = 1
		AND ih.[Customer Source] = 'I'
		AND ih.[No_ Printed] > 0
		AND il.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
		AND LEN(il.[No_]) > 0
		AND (LEFT(il.[No_], 3) NOT IN (select mfgid collate SQL_Latin1_General_CP1_CI_AS from mfg where Active = 1 and shipDirect = 1)	
			OR il.[No_] IN (
					select [No_]
					from fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Item]
					where [Maximum Inventory] > 0
					))
		
	--PO Processed
	select @orderTT_PO = count(distinct h.[No_]), @salesTT_PO = ISNULL(sum(distinct l.Amount), 0)
	from fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Purchase Header] h
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Purchase Line] l ON h.[No_] = l.[Document No_]
	where h.[Ship-to Name] <> 'WAREHOUSE FACILITY' and LEN(ISNULL(l.[No_], '')) > 0 
			and l.Amount > 0 and l.[No_] not in ('555', '540', '205')
			and DATEDIFF(day, h.[Order Date], @today) = 0
	
	IF @orderTT_SD_Printed < @orderTT_SD
		SET @fontColor = 'style="color:#FF0000; font-weight: bold;"'
	ELSE 
		SET @fontColor = ''
	
	SET @body1 = @body1 + ' <tr>
		<td>Ship Direct</td>
		<td align="center" >' + '$' + LEFT(CONVERT(varchar(50), ROUND(@salesTT_SD, 0), 1), CHARINDEX('.', @salesTT_SD)) + ' (' + CONVERT(varchar(50), ISNULL(@orderTT_SD, 0), 1) + ')</td>
		<td align="center" >' + '$' + LEFT(CONVERT(varchar(50), ROUND(@salesTT_SD_Miva, 0), 1), CHARINDEX('.', @salesTT_SD_Miva)) + ' (' + CONVERT(varchar(50), ISNULL(@orderTT_SD_Miva, 0), 1) + ')</td>
		<td align="center" ' + @fontColor + '>' + '$' + LEFT(CONVERT(varchar(50), ROUND(@salesTT_SD_Printed, 0), 1), CHARINDEX('.', @salesTT_SD_Printed)) + ' (' + CONVERT(varchar(50), ISNULL(@orderTT_SD_Printed, 0), 1) + ')</td>
		<td align="center" >' + '$' + LEFT(CONVERT(varchar(50), ROUND(@salesTT_PO, 0), 1), CHARINDEX('.', @salesTT_PO)) + ' (' + CONVERT(varchar(50), ISNULL(@orderTT_PO, 0), 1) + ')</td>
		<td align="center" >N/A</td>
		<td align="center" >N/A</td>
	</tr>'
	
	IF @orderTT_Printed < @orderTT
		SET @fontColor = 'style="color:#FF0000; font-weight: bold;"'
	ELSE 
		SET @fontColor = ''
		
	SET @body1 = @body1 + '<tr>
		<td>All Others</td>
		<td align="center" >' + '$' + LEFT(CONVERT(varchar(50), ROUND(@salesTT, 0), 1), CHARINDEX('.', @salesTT)) + ' (' + CONVERT(varchar(50), ISNULL(@orderTT, 0), 1) + ')</td>
		<td align="center" >' + '$' + LEFT(CONVERT(varchar(50), ROUND(@salesTT_Miva, 0), 1), CHARINDEX('.', @salesTT_Miva)) + ' (' + CONVERT(varchar(50), ISNULL(@orderTT_Miva, 0), 1) + ')</td>
		<td align="center" ' + @fontColor + '>' + '$' + LEFT(CONVERT(varchar(50), ROUND(@salesTT_Printed, 0), 1), CHARINDEX('.', @salesTT_Printed))  + ' (' + CONVERT(varchar(50), ISNULL(@orderTT_Printed, 0), 1) + ')</td>
		<td align="center" >0</td>
		<td align="center" >N/A</td>
		<td align="center" >N/A</td>
	</tr></table>'
	
	SET @body1 = @body1 + '<H2>Overall Performance</H2>
	<table border="0" cellspacing="0" cellpadding="3" style="border:solid 1px #000000;">
	  <tr style="background-color:#446fb7; color:#FFFFFF; font-weight: bold;">
		   <td width="100">&nbsp;</td>
		<td style="text-align:center; width: 120px;">Invoiced</td>
		<td style="text-align:center; width: 120px;">Open</td>
	  </tr>'
	
	DECLARE @iInvoiced VARCHAR(50), @iSales VARCHAR(50), @cInvoiced VARCHAR(50), @cSales VARCHAR(50),
			@sInvoiced VARCHAR(50), @sSales VARCHAR(50), @bInvoiced VARCHAR(50), @bSales VARCHAR(50),
			@gInvoiced VARCHAR(50), @gSales VARCHAR(50), @uInvoiced VARCHAR(50), @uSales VARCHAR(50),
			@tInvoiced VARCHAR(50), @tSales VARCHAR(50)
	
	SELECT @iInvoiced = ISNULL([InternetInvoiced], '0 (0)'), 
			@iSales = ISNULL([InternetSales], '0 (0)'), 
			@cInvoiced = ISNULL([CatalogInvoiced], '0 (0)'), 
			@cSales = ISNULL([CatalogSales], '0 (0)'), 
			@sInvoiced = ISNULL([SalesmenInvoiced], '0 (0)'), 
			@sSales = ISNULL([SalesmenSales], '0 (0)'), 
			@bInvoiced = ISNULL([BidInvoiced], '0 (0)'), 
			@bSales = ISNULL([BidSales], '0 (0)'), 
			@gInvoiced = ISNULL([GSAInvoiced], '0 (0)'), 
			@gSales = ISNULL([GSASales], '0 (0)'), 
			@uInvoiced = ISNULL([UnknownInvoiced], '0 (0)'), 
			@uSales = ISNULL([UnknownSales], '0 (0)'), 
			@tInvoiced = ISNULL([TotalInvoiced], '0 (0)'), 
			@tSales = ISNULL([TotalSales], '0 (0)')
	FROM salesStat30Days
	WHERE DATEDIFF(DAY, [Date], GETDATE()) = 0
	
	SET @body1 = @body1 + '
	  <tr>
		<td width="75">Internet</td>
		<td style="text-align:center; width: 120px;">$' + LEFT(CONVERT(varchar(50), ROUND(CAST(Left(@iInvoiced, charindex('(', @iInvoiced)-1) AS MONEY), 0), 1), charindex('.', @iInvoiced)) + right(@iInvoiced, len(@iInvoiced)-charindex('(', @iInvoiced)+2) + '</td>
		<td style="text-align:center; width: 120px;">$' + LEFT(CONVERT(varchar(50), ROUND(CAST(Left(@iSales, charindex('(', @iSales)-1) AS MONEY), 0), 1), charindex('.', @iSales)) + right(@iSales, len(@iSales)-charindex('(', @iSales)+2) + '</td>
	  </tr>
	  <tr>
		<td width="75">Catalog</td>
		<td style="text-align:center; width: 120px;">$' + LEFT(CONVERT(varchar(50), ROUND(CAST(Left(@cInvoiced, charindex('(', @cInvoiced)-1) AS MONEY), 0), 1), charindex('.', @cInvoiced)) + right(@cInvoiced, len(@cInvoiced)-charindex('(', @cInvoiced)+2) + '</td>
		<td style="text-align:center; width: 120px;">$' + LEFT(CONVERT(varchar(50), ROUND(CAST(Left(@cSales, charindex('(', @cSales)-1) AS MONEY), 0), 1), charindex('.', @cSales)) + right(@cSales, len(@cSales)-charindex('(', @cSales)+2) + '</td>
	  </tr>
	  <tr>
		<td width="75">Outside Sales</td>
		<td style="text-align:center; width: 120px;">$' + LEFT(CONVERT(varchar(50), ROUND(CAST(Left(@sInvoiced, charindex('(', @sInvoiced)-1) AS MONEY), 0), 1), charindex('.', @sInvoiced)) + right(@sInvoiced, len(@sInvoiced)-charindex('(', @sInvoiced)+2) + '</td>
		<td style="text-align:center; width: 120px;">$' + LEFT(CONVERT(varchar(50), ROUND(CAST(Left(@sSales, charindex('(', @sSales)-1) AS MONEY), 0), 1), charindex('.', @sSales)) + right(@sSales, len(@sSales)-charindex('(', @sSales)+2) + '</td>
	  </tr>
	  <tr>
		<td width="75">Bid</td>
		<td style="text-align:center; width: 120px;">$' + LEFT(CONVERT(varchar(50), ROUND(CAST(Left(@bInvoiced, charindex('(', @bInvoiced)-1) AS MONEY), 0), 1), charindex('.', @bInvoiced)) + right(@bInvoiced, len(@bInvoiced)-charindex('(', @bInvoiced)+2) + '</td>
		<td style="text-align:center; width: 120px;">$' + LEFT(CONVERT(varchar(50), ROUND(CAST(Left(@bSales, charindex('(', @bSales)-1) AS MONEY), 0), 1), charindex('.', @bSales)) + right(@bSales, len(@bSales)-charindex('(', @bSales)+2) + '</td>
	  </tr>
	  <tr>
		<td width="75">GSA</td>
		<td style="text-align:center; width: 120px;">$' + LEFT(CONVERT(varchar(50), ROUND(CAST(Left(@gInvoiced, charindex('(', @gInvoiced)-1) AS MONEY), 0), 1), charindex('.', @gInvoiced)) + right(@gInvoiced, len(@gInvoiced)-charindex('(', @gInvoiced)+2) + '</td>
		<td style="text-align:center; width: 120px;">$' + LEFT(CONVERT(varchar(50), ROUND(CAST(Left(@gSales, charindex('(', @gSales)-1) AS MONEY), 0), 1), charindex('.', @gSales)) + right(@gSales, len(@gSales)-charindex('(', @gSales)+2) + '</td>
	  </tr>
	  <tr>
		<td width="75">Unknown</td>
		<td style="text-align:center; width: 120px;">$' + LEFT(CONVERT(varchar(50), ROUND(CAST(Left(@uInvoiced, charindex('(', @uInvoiced)-1) AS MONEY), 0), 1), charindex('.', @uInvoiced)) + right(@uInvoiced, len(@uInvoiced)-charindex('(', @uInvoiced)+2) + '</td>
		<td style="text-align:center; width: 120px;">$' + LEFT(CONVERT(varchar(50), ROUND(CAST(Left(@uSales, charindex('(', @uSales)-1) AS MONEY), 0), 1), charindex('.', @uSales)) + right(@uSales, len(@uSales)-charindex('(', @uSales)+2) + '</td>
	  </tr>
	  <tr>
		<td width="75">Total</td>
		<td style="text-align:center; width: 120px;">$' + LEFT(CONVERT(varchar(50), ROUND(CAST(Left(@tInvoiced, charindex('(', @tInvoiced)-1) AS MONEY), 0), 1), charindex('.', @tInvoiced)) + right(@tInvoiced, len(@tInvoiced)-charindex('(', @tInvoiced)+2) + '</td>
		<td style="text-align:center; width: 120px;">$' + LEFT(CONVERT(varchar(50), ROUND(CAST(Left(@tSales, charindex('(', @tSales)-1) AS MONEY), 0), 1), charindex('.', @tSales)) + right(@tSales, len(@tSales)-charindex('(', @tSales)+2) + '</td>
	  </tr></table>'	
	
	SET @body1 = @body1 + '</body></html>'
	 
	-- PREPPING THE EMAIL TO SEND
	SET @email1 = 'dlu@katom.com; pbible@katom.com; pchesworth@katom.com; cbible@katom.com; jrogers@katom.com'
--	SET @email1 = 'david@katom.com'
	SET @subject1 = 'Internet Open Orders as of ' + CONVERT(VARCHAR(20), @today, 0) --Stat For '  + CAST(@mm AS VARCHAR(2)) + '/' + CAST(@dd AS VARCHAR(2)) + '/' + CAST(@yyyy AS VARCHAR(4))

	EXEC msdb.dbo.sp_send_dbmail @profile_name='katom', @recipients=@email1,	@subject=@subject1,	@body=@body1, @body_format ='HTML',	@blind_copy_recipients='katom.archive@gmail.com'	

END
GO
