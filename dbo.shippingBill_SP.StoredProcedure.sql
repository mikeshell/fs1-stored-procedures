USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[shippingBill_SP]    Script Date: 01/30/2013 16:26:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[shippingBill_SP]
AS
BEGIN
	DECLARE @sql VARCHAR(MAX), @upsLastUpdate DATETIME
	
	SELECT @upsLastUpdate = MAX([Column 4]) FROM UPSBILL
	TRUNCATE TABLE upsBill_RPT
	
	INSERT INTO upsBill_RPT
	SELECT @upsLastUpdate upsDT, i.[No_], i.[Web Order No_],  
		   CASE
			WHEN LEN(i.[Web Order No_]) = 0 THEN 'ph'
			WHEN LEFT(i.[Web Order No_], 2) = 'KT' THEN 'kt'
			WHEN LEFT(i.[Web Order No_], 2) = 'FA' THEN 'fa'
			WHEN LEFT(i.[Web Order No_], 2) = 'BB' THEN 'bb'
			WHEN LEFT(i.[Web Order No_], 2) = 'PI' THEN 'pi'
			ELSE 'eb'
		   END orderType, 
		   i.[Order Date], i.[Posting Date], 
		   CAST(CAST(t.[Ship Date] AS VARCHAR(11)) AS DATETIME) [Ship Date], 
		   MAX(l.[Description]) [Description], o.shipMethod, 
		   CAST(SUM(distinct l.[Unit Price]) AS DECIMAL(10, 2)) chargedInNAV, 
		   CAST(ISNULL(o.shipamount, 0) AS DECIMAL(10, 2)) estimatedFromWeb, 
		   CAST(SUM(ISNULL(t.shippingCost, 0)) AS DECIMAL(10, 2)) BilledFromUPS, 
		   MAX(CAST(ISNULL(t.enteredWeight, 0) AS DECIMAL(10, 2))) enteredWeight, 
		   MAX(CAST(ISNULL(t.billedWeight, 0) AS DECIMAL(10, 2))) billedWeight,
		   COUNT(t.[Tracking Number]) numOfPackage
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] i 
			JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] l ON i.[No_] = l.[Document No_]
			LEFT JOIN order_hdr o ON o.customerpo = i.[Web Order No_] COLLATE Latin1_General_CS_AS
			LEFT JOIN trackingInfo t ON t.KatomOrderID = i.[Order No_] COLLATE Latin1_General_CS_AS
	WHERE l.[No_] = '420'
		AND DATEDIFF(DAY, i.[Order Date], GETDATE()) < 91 
	GROUP BY i.[No_], i.[Web Order No_], i.[Order Date], i.[Posting Date], CAST(CAST(t.[Ship Date] AS VARCHAR(11)) AS DATETIME), 
		o.shipMethod, o.shipamount 
	ORDER BY 5 DESC
END
GO
