USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[trackingInfo_SP]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[trackingInfo_SP]
AS
BEGIN
	DECLARE @jobID INT
	INSERT INTO jobHistory(jobName, startdT) values('TrackingInfo', GETDATE())
	SET @jobID = @@identity

	--INSERTING FROM LOU DAILY FEDEX FILE - PACKAGE FROM WAREHOUSE ONLY
	INSERT INTO trackinginfo([Tracking Number], [katomOrderID], [carrier], [Ship Date], [Service Type], lastupdateDT, NAVImported, sendToWEB)
	SELECT [tracking Number], orderID, 'FXGROUND', GETDATE(), 'GROUND', GETDATE(), 0, 0
	FROM fedExFile
	WHERE [tracking Number] NOT IN (SELECT [Tracking Number] FROM trackinginfo)

	--INSERTING FROM ADAM INTRANET SYSTEM - FREIGHT AND TYPICALLY THIRD-PARTY
	INSERT INTO trackinginfo([Tracking Number], [katomOrderID], [carrier], [Ship Date], [Service Type], lastupdateDT, NAVImported, sendToWEB)
	SELECT  UPPER(TrackingNo), OrderNo, ShippingAgentCode, GETDATE(), ShipMethodCode, GETDATE(), 0, 1
	FROM ShipTracking.dbo.Tracking
	WHERE UPPER(TrackingNo) NOT IN (SELECT [Tracking Number] FROM trackinginfo)

	INSERT INTO ShipTracking.dbo.TrackingArchive
	SELECT * FROM ShipTracking.dbo.Tracking

	TRUNCATE TABLE ShipTracking.dbo.Tracking
	
	UPDATE trackinginfo
	SET [Service Type] = 'Ground'
	WHERE carrier = 'USMAIL'

	--INSERTING FROM FEDEX INSIGHT FILE
	UPDATE t
	SET t.[katomInvoiceID] = f.[katomInvoiceID], 
		t.[Delivered] = f.[Delivered], 
		t.[Ship Date] = f.[Ship Date], 
		t.[Delivered Date] = f.[Delivered Date], 
		t.[Service Type] = f.[Service Type], 
		t.[Sign by] = f.[Sign by], 
		t.[Company] = f.[Company], 
		t.[Contact] = f.[Contact], 
		t.[Address 1] = f.[Address 1], 
		t.[Address 2] = f.[Address 2], 
		t.[City] = f.[City], 
		t.[State] = f.[State], 
		t.[Zip] = f.[Zip], 
		t.[Country] = f.[Country], 
		t.[Phone] = f.[Phone], 
		t.[E-Mail] = f.[E-Mail], 
		t.[Tracking Scan] = f.[Tracking Scan], 
		t.[lastUpdateDT] = f.[lastUpdateDT], 
		t.[sendToWEB] = 1
	FROM trackinginfo t JOIN fedexTracking f on t.[Tracking Number] = f.[Tracking Number]
	WHERE t.[carrier] = 'FXGROUND' 
		AND (DATEDIFF(DAY, f.[lastUpdateDT], GETDATE()) = 0 OR t.[Zip] IS NULL)

	UPDATE t
	SET t.[KatomOrderID] = f.[KatomOrderID], 
		t.[lastUpdateDT] = GETDATE(), 
		t.[sendToWEB] = 1
	FROM trackinginfo t JOIN fedexTracking f on t.[Tracking Number] = f.[Tracking Number]
	WHERE t.[carrier] = 'FXGROUND' AND t.[KatomOrderID] IS NULL

	INSERT INTO trackinginfo ([Tracking Number], [katomOrderID], [katominvoiceid], [carrier], delivered, [Ship Date], [Delivered Date], [Service Type], [Sign by], company, contact, [Address 1], [Address 2], [City], [State], [Zip], [Country], [Phone], [E-Mail], [Tracking Scan], lastupdateDT, NAVImported, sendToWEB)
	SELECT [Tracking Number], [KatomOrderID], [katomInvoiceID], 'FXGROUND', [Delivered], [Ship Date], [Delivered Date], [Service Type], [Sign by], [Company], [Contact], [Address 1], [Address 2], [City], [State], [Zip], [Country], [Phone], [E-Mail], [Tracking Scan], [lastUpdateDT], 0, 1
	FROM fedexTracking
	WHERE [tracking Number] NOT IN (SELECT [Tracking Number] FROM trackinginfo)
		AND KatomOrderID IS NOT NULL

	--DONT IMPORT IF THE TRACKING INFO IS ALREADY IN NAV
	UPDATE t
	SET t.NAVImported = 1
	from fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] i
		JOIN trackinginfo t on t.[katomOrderID] = i.[Order No_] COLLATE Latin1_General_CS_AS
							AND t.[Tracking Number] = i.[Package Tracking No_] COLLATE Latin1_General_CS_AS
	WHERE LEFT([No_], 1) = 'I' AND LEN([Package Tracking No_]) > 0 AND t.NAVImported = 0

	--IMPORT FROM UPS TABLE FROM KATOM WAREHOUSE	
	delete from UPSTracking
	where TrackingNumber in (
	select TrackingNumber from UPSTracking
	group by TrackingNumber
	having COUNT(*) > 1)

	INSERT INTO trackingInfo([Tracking Number], katomOrderID, carrier, Delivered, [Ship Date], 
								[Service Type], lastUpdateDT, NAVImported, SendToWeB)
	select TrackingNumber, Reference1, 'UPS', 'FALSE', UPDATEDT, 'UPS', GETDATE(), 0, 1
	from UPSTracking
	where TrackingNumber not in (SELECT [Tracking Number] FROM trackingInfo)
	and [Reference1] is not null
	and CHARINDEX('#', reference1) = 0
	and LEN(reference1) < 11

	DELETE FROM trackinginfo WHERE ISNUMERIC(katomorderid) = 0 AND CHARINDEX('-', katomorderid) = 0
	
	-- BEGIN --
	--INSERTING ITEM SHIPPED FOR ALL ORDERS SHIPPED FROM KATOM - ONLY STORING 60 DAYS WORTH OF DATA
	INSERT INTO trackingInfo_line_worktable
	SELECT h.[Order No_], l.[Document No_], l.[No_], l.[Description], CAST(l.Quantity AS DECIMAL(10, 0)), u.TrackingNumber  
	FROM upsTracking u join fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] h 
					on u.[Reference1] = h.[Order No_] COLLATE Latin1_General_CS_AS
		join fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] l on h.[No_] = l.[Document No_]
	WHERE l.[No_] NOT LIKE '100-%'
		AND l.[No_] NOT IN ('205','420','505','506','520','720')
		AND LEN(l.[No_]) > 0
		AND l.Quantity > 0
		AND DATEDIFF(DAY, h.[Posting Date], GETDATE()) < 60
	
	--INSERTING ITEM SHIPPED FOR ALL ORDERS FOR SHIP DIRECT FOR OPEN PO
	INSERT INTO trackingInfo_line_worktable
	SELECT ph.[Sales Order No_],
		ih.[No_],
		pl.[No_] prodNum,
		pl.[Description],
		CASE pl.[Quantity Invoiced]
			WHEN 0 THEN CAST(pl.[Quantity] AS DECIMAL(10, 0))
			ELSE CAST(pl.[Quantity Invoiced] AS DECIMAL(10, 0))
		END qtyShipped,
		u.[tracking Number]
	FROM upsWorldShip u JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Purchase Header] ph
					ON ISNULL(u.katomPONum, '') = ph.[No_] COLLATE Latin1_General_CS_AS
			JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Purchase Line] pl ON pl.[Document No_] = ph.[No_]
			JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] ih ON ph.[Sales Order No_] = ih.[Order No_]
	WHERE ph.[Ship-to Name] NOT IN ('B & B Equipment & Supply, Inc.', 'WAREHOUSE FACILITY', 'WAREHOUSER FACILITY', 'PILOT')	
			AND LEN(ph.[Sales Order No_]) > 0
			AND DATEDIFF(DAY, ih.[Posting Date], GETDATE()) < 60
			AND LEN(pl.[No_]) > 0
	
	--INSERTING ITEM SHIPPED FOR ALL ORDERS FOR SHIP DIRECT FOR PAID PO		
	INSERT INTO trackingInfo_line_worktable
	SELECT ph.[Sales Order No_],
		ih.[No_],
		pl.[No_] prodNum,
		pl.[Description],
		CAST(pl.[Quantity] AS DECIMAL(10, 0)) qtyShipped,
		u.[tracking Number]
	FROM upsWorldShip u JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Purch_ Inv_ Header] ph
					ON ISNULL(u.katomPONum, '') = ph.[Order No_] COLLATE Latin1_General_CS_AS
			JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Purch_ Inv_ Line] pl ON pl.[Document No_] = ph.[No_]
			JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] ih ON ph.[Sales Order No_] = ih.[Order No_]
	WHERE ph.[Ship-to Name] NOT IN ('B & B Equipment & Supply, Inc.', 'WAREHOUSE FACILITY', 'WAREHOUSER FACILITY', 'PILOT')	
			AND LEN(ph.[Sales Order No_]) > 0
			AND DATEDIFF(DAY, ih.[Posting Date], GETDATE()) < 60
			AND LEN(pl.[No_]) > 0
			AND pl.Quantity > 0

	DELETE w
	FROM trackingInfo_line_worktable w JOIN trackingInfo_line l 
			ON w.OrderNo = l.OrderNo AND w.InvoiceNo = l.InvoiceNo AND w.ItemNo = l.ItemNo AND w.trackingNo = l.trackingNo

	INSERT INTO trackingInfo_line (orderNo, InvoiceNo, ItemNo, itemDesc, qtyShipped, trackingNo, updatedt)
	SELECT *, GETDATE() FROM trackingInfo_line_worktable

	TRUNCATE TABLE trackingInfo_line_worktable
	DELETE trackingInfo_line WHERE DATEDIFF(DAY, updateDT, GETDATE()) > 61
	
	--RESET NAVIMPORTED FLAG TO 0 IF TRACKING NUMBER IS NOT PRESENT IN NAV INVOICE TABLE
	UPDATE u
	SET u.NAVImported = 0, lastUpdateDT = GETDATE()
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] i
		LEFT JOIN trackingInfo u ON u.katomorderid = i.[Order No_] COLLATE Latin1_General_CS_AS
	WHERE LEN([Package Tracking No_]) = 0
		AND DATEDIFF(day, [Posting Date], getdate()) < 91
		AND NAVImported = 1

	UPDATE trackingInfo
	SET NAVImported = 1
	WHERE [Tracking Number] IN
		(
		SELECT [Package Tracking No_] COLLATE Latin1_General_CS_AS
		FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line]
		WHERE LEN([Package Tracking No_]) > 0
		) AND NAVImported = 0
		
	--REMOVE TRACKING DETAIL FOR ALL PILOT ORDERS (THEY DONT GET TRACKING)
	DELETE t
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] i	
			join trackingInfo_line t on i.[Order No_] = t.orderNo COLLATE SQL_Latin1_General_CP1_CI_AS
	WHERE [Bill-to Name] = 'PILOT'
	
	exec dbo.[ups_tracking]
	
	--REMOVE ALL INVALID KATOMORDERID
	DELETE FROM trackingInfo 
	WHERE ISNUMERIC(katomorderid) = 0 AND CHARINDEX('-', katomorderid) = 0
		
	--UPDATE TABLE TO INCLUDE WEB ORDER ID
	Update trackinginfo
	set weborderid = (select top 1 webnum = CASE [Web Order No_]
										WHEN '' THEN 'NA'
										ELSE [Web Order No_]
									  END
						from invoice_backup
						where katomorderid COLLATE Latin1_General_CS_AS = [Order No_]COLLATE Latin1_General_CS_AS
						),
		zip = (	select top 1 [ship-to post code]
						from invoice_backup
						where katomorderid COLLATE Latin1_General_CS_AS = [Order No_]COLLATE Latin1_General_CS_AS)
	where weborderid = '0' or weborderid is null or weborderid = ''
	
	--DELETING ALL TRACKING THAT IS OVER 90 DAYS		
	DELETE FROM trackinginfo WHERE DATEDIFF(DAY, ISNULL([Delivered Date], GETDATE()), GETDATE()) > 90
	DELETE FROM trackinginfo WHERE DATEDIFF(DAY, ISNULL(lastupdateDT, GETDATE()), GETDATE()) > 90

	--ARCHIVING FEDEXFILE TABLE OVER 45 DAYS
	INSERT INTO fedExFileArchive
	SELECT * FROM fedExFile WHERE DATEDIFF(DAY, ISNULL([timeStamp], GETDATE()), GETDATE()) > 45
	DELETE FROM fedExFile WHERE DATEDIFF(DAY, ISNULL([timeStamp], GETDATE()), GETDATE()) > 45

	--ARCHIVING FEDEXTRACKING TABLE OVER 45 DAYS
	INSERT INTO fedexTrackingArchive
	SELECT * FROM fedexTracking WHERE DATEDIFF(DAY, ISNULL(lastupdateDT, GETDATE()), GETDATE()) > 45
	DELETE FROM fedexTracking WHERE DATEDIFF(DAY, ISNULL(lastupdateDT, GETDATE()), GETDATE()) > 45
		
	--REMOVING TRACKING WITH BAD KATOM ORDER ID
	DELETE FROM trackinginfo
	WHERE ISNUMERIC(left(katomorderID, 2)) = 0 AND LEN(ISNULL(katomOrderID, '')) > 0
	
	DELETE FROM trackinginfo
	WHERE katomOrderID IS NULL
			AND DATEDIFF(DAY, [Ship Date], GETDATE()) > 3
	
	DELETE FROM trackinginfo
	WHERE LEN(katomOrderID) < 6
	
	EXEC DBO.[DUP_trakingNumberCleanup]

	--DELETING BAD KATOM ORDER ID TRACKING NUMBER
	DELETE FROM trackingInfo WHERE LEN(katomorderid) > 10

	DELETE from trackingInfo 
	where isnumeric(katomorderid)= 0 
			AND LEFT(katomorderid, 1) <> 'i'
			AND LEFT(RIGHT(katomorderid, 2), 1) <> '-'
	
	--UPLOAD TRACKING NUMBERS TO WEB FOR SEARCH --Added 3-4-11 MS
	--EXEC dbo.webtrackinginfo
			
	UPDATE jobHistory SET endDT = GETDATE() WHERE jobID = @jobID
END
GO
