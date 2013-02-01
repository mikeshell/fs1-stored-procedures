USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[trackingRPT_SP]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[trackingRPT_SP]
AS
BEGIN

truncate table trackingRpt

--COUNTING THE NUMBER OF PO OVER THE LAST 30 DAYS
INSERT INTO trackingRpt(vendorID, vendor, numPO)
SELECT [Buy-from Vendor No_], [Buy-from Vendor Name], count(*)
FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Purch_ Inv_ Header]
WHERE [Ship-to Name] NOT IN ('WAREHOUSE FACILITY', 'B & B Equipment & Supply, Inc.')
	AND DATEDIFF(DAY, [Order Date], GETDATE()) BETWEEN 3 AND 30 AND LEN([Buy-from Vendor No_]) = 3
GROUP BY [Buy-from Vendor No_], [Buy-from Vendor Name]

truncate table trackingRptWorktable

INSERT INTO trackingRptWorktable(vendorID, vendor, numPO)
SELECT [Buy-from Vendor No_], [Buy-from Vendor Name], count(*) 			
FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Purchase Header]
WHERE [Ship-to Name] NOT IN ('WAREHOUSE FACILITY', 'B & B Equipment & Supply, Inc.')
	AND DATEDIFF(DAY, [Order Date], GETDATE()) BETWEEN 3 AND 30 AND LEN([Buy-from Vendor No_]) = 3
GROUP BY [Buy-from Vendor No_], [Buy-from Vendor Name]

UPDATE t
SET t.numPO = t.numPO + w.numPO
FROM trackingRpt t JOIN trackingRptWorktable w ON t.vendorID = w.vendorID

INSERT INTO trackingRpt(vendorID, vendor, numPO)
SELECT * FROM trackingRptWorktable WHERE vendorID NOT IN (SELECT vendorID FROM trackingRpt)

--COUNTING THE NUMBER OF TRACKING IN THE UPSWORLDSHIP TABLE THAT IS NOT SHIPPING FROM HERE 
truncate table trackingRptWorktable

INSERT INTO trackingRptWorktable(vendorID, numPO)
SELECT m.mfgid, count(*)
FROM upsworldship u left join mfgupscode m on u.[shipper number] = m.upscode
WHERE u.[shipper number] <> '307417'
group by m.mfgid

UPDATE t
SET t.numTracking = w.numPO
FROM trackingRpt t JOIN trackingRptWorktable w ON t.vendorID = w.vendorID

INSERT INTO trackingRpt(vendorID, vendor, numTracking)
SELECT * FROM trackingRptWorktable WHERE vendorID NOT IN (SELECT vendorID FROM trackingRpt)

--CALCULATING THE NUMBER OF MATCHED TRACKING WITH ALL AVAILABLE TRACKING NUMBER FROM UPSWORLDSHIP TBL
truncate table trackingRptWorktable

INSERT INTO trackingRptWorktable(vendorID, numPO)
SELECT m.mfgid, count(*)
FROM upsworldship u left join mfgupscode m on u.[shipper number] = m.upscode
WHERE u.[shipper number] <> '307417' and len(isnull(u.katomOrderID, '')) > 0
group by m.mfgid

UPDATE t
SET t.[matched] = w.numPO
FROM trackingRpt t JOIN trackingRptWorktable w ON t.vendorID = w.vendorID

--CALCULATE THE NUMBER OF PO WITH TRACKING
truncate table trackingRptWorktable

INSERT INTO trackingRptWorktable(vendorID, numPO)
SELECT p.[Buy-from Vendor No_], COUNT(distinct p.[No_])
FROM trackingInfo t join fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Purch_ Inv_ Header] p
						ON t.KatomOrderID = p.[Sales Order No_] collate Latin1_General_CS_AS
					join upsWorldShip u on u.[Tracking Number] = t.[Tracking Number] 
					join mfgUPSCode m on m.upsCode = u.[shipper number] and m.mfgID = p.[Buy-from Vendor No_] collate Latin1_General_CS_AS
WHERE [Ship-to Name] NOT IN ('WAREHOUSE FACILITY', 'B & B Equipment & Supply, Inc.')
	AND DATEDIFF(DAY, [Order Date], GETDATE()) BETWEEN 3 AND 30 AND LEN([Buy-from Vendor No_]) = 3
group by p.[Buy-from Vendor No_]

UPDATE t
SET t.[missingTrackingPO] = w.numPO
FROM trackingRpt t JOIN trackingRptWorktable w ON t.vendorID = w.vendorID

truncate table trackingRptWorktable
INSERT INTO trackingRptWorktable(vendorID, numPO)
SELECT p.[Buy-from Vendor No_], COUNT(distinct p.[No_])
FROM trackingInfo t join fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Purchase Header] p
						ON t.KatomOrderID = p.[Sales Order No_] collate Latin1_General_CS_AS
					join upsWorldShip u on u.[Tracking Number] = t.[Tracking Number] 
					join mfgUPSCode m on m.upsCode = u.[shipper number]
WHERE [Ship-to Name] NOT IN ('WAREHOUSE FACILITY', 'B & B Equipment & Supply, Inc.')
	AND DATEDIFF(DAY, [Order Date], GETDATE()) BETWEEN 3 AND 30 AND LEN([Buy-from Vendor No_]) = 3
group by p.[Buy-from Vendor No_]

UPDATE t
SET t.[missingTrackingPO] = t.[missingTrackingPO] + w.numPO
FROM trackingRpt t JOIN trackingRptWorktable w ON t.vendorID = w.vendorID

truncate table trackingRptWorktable

update t
set t.[vendor] = v.[Name]
from trackingRpt t join fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Vendor] v ON t.vendorID = v.[No_] collate Latin1_General_CS_AS
WHERE LEN(ISNULL(t.[vendor], '')) = 0

update trackingRpt set vendor = 'UNKNOWN' WHERE LEN(VENDORID) = 0


END
GO
