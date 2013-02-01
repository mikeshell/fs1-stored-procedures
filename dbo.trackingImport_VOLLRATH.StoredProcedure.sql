USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[trackingImport_VOLLRATH]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[trackingImport_VOLLRATH]
AS
BEGIN
	UPDATE vollrathtracking
	SET PONumber = LTRIM(REPLACE(PONumber, 'N/CHG:', ''))
	WHERE CHARINDEX('N/CHG:', PONumber) > 0

	UPDATE v
	SET v.katomOrderID = p.[Sales Order No_]
	FROM vollrathtracking v JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Purchase Header] p
					ON v.ponumber = p.[No_] COLLATE Latin1_General_CS_AS
	WHERE LEN(ISNULL(trackingno, '')) > 0

	UPDATE v
	SET v.katomOrderID = p.[Sales Order No_]
	FROM vollrathtracking v JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Purch_ Inv_ Header] p
					ON v.ponumber = p.[Order No_] COLLATE Latin1_General_CS_AS
	WHERE LEN(ISNULL(trackingno, '')) > 0

	UPDATE f
	SET [PO Number] = v.PONumber
	FROM vollrathtracking v JOIN fedexTracking f ON v.trackingNo = f.[Tracking Number] 	
	WHERE LEN(ISNULL(trackingno, '')) > 0
		AND f.katomorderID IS NULL
		
	INSERT INTO trackinginfo([Tracking Number], [katomOrderID], [carrier], [Ship Date], [Service Type], lastupdateDT, NAVImported, sendToWEB)	
	select trackingNo, katomorderID, carrier, GETDATE()-1, 'GROUND', GETDATE(), 1, 0
	from vollrathtracking
	WHERE LEN(ISNULL(trackingno, '')) > 0
		AND LEN(ISNULL(katomOrderID, '')) > 0
		AND trackingNo NOT IN (SELECT [Tracking Number] FROM trackinginfo)
	
	--UPDATE THE NEW MATCHED FEDEX NUM WITH NAV DATA	
	UPDATE f
	SET f.KatomOrderID = P.[Sales Order No_],
		f.KatomInvoiceID = i.[No_],
		f.[lastUpdateDT] = GETDATE(),
		f.[emailUpdate] = 1	
	FROM FedexTracking f 
			JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Purch_ Inv_ Header] p 
				ON f.[PO Number]= p.[Order No_] COLLATE Latin1_General_CS_AS
							AND LEFT(f.[Zip], 5) = LEFT(p.[Ship-to Post Code], 5) COLLATE Latin1_General_CS_AS
			JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] i
				ON P.[Sales Order No_] = i.[Order No_]
	WHERE f.KatomOrderID IS NULL AND LEN(P.[Sales Order No_]) > 0 and toKatom = 0

	UPDATE f
	SET f.KatomOrderID = i.[Order No_],
		f.KatomInvoiceID = i.[No_],
		f.[lastUpdateDT] = GETDATE(),
		f.[emailUpdate] = 1	
	FROM FedexTracking f JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] i 
		ON f.[PO Number] = i.[Order No_] COLLATE Latin1_General_CS_AS
				AND LEFT(f.[Zip], 5) = LEFT(i.[Ship-to Post Code], 5) COLLATE Latin1_General_CS_AS
	WHERE f.KatomOrderID IS NULL and toKatom = 0
		
END
GO
