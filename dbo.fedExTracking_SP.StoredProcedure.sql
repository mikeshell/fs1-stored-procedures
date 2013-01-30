USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[fedExTracking_SP]    Script Date: 01/30/2013 16:26:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[fedExTracking_SP]
AS
BEGIN
	DECLARE @jobID INT
	INSERT INTO jobHistory(jobName, startdT) values('FEDEXTracking', GETDATE())
	SET @jobID = @@identity

	DECLARE @trackingNum VARCHAR(100), @Delivered BIT, @DeliveredDT DATETIME, @Company VARCHAR(100)
	DECLARE @signBy VARCHAR(100), @shipDT DATETIME, @city VARCHAR(100), @state VARCHAR(100), @zip VARCHAR(100)
	DECLARE @reference VARCHAR(100)

	--DATA CLEAN UP

	DECLARE catCursor CURSOR FOR 
	SELECT [Tracking Number] FROM fedExWorktable GROUP BY [Tracking Number] HAVING COUNT(*) > 1

	OPEN catCursor
	FETCH NEXT FROM catCursor INTO @trackingNum

	WHILE @@FETCH_STATUS = 0
	   BEGIN
		DELETE FROM fedExWorktable
		WHERE [Tracking Number] = @trackingNum 
			AND id <> (select max(id) from fedExWorktable where [Tracking Number] = @trackingNum)

		FETCH NEXT FROM catCursor INTO @trackingNum
	   END

	CLOSE catCursor
	DEALLOCATE catCursor
	
	DELETE FROM FedexTracking WHERE [Ship Date] IS NULL
	
	--UPDATING FEDEX TRACKING TABLE
	UPDATE t
	SET t.[Delivered] = w.[Delivered], 
		t.[Ship Date] = w.[Ship Date], 
		t.[Delivered Date] = w.[Delivered Date], 
		t.[Service Type] = w.[Service Type], 
		t.[Sign by] = w.[Sign by], 
		t.[Company Code] = w.[Company Code], 
		t.[Company] = w.[Company], 
		t.[Contact] = w.[Contact], 
		t.[Address 1] = w.[Address 1], 
		t.[Address 2] = w.[Address 2], 
		t.[City] = w.[City], 
		t.[State] = w.[State], 
		t.[Zip] = w.[Zip], 
		t.[Country] = w.[Country], 
		t.[Phone] = w.[Phone], 
		t.[E-Mail] = w.[E-Mail], 
		t.[Reference] = w.[Reference], 
		t.[PO Number] = w.[PO Number], 
		t.[Invoice Number] = w.[Invoice Number], 
		t.[Weight] = w.[Weight], 
		t.[Length] = w.[Length], 
		t.[Width] = w.[Width], 
		t.[Height] = w.[Height], 
		t.[Net Charge] = w.[Net Charge], 
		t.[List Charge] = w.[List Charge], 
		t.[Total Surcharge] = w.[Total Surcharge], 
		t.[Tracking Scan] = w.[Tracking Scan], 
		t.[COD Surcharge] = w.[COD Surcharge], 
		t.[Declared Value Surcharge] = w.[Declared Value Surcharge], 
		t.[Fuel Surcharge] = w.[Fuel Surcharge], 
		t.[Delivery Area Surcharge] = w.[Delivery Area Surcharge], 
		t.[Residential Surcharge] = w.[Residential Surcharge], 
		t.[Saturday Delivery Surcharge] = w.[Saturday Delivery Surcharge], 
		t.[Signature Surcharge] = w.[Signature Surcharge], 
		t.[Last Scan Code] = w.[Last Scan Code], 
		t.[Delivery Attempts] = w.[Delivery Attempts], 
		t.[lastUpdateDT] = GETDATE(),  
		t.[emailUpdate] =		
		CASE 
			WHEN t.[Delivered] = 'False' AND w.[Delivered] = 'True' THEN 1
			WHEN t.[Tracking Scan] like '%On FedEx vehicle for delivery%' THEN 1
			ELSE 0
		END
	FROM FedexTracking t JOIN fedExWorktable w ON t.[Tracking Number] = w.[Tracking Number]
	WHERE t.[Tracking Scan] <> w.[Tracking Scan]
		and t.[Tracking Scan] <> w.[Tracking Scan]
		
	UPDATE t
	SET t.[Reference] = w.[Reference],
		t.[PO Number] = w.[PO Number]
	FROM FedexTracking t JOIN fedExWorktable w ON t.[Tracking Number] = w.[Tracking Number]
	WHERE (t.[Reference] <> w.[Reference] OR t.[PO Number] = w.[PO Number])
			AND t.katomOrderId is NULL

	--INSERT NEW TRACKING RECORD
	INSERT INTO FedexTracking ([Tracking Number], [Delivered], [Ship Date], [Delivered Date], [Service Type], 
	[Sign by], [Company Code], [Company], [Contact], [Address 1], [Address 2], [City], [State], 
	[Zip], [Country], [Phone], [E-Mail], [Reference], [PO Number], [Invoice Number], [Weight], 
	[Length], [Width], [Height], [Net Charge], [List Charge], [Total Surcharge], [Tracking Scan], 
	[COD Surcharge], [Declared Value Surcharge], [Fuel Surcharge], [Delivery Area Surcharge], 
	[Residential Surcharge], [Saturday Delivery Surcharge], [Signature Surcharge], [Last Scan Code], 
	[Delivery Attempts], [lastUpdateDT], [stopEmailRequest], [emailUpdate])
	SELECT [Tracking Number], [Delivered], [Ship Date], [Delivered Date], [Service Type], 
	[Sign by], [Company Code], [Company], [Contact], [Address 1], [Address 2], [City], [State], 
	[Zip], [Country], [Phone], [E-Mail], [Reference], [PO Number], [Invoice Number], [Weight], 
	[Length], [Width], [Height], [Net Charge], [List Charge], [Total Surcharge], [Tracking Scan], 
	[COD Surcharge], [Declared Value Surcharge], [Fuel Surcharge], [Delivery Area Surcharge], 
	[Residential Surcharge], [Saturday Delivery Surcharge], [Signature Surcharge], [Last Scan Code], 
	[Delivery Attempts], GETDATE(), 0, 1
	FROM fedExWorktable
	WHERE [Tracking Number] NOT IN (SELECT [Tracking Number] FROM FedexTracking)

	--MATCHING WITH ORDER ID FROM NAVISION
	UPDATE f
	SET f.KatomOrderID = i.[Order No_],
		f.KatomInvoiceID = i.[No_],
		f.[lastUpdateDT] = GETDATE(),
		f.[emailUpdate] = 1	
	FROM FedexTracking f JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] i 
		ON f.[Reference] = i.[No_] COLLATE Latin1_General_CS_AS
	WHERE f.KatomOrderID IS NULL and toKatom = 0

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
		ON f.[Reference] = i.[Order No_] COLLATE Latin1_General_CS_AS
				AND LEFT(f.[Zip], 5) = LEFT(i.[Ship-to Post Code], 5) COLLATE Latin1_General_CS_AS
	WHERE f.KatomOrderID IS NULL and toKatom = 0

	UPDATE f
	SET f.KatomOrderID = P.[Sales Order No_],
		f.KatomInvoiceID = i.[No_],
		f.[lastUpdateDT] = GETDATE(),
		f.[emailUpdate] = 1	
	FROM FedexTracking f 
			JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Purch_ Inv_ Header] p 
				ON f.[PO Number]= p.[Order No_] COLLATE Latin1_General_CS_AS
							AND LEFT(f.[Company], 5) = LEFT(p.[Ship-to Name], 5) COLLATE Latin1_General_CS_AS
			JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] i
				ON P.[Sales Order No_] = i.[Order No_]
	WHERE f.KatomOrderID IS NULL AND LEN(P.[Sales Order No_]) > 0 and toKatom = 0

	UPDATE f
	SET f.KatomOrderID = i.[Order No_],
		f.KatomInvoiceID = i.[No_],
		f.[lastUpdateDT] = GETDATE(),
		f.[emailUpdate] = 1	
	FROM FedexTracking f JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] i 
		ON f.[Reference] = i.[Order No_] COLLATE Latin1_General_CS_AS
				AND LEFT(f.[Company], 5) = LEFT(i.[Ship-to Name], 5) COLLATE Latin1_General_CS_AS
	WHERE f.KatomOrderID IS NULL and toKatom = 0
			
	UPDATE f
	SET f.KatomOrderID = i.[Order No_],
		f.KatomInvoiceID = i.[No_],
		f.[lastUpdateDT] = GETDATE(),
		f.[emailUpdate] = 1	
	FROM FedexTracking f 
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] i
			ON f.[PO Number] = i.[Order No_] Collate Latin1_General_CS_AS
				AND LEFT(f.[Zip], 5) = LEFT(i.[Ship-to Post Code], 5) COLLATE Latin1_General_CS_AS
	WHERE f.KatomOrderID IS NULL and toKatom = 0
	
	UPDATE f
	SET f.KatomOrderID = P.[Sales Order No_],
		f.KatomInvoiceID = i.[No_],
		f.[lastUpdateDT] = GETDATE(),
		f.[emailUpdate] = 1	
	FROM FedexTracking f 
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Purchase Header] p 
			ON f.[PO Number]= p.[No_] COLLATE Latin1_General_CS_AS
					AND LEFT(f.[Zip], 5) = LEFT(p.[Ship-to Post Code], 5) COLLATE Latin1_General_CS_AS
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] i
			ON P.[Sales Order No_] = i.[Order No_]
	WHERE f.KatomOrderID IS NULL AND LEN(P.[Sales Order No_]) > 0 and toKatom = 0
	
	UPDATE f
	SET f.KatomOrderID = P.[Sales Order No_],
		f.KatomInvoiceID = i.[No_],
		f.[lastUpdateDT] = GETDATE(),
		f.[emailUpdate] = 1	
	FROM FedexTracking f 
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Purchase Header] p 
			ON f.[Reference]= p.[No_] COLLATE Latin1_General_CS_AS
					AND LEFT(f.[Zip], 5) = LEFT(p.[Ship-to Post Code], 5) COLLATE Latin1_General_CS_AS
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] i
			ON P.[Sales Order No_] = i.[Order No_]
	WHERE f.KatomOrderID IS NULL AND LEN(P.[Sales Order No_]) > 0 and toKatom = 0
	
	UPDATE f
	SET f.KatomOrderID = i.[Order No_],
		f.KatomInvoiceID = i.[No_],
		f.[lastUpdateDT] = GETDATE(),
		f.[emailUpdate] = 1
	FROM fedexTracking f join fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] i
			ON i.[Order No_] = RIGHT(Reference, 6) COLLATE SQL_Latin1_General_CP1_CI_AS
					AND LEFT(f.[Zip], 5) = LEFT(i.[Ship-to Post Code], 5) COLLATE Latin1_General_CS_AS				
	WHERE ISNUMERIC(RIGHT(reference, 6)) = 1 
			AND CHARINDEX(' ', Reference) > 0
			and katomorderid is null and toKatom = 0
	
	UPDATE f
	SET f.KatomOrderID = P.[Sales Order No_],
		f.KatomInvoiceID = i.[No_],
		f.[lastUpdateDT] = GETDATE(),
		f.[emailUpdate] = 1	
	FROM fedexTracking f JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Purch_ Inv_ Header] p 
				ON RIGHT(Reference, 6)= p.[Order No_] COLLATE Latin1_General_CS_AS
							AND LEFT(f.[Zip], 5) = LEFT(p.[Ship-to Post Code], 5) COLLATE Latin1_General_CS_AS
			JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] i
				ON P.[Sales Order No_] = i.[Order No_]							
	WHERE ISNUMERIC(RIGHT(reference, 6)) = 1 
			AND CHARINDEX(' ', Reference) > 0
			and katomorderid is null and toKatom = 0
	
	UPDATE f
	SET f.KatomOrderID = P.[Sales Order No_],
		f.KatomInvoiceID = i.[No_],
		f.[lastUpdateDT] = GETDATE(),
		f.[emailUpdate] = 1	
	FROM fedexTracking f JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Purchase Header] p
				ON RIGHT(Reference, 6)= p.[No_] COLLATE Latin1_General_CS_AS
							AND LEFT(f.[Zip], 5) = LEFT(p.[Ship-to Post Code], 5) COLLATE Latin1_General_CS_AS
			JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] i
				ON P.[Sales Order No_] = i.[Order No_]							
	WHERE ISNUMERIC(RIGHT(reference, 6)) = 1 
			AND CHARINDEX(' ', Reference) > 0
			and katomorderid is null and toKatom = 0
	truncate table fedExWorktable
	
	UPDATE f
	SET f.KatomOrderID = P.[Sales Order No_],
		f.KatomInvoiceID = i.[No_],
		f.[lastUpdateDT] = GETDATE(),
		f.[emailUpdate] = 1	
	FROM fedexTracking f JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Purch_ Inv_ Header] p 
		ON p.[Order No_] = REPLACE(f.Reference, 'PO#', '') COLLATE SQL_Latin1_General_CP1_CI_AS
				AND LEFT(f.[Zip], 5) = LEFT(p.[Ship-to Post Code], 5) COLLATE Latin1_General_CS_AS
			JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] i
				ON P.[Sales Order No_] = i.[Order No_]	
	WHERE ISNUMERIC(RIGHT(reference, 6)) = 1 
			AND CHARINDEX('PO#', Reference) > 0
			AND f.KatomOrderID IS NULL AND toKatom = 0	
	
	UPDATE f
	SET f.KatomOrderID = P.[Sales Order No_],
		f.KatomInvoiceID = i.[No_],
		f.[lastUpdateDT] = GETDATE(),
		f.[emailUpdate] = 1	
	FROM fedexTracking f JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Purchase Header] p 
		ON p.[No_] = REPLACE(f.Reference, 'PO#', '') COLLATE SQL_Latin1_General_CP1_CI_AS
				AND LEFT(f.[Zip], 5) = LEFT(p.[Ship-to Post Code], 5) COLLATE Latin1_General_CS_AS
			JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] i
				ON P.[Sales Order No_] = i.[Order No_]	
	WHERE ISNUMERIC(RIGHT(reference, 6)) = 1 
			AND CHARINDEX('PO#', Reference) > 0
			AND f.KatomOrderID IS NULL AND toKatom = 0			
		
		
	UPDATE jobHistory SET endDT = GETDATE() WHERE jobID = @jobID

END
GO
