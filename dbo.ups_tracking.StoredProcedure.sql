USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[ups_tracking]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[ups_tracking]
AS
BEGIN
	DELETE FROM upsFile WHERE [Subscriber ID] = 'Subscriber ID'

	INSERT INTO upsworldship
	SELECT 
		[Tracking Number], [Query Begin Date], [Query End Date], [Record Type], [Shipper Number], [Shipper Name], 
		[Shipper City], [Shipper State Province], [Shipper Postal Code], [Shipper Country], [Ship To Attention], 
		[Ship To Phone], [Ship To Name], [Ship To Address Line 1], [Ship To Address Line 2], 
		[Ship To Address Line 3], [Ship To City], [Ship To State Province], [Ship To Postal Code], 
		[Ship To Country], 
		CASE 
			WHEN LEN([Shipment Reference Number Value 1]) > 0 THEN [Shipment Reference Number Value 1]
			ELSE [Package Reference Number Value 1]
		END	[Shipment Reference Number Value 1], 
		CASE 
			WHEN LEN([Shipment Reference Number Value 2]) > 0 THEN [Shipment Reference Number Value 2]
			ELSE [Package Reference Number Value 2]
		END	[Shipment Reference Number Value 2],
		[UPS Service], [Pickup Date], [Scheduled Delivery Date], [Scheduled Delivery Time], [Document Type], [Package Activity Date], 
		[Package Activity Time], [Package Count], [Package Dimensions Unit of Measurement], [Length], [Width], [Height], 
		[Package Dimensional Weight], [Package Weight], [Large Package], [Earliest Delivery Time], [Hold For Pickup], 
		[Saturday Delivery Indicator],[Special Instructions], [UPS Location], [UPS Location State Province], 
		[UPS Location Country], [Updated Ship To Name], [Updated Ship To Street Number], [Updated Ship To Street Prefix], 
		[Updated Ship To Street Name], [Updated Ship To Street Type], [Updated Ship To Street Suffix], [Updated Ship To Building Name], 
		[Updated Ship To Room Suite Floor], [Updated Ship To Political Division 3], [Updated Ship To City], [Updated Ship To State Province], 
		[Updated Ship To Country], [Updated Ship To Postal Code], [Exception Status Description], [Exception Reason Description], 
		[Exception Resolution Type], [Exception Resolution Description], [Rescheduled Delivery Date], [Rescheduled Delivery Time], 
		[Driver Release], [Delivery Location], [Delivery Name], [Delivery Street Number], [Delivery Street Prefix], [Delivery Street Name], 
		[Delivery Street Type], [Delivery Street Suffix], [Delivery Building Name], [Delivery Room Suite Floor], [Delivery Political Division 3], 
		[Delivery City], [Delivery State Province], [Delivery Country], [Delivery Postal Code], [Residential Address], [Signed For By],		
		CASE
			WHEN LEN([Shipment Reference Number Value 1]) > 0 
					THEN dbo.[tracking_orderNo]([Shipper Number], [Shipment Reference Number Value 1], [Shipment Reference Number Value 2])
			ELSE dbo.[tracking_orderNo]([Shipper Number], [Package Reference Number Value 1], [Package Reference Number Value 2])
		END,
		NULL, GETDATE(), [Driver Release], [Signed For By], 0,  
		CASE [Subscription Name]
			WHEN 'Outbound_Katom' THEN ''
			ELSE 
				CASE
					WHEN LEN([Shipment Reference Number Value 1]) > 0 
							THEN dbo.[tracking_PONo]([Shipper Number], [Shipment Reference Number Value 1], [Shipment Reference Number Value 2])
					ELSE dbo.[tracking_PONo]([Shipper Number], [Package Reference Number Value 1], [Package Reference Number Value 2])
				END			
		END
	FROM upsFile
	WHERE [record type] in ('M1', 'M2')
		AND [Tracking Number] NOT IN (SELECT [Tracking Number] FROM upsworldship)
				
	--REMOVE DUPLICATE
	DECLARE @ID int, @trackingNum varchar(100), @trackingNum2 varchar(100)

	SET @trackingNum2 = 'DAVIDLU'

	DECLARE catCursor CURSOR FOR 
	SELECT ID, [Tracking Number]
	FROM upsworldship
	WHERE [Tracking Number] IN 
		(SELECT [Tracking Number]
		FROM upsworldship
		GROUP BY [Tracking Number]
		HAVING COUNT(*) >1)
	ORDER BY [Tracking Number], id

	OPEN catCursor
	FETCH NEXT FROM catCursor INTO @ID, @trackingNum

	WHILE @@FETCH_STATUS = 0
	   BEGIN
		IF @trackingNum2 = @trackingNum
		   BEGIN
			DELETE FROM upsworldship WHERE id = @ID
		   END   
		ELSE 
		  BEGIN
			SET @trackingNum2 = @trackingNum
		  END

		FETCH NEXT FROM catCursor INTO @ID, @trackingNum
	END

	CLOSE catCursor
	DEALLOCATE catCursor

	--REMOVE ALL THE NEW INSERTED RECORDS FROM UPSFILE TBL
	DELETE FROM upsfile 
	WHERE [record type] IN ('M1', 'M2') 
			AND [Tracking Number] IN (SELECT [Tracking Number] FROM upsworldship WHERE DATEDIFF(DAY, GETDATE(), UpdateDT) = 0) 

	--UPDATE CURRENT TRACKING INFO
	DECLARE catCursor CURSOR FOR 
	SELECT [Tracking Number]
	FROM upsFile
	WHERE [Tracking Number] IN (SELECT [Tracking Number] FROM upsworldship WHERE [record type] NOT IN ('D1', 'D2'))
	GROUP BY [Tracking Number]

	OPEN catCursor
	FETCH NEXT FROM catCursor INTO @trackingNum

	WHILE @@FETCH_STATUS = 0
	   BEGIN
		TRUNCATE TABLE upsFileWorktable
		INSERT INTO upsFileWorktable
		SELECT TOP 1 * FROM upsFile WHERE [Tracking Number] = @trackingNum 
		ORDER BY [Package Activity Date] DESC, [Package Activity Time] DESC
		
		UPDATE w
		SET w.[Record Type]  = f.[Record Type] , 
			w.[Package Activity Date] = f.[Package Activity Date], 
			w.[Package Activity Time] = f.[Package Activity Time], 
			w.[UPS Location] = f.[UPS Location], 
			w.[UPS Location State Province] = f.[UPS Location State Province], 
			w.[UPS Location Country] = f.[UPS Location Country], 
			w.[Exception Status Description] = f.[Exception Status Description], 
			w.[Exception Resolution Description] = f.[Exception Resolution Description], 
			w.[Driver Released] = f.[Driver Release], 
			w.[Delivery Location] = f.[Delivery Location], 
			w.[Delivery Name] = f.[Delivery Name], 
			w.[Delivery Street Number] = f.[Delivery Street Number], 
			w.[Delivery street Prefix] = f.[Delivery street Prefix], 
			w.[Delivery Street Name] = f.[Delivery Street Name], 
			w.[Delivery Street Type] = f.[Delivery Street Type], 
			w.[Delivery City] = f.[Delivery City], 
			w.[Delivery State Province] = f.[Delivery State Province], 
			w.[Delivery Country] = f.[Delivery Country], 
			w.[Delivery Postal Code] = f.[Delivery Postal Code], 
			w.[Residential Address] = f.[Residential Address], 
			w.[Sign for By] = f.[Signed For By],
			w.updateTracking = 0,
			w.updateDT = GETDATE()
		FROM upsworldship w JOIN upsFileWorktable f ON w.[Tracking Number] = f.[Tracking Number]
		WHERE w.[Tracking Number] = @trackingNum		

		FETCH NEXT FROM catCursor INTO @trackingNum
	END

	CLOSE catCursor
	DEALLOCATE catCursor


	--REMOVE UPDATED TRACKING INFO FROM UPSFILE TBL		
	DELETE FROM upsFile WHERE [Tracking Number] IN (SELECT [Tracking Number] FROM upsWorldShip)

	--INSERT NEW TRACKING INFO WHERE THERE IS ONLY 1 INSTANCE		
	INSERT INTO upsworldship
	SELECT 
		[Tracking Number], [Query Begin Date], [Query End Date], [Record Type], [Shipper Number], [Shipper Name], 
		[Shipper City], [Shipper State Province], [Shipper Postal Code], [Shipper Country], [Ship To Attention], 
		[Ship To Phone], [Ship To Name], [Ship To Address Line 1], [Ship To Address Line 2], [Ship To Address Line 3], 
		[Ship To City], [Ship To State Province], [Ship To Postal Code], [Ship To Country], 
		CASE 
			WHEN LEN([Shipment Reference Number Value 1]) > 0 THEN [Shipment Reference Number Value 1]
			ELSE [Package Reference Number Value 1]
		END	[Shipment Reference Number Value 1], 
		CASE 
			WHEN LEN([Shipment Reference Number Value 2]) > 0 THEN [Shipment Reference Number Value 2]
			ELSE [Package Reference Number Value 2]
		END	[Shipment Reference Number Value 2],
		[UPS Service], [Pickup Date], [Scheduled Delivery Date], [Scheduled Delivery Time], [Document Type], [Package Activity Date], 
		[Package Activity Time], [Package Count], [Package Dimensions Unit of Measurement], [Length], [Width], [Height], 
		[Package Dimensional Weight], [Package Weight], [Large Package], [Earliest Delivery Time], [Hold For Pickup], 
		[Saturday Delivery Indicator],[Special Instructions], [UPS Location], [UPS Location State Province], 
		[UPS Location Country], [Updated Ship To Name], [Updated Ship To Street Number], [Updated Ship To Street Prefix], 
		[Updated Ship To Street Name], [Updated Ship To Street Type], [Updated Ship To Street Suffix], [Updated Ship To Building Name], 
		[Updated Ship To Room Suite Floor], [Updated Ship To Political Division 3], [Updated Ship To City], [Updated Ship To State Province], 
		[Updated Ship To Country], [Updated Ship To Postal Code], [Exception Status Description], [Exception Reason Description], 
		[Exception Resolution Type], [Exception Resolution Description], [Rescheduled Delivery Date], [Rescheduled Delivery Time], 
		[Driver Release], [Delivery Location], [Delivery Name], [Delivery Street Number], [Delivery Street Prefix], [Delivery Street Name], 
		[Delivery Street Type], [Delivery Street Suffix], [Delivery Building Name], [Delivery Room Suite Floor], [Delivery Political Division 3], 
		[Delivery City], [Delivery State Province], [Delivery Country], [Delivery Postal Code], [Residential Address], [Signed For By],		
		CASE
			WHEN LEN([Shipment Reference Number Value 1]) > 0 
					THEN dbo.[tracking_orderNo]([Shipper Number], [Shipment Reference Number Value 1], [Shipment Reference Number Value 2])
			ELSE dbo.[tracking_orderNo]([Shipper Number], [Package Reference Number Value 1], [Package Reference Number Value 2])
		END, 
		NULL, GETDATE(), [Driver Release], [Signed For By], 0, 
		CASE [Subscription Name]
			WHEN 'Outbound_Katom' THEN ''
			ELSE 
				CASE
					WHEN LEN([Shipment Reference Number Value 1]) > 0 
							THEN dbo.[tracking_PONo]([Shipper Number], [Shipment Reference Number Value 1], [Shipment Reference Number Value 2])
					ELSE dbo.[tracking_PONo]([Shipper Number], [Package Reference Number Value 1], [Package Reference Number Value 2])
				END			
		END
	FROM upsFile
	WHERE [tracking number] IN (SELECT [tracking number] FROM upsfile GROUP BY [tracking number] HAVING COUNT(*) = 1)
			AND [Tracking Number] NOT IN (SELECT [Tracking Number] FROM upsworldship)

	DELETE FROM upsFile WHERE [Tracking Number] IN (SELECT [Tracking Number] FROM upsWorldShip)

		
	--INSERTING THE REMAINING TRACKING
	DECLARE catCursor CURSOR FOR 
	SELECT DISTINCT [Tracking Number] FROM upsFile

	OPEN catCursor
	FETCH NEXT FROM catCursor INTO @trackingNum

	WHILE @@FETCH_STATUS = 0
	   BEGIN
		INSERT INTO upsworldship
		SELECT TOP 1 
			[Tracking Number], [Query Begin Date], [Query End Date], [Record Type], [Shipper Number], [Shipper Name], 
			[Shipper City], [Shipper State Province], [Shipper Postal Code], [Shipper Country], [Ship To Attention], 
			[Ship To Phone], [Ship To Name], [Ship To Address Line 1], [Ship To Address Line 2], [Ship To Address Line 3], 
			[Ship To City], [Ship To State Province], [Ship To Postal Code], [Ship To Country], 
			CASE 
				WHEN LEN([Shipment Reference Number Value 1]) > 0 THEN [Shipment Reference Number Value 1]
				ELSE [Package Reference Number Value 1]
			END	[Shipment Reference Number Value 1], 
			CASE 
				WHEN LEN([Shipment Reference Number Value 2]) > 0 THEN [Shipment Reference Number Value 2]
				ELSE [Package Reference Number Value 2]
			END	[Shipment Reference Number Value 2],
			[UPS Service], [Pickup Date], [Scheduled Delivery Date], [Scheduled Delivery Time], [Document Type], [Package Activity Date], 
			[Package Activity Time], [Package Count], [Package Dimensions Unit of Measurement], [Length], [Width], [Height], 
			[Package Dimensional Weight], [Package Weight], [Large Package], [Earliest Delivery Time], [Hold For Pickup], 
			[Saturday Delivery Indicator],[Special Instructions], [UPS Location], [UPS Location State Province], 
			[UPS Location Country], [Updated Ship To Name], [Updated Ship To Street Number], [Updated Ship To Street Prefix], 
			[Updated Ship To Street Name], [Updated Ship To Street Type], [Updated Ship To Street Suffix], [Updated Ship To Building Name], 
			[Updated Ship To Room Suite Floor], [Updated Ship To Political Division 3], [Updated Ship To City], [Updated Ship To State Province], 
			[Updated Ship To Country], [Updated Ship To Postal Code], [Exception Status Description], [Exception Reason Description], 
			[Exception Resolution Type], [Exception Resolution Description], [Rescheduled Delivery Date], [Rescheduled Delivery Time], 
			[Driver Release], [Delivery Location], [Delivery Name], [Delivery Street Number], [Delivery Street Prefix], [Delivery Street Name], 
			[Delivery Street Type], [Delivery Street Suffix], [Delivery Building Name], [Delivery Room Suite Floor], [Delivery Political Division 3], 
			[Delivery City], [Delivery State Province], [Delivery Country], [Delivery Postal Code], [Residential Address], [Signed For By],		
			CASE
				WHEN LEN([Shipment Reference Number Value 1]) > 0 
						THEN dbo.[tracking_orderNo]([Shipper Number], [Shipment Reference Number Value 1], [Shipment Reference Number Value 2])
				ELSE dbo.[tracking_orderNo]([Shipper Number], [Package Reference Number Value 1], [Package Reference Number Value 2])
			END,
			NULL, GETDATE(), [Driver Release], [Signed For By], 0, 
			CASE [Subscription Name]
				WHEN 'Outbound_Katom' THEN ''
				ELSE 
					CASE
						WHEN LEN([Shipment Reference Number Value 1]) > 0 
								THEN dbo.[tracking_PONo]([Shipper Number], [Shipment Reference Number Value 1], [Shipment Reference Number Value 2])
						ELSE dbo.[tracking_PONo]([Shipper Number], [Package Reference Number Value 1], [Package Reference Number Value 2])
					END			
			END
		FROM upsFile
		WHERE [tracking number] = @trackingNum
		ORDER BY [Package Activity Date] DESC, [Package Activity Time] DESC
		
		FETCH NEXT FROM catCursor INTO @trackingNum
	END

	CLOSE catCursor
	DEALLOCATE catCursor

	TRUNCATE TABLE upsFile
	
	--UPDATE upsworldship
	--SET katomOrderID = dbo.tracking_orderNo([Tracking Number])
	--WHERE LEN(ISNULL(katomOrderID, '')) = 0 AND DATEDIFF(DAY, updateDT, GETDATE()) < 7
	

	--UPDATE TRACKING INFO WITH DELIVERED STATUS FROM UPSWORLDSHIP TBL
	UPDATE i
	SET i.Delivered = 'True',
		i.lastUpdatedt = getdate(),
		i.sendToWeb = 1,
		i.emailUpdate = 1
	from upsworldship u JOIN trackingInfo i on u.[tracking Number] = i.[tracking Number]
	where i.[carrier] = 'ups' and ISNULL(i.delivered, 'false') = 'false'
		and u.[Record Type] in ('D1', 'D2') and u.updateTracking = 0
		
	UPDATE u
	SET u.updateTracking = 1
	from upsworldship u JOIN trackingInfo i on u.[tracking Number] = i.[tracking Number]
	where i.[carrier] = 'ups' and u.[Record Type] in ('D1', 'D2') and u.updateTracking = 0

	UPDATE i
	SET i.Delivered = 'True',
		i.lastUpdatedt = getdate(),
		i.sendToWeb = 1,
		i.emailUpdate = 1
	from upsworldship u JOIN trackingInfo i on u.[tracking Number] = i.[tracking Number]
	where i.[carrier] = 'ups' and [Driver Released] = 'DRIVER RELEASED' and u.updateTracking = 0
		
	UPDATE u
	SET u.updateTracking = 1
	from upsworldship u JOIN trackingInfo i on u.[tracking Number] = i.[tracking Number]
	where i.[carrier] = 'ups' and [Driver Released] = 'DRIVER RELEASED' and u.updateTracking = 0

	UPDATE i
	SET i.Delivered = 'True',
		i.lastUpdatedt = getdate(),
		i.sendToWeb = 1,
		i.emailUpdate = 1
	from upsworldship u JOIN trackingInfo i on u.[tracking Number] = i.[tracking Number]
	where LEN(ISNULL([Sign For By], '')) > 0 AND updateTracking = 0
		
	UPDATE u
	SET u.updateTracking = 1
	from upsworldship u JOIN trackingInfo i on u.[tracking Number] = i.[tracking Number]
	where LEN(ISNULL([Sign For By], '')) > 0 AND updateTracking = 0
	
	UPDATE upsworldship
	SET katomorderid = NULL
	WHERE ISNUMERIC(katomorderid) = 0 AND CHARINDEX('-', katomorderid) = 0

	INSERT INTO trackingInfo
	SELECT [Tracking Number], katomOrderID, NULL, 'UPS', 
		CASE
			WHEN [Record Type] = 'D1' THEN 'True'
			WHEN [Record Type] = 'D2' THEN 'True'
			WHEN [Driver Released] = 'DRIVER RELEASED' THEN 'True'
			WHEN LEN(ISNULL([Sign For By], '')) > 0 THEN 'True'
			ELSE 'False'
		END,
		CASE 
			WHEN LEN([Pickup Date]) > 0 THEN CAST([Pickup Date] AS DATETIME)
			ELSE ''
		END,
		CASE 
			WHEN LEN([Scheduled Delivery Date]) > 0 THEN CAST([Scheduled Delivery Date] AS DATETIME)
			ELSE ''
		END
		, 'GROUND', [Sign For By], [Ship to Attention], [Ship to Name], [Ship to Address Line 1], [Ship to Address Line 2], 
		[Ship to City], [Ship to State Province], [Ship to Postal Code], [Ship to Country], [Ship to Phone], NULL,
		NULL, GETDATE(), 0, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 1, NULL, NULL, NULL, NULL, 
		CASE
			WHEN ISNUMERIC(katomPONum) = 1 AND LEN(katomPONum) = 6 THEN katomPONum
			ELSE NULL
		END
	FROM upsWorldShip
	WHERE [Tracking Number] NOT IN (SELECT [Tracking Number] FROM trackingInfo WHERE carrier = 'UPS')
			AND LEN(ISNULL([katomOrderID], '')) > 0		
	
	update u
	set u.katomOrderID = v.katomOrderID,
		u.katomPONum = 
			CASE 
				WHEN ISNUMERIC(v.PONumber) = 1 THEN v.PONumber
				ELSE NULL
			END
	from vollrathTracking v join upsWorldShip u on v.trackingNo = u.[Tracking Number] 
	
	delete from trackingInfo 
	where isnumeric(KatomOrderID) = 0 and charindex('-', KatomOrderID) = 0
END
GO
