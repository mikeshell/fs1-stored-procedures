USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[upsBill_processFile]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[upsBill_processFile]
AS
BEGIN	
	TRUNCATE TABLE prodTemp

	INSERT INTO prodTemp([Product Name], [Shipping Length], [Shipping Width], [UnitPrice], [UnitPrice2]) 
	SELECT [COLUMN 20], MAX(CAST([COLUMN 26] AS DECIMAL(10, 2))) submittedWeight, 
		MAX(CAST([COLUMN 28] AS DECIMAL(10, 2))) billedWeight,
		SUM(CAST([COLUMN 52] AS DECIMAL(10, 2))) costTT, COUNT([COLUMN 52]) chargeLine
	FROM upsBillFile
	WHERE ISNUMERIC([COLUMN 26]) = 1
		AND LEFT([COLUMN 20], 2) = '1Z'	
	GROUP BY [COLUMN 20]

	--REMOVE ALL TRACKING STAT WITH BAD DATA
	DELETE FROM prodTemp
	WHERE [Product Name] IN (SELECT [COLUMN 20] FROM upsBillFile WHERE ISNUMERIC([COLUMN 26]) = 0)
		
	UPDATE u
	SET u.shippingCost = p.[UnitPrice],
		u.enteredWeight = p.[Shipping Length],
		u.billedWeight = p.[Shipping Width],
		u.numCharges = p.[UnitPrice2]
	FROM trackingInfo u JOIN prodTemp p ON p.[Product Name] = u.[Tracking Number]
	WHERE u.shippingCost IS NULL

	INSERT INTO upsbill
	SELECT uf.* 
	FROM upsbillFile uf LEFT JOIN upsbill u ON uf.[COLUMN 20] = u.[COLUMN 20] 
									AND uf.[COLUMN 44] = u.[COLUMN 44] 
									AND uf.[COLUMN 45] = u.[COLUMN 45] 
									AND uf.[COLUMN 52] = u.[COLUMN 52] 
	WHERE u.[COLUMN 20] IS NULL

	TRUNCATE TABLE prodTemp
	TRUNCATE TABLE upsBillFile
END
GO
