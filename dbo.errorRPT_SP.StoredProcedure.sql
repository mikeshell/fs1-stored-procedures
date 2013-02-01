USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[errorRPT_SP]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[errorRPT_SP]
	@filterBy VARCHAR(50), @orderType varchar(10), @part INT
AS
BEGIN
	DECLARE @startRowIndex INT, @recTT INT, @query VARCHAR(MAX), @queryToAdd VARCHAR(MAX)
	
	SET @query = 'SELECT [Code], ICDiscountGrp priceGroup,
					CASE 
						WHEN [NAME] IS NULL THEN ''X''
						WHEN LEN([NAME]) = 0 THEN ''X''
						ELSE ''''
					END [NAME],
					CASE 
						WHEN [IMAGE] IS NULL THEN ''X''
						WHEN LEN([IMAGE]) = 0 THEN ''X''
						ELSE ''''
					END [IMAGE],
					CASE 
						WHEN [mpn] IS NULL THEN ''X''
						WHEN LEN([mpn]) = 0 THEN ''X''
						ELSE ''''
					END [mpn],
					CASE 
						WHEN [price] IS NULL THEN ''X''
						WHEN LEN([price]) = 0 THEN ''X''
						WHEN [price] = 0 THEN ''X''
						ELSE ''''
					END [price],
					CASE 
						WHEN [listPrice] IS NULL THEN ''X''
						WHEN LEN([listPrice]) = 0 THEN ''X''
						WHEN [listPrice] = 0 THEN ''X''
						ELSE ''''
					END [listPrice],
					CASE 
						WHEN [prodDesc] IS NULL THEN ''X''
						WHEN LEN(CAST([prodDesc] AS VARCHAR(MAX))) = 0 THEN ''X''
						ELSE ''''
					END [prodDesc],
					CASE 
						WHEN [UOM] IS NULL THEN ''X''
						WHEN LEN([UOM]) = 0 THEN ''X''
						ELSE ''''
					END [UOM],
					CASE 
						WHEN [WEIGHT] IS NULL THEN ''X''
						WHEN LEN([WEIGHT]) = 0 THEN ''X''
						WHEN [WEIGHT] = 0 THEN ''X''
						ELSE ''''
					END [WEIGHT],
					CASE 
						WHEN vendorID IS NULL THEN ''X''
						WHEN LEN(vendorID) = 0 THEN ''X''
						ELSE ''''
					END vendorID,
					CASE 
						WHEN ICDiscountGrp IS NULL THEN ''X''
						WHEN LEN(ICDiscountGrp) = 0 THEN ''X''
						ELSE ''''
					END ICDiscountGrp
				FROM products
				WHERE active = 1 AND isweb=1 AND 
					(LEN(ISNULL([IMAGE] , '''')) = 0 OR
						LEN(ISNULL([listPrice] , '''')) = 0 OR
						[listPrice] = 0 OR
						LEN(ISNULL([mpn] , '''')) = 0 OR
						LEN(ISNULL([NAME] , '''')) = 0 OR
						LEN(ISNULL([price] , '''')) = 0 OR
						[price] = 0 OR
						LEN(ISNULL(CAST([prodDesc] AS VARCHAR(MAX)), '''')) = 0 OR
						LEN(ISNULL([UOM] , '''')) = 0 OR
						LEN(ISNULL([WEIGHT] , '''')) = 0 OR
						[WEIGHT] = 0 OR
						LEN(ISNULL(vendorID, '''')) = 0 OR
						LEN(ISNULL(ICDiscountGrp, '''')) = 0) ' +
				CASE @part
					WHEN 1 THEN ''
					ELSE 'AND CHARINDEX(''-PART'', ICDiscountGrp) = 0 '
				END +
				'ORDER BY ' +
				CASE @orderType
					WHEN 1 THEN '[NAME] DESC, code' 
					WHEN 2 THEN '[IMAGE] DESC, code' 
					WHEN 3 THEN '[mpn] DESC, code' 
					WHEN 4 THEN '[price] DESC, code' 
					WHEN 5 THEN '[listprice] DESC, code' 
					WHEN 6 THEN '[prodDesc] DESC, code' 
					WHEN 7 THEN '[UOM] DESC, code' 
					WHEN 8 THEN '[WEIGHT] DESC, code' 
					WHEN 9 THEN 'vendorID DESC, code' 
					WHEN 10 THEN 'ICDiscountGrp DESC, code' 
					ELSE 'code'
				END			
					
	EXEC(@query)
END
GO
