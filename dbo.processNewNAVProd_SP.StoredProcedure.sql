USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[processNewNAVProd_SP]    Script Date: 01/30/2013 16:26:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[processNewNAVProd_SP]
AS
BEGIN
DECLARE @tmpString varchar(50)
DECLARE @CODE VARCHAR(100), @shortDesc VARCHAR(30), @katomUOM VARCHAR(25), 
		@vendorID VARCHAR(100), @mpn VARCHAR(100), @WEIGHT FLOAT, @shipAlone VARCHAR(1), 
		@freightOnly VARCHAR(1), @freeShipping VARCHAR(1), @prodL FLOAT, @prodW FLOAT, @prodH FLOAT, 
		@cost MONEY, @listPrice MONEY, @ACTIVE BIT, @isWeb BIT, @IMAGE VARCHAR(255),  
	    @NAME VARCHAR(255), @keywords VARCHAR(255), @prodDesc VARCHAR(MAX), @UPC float, @handlingCharge MONEY,
		@discountGroup varchar(255), @equivGroup varchar(255), @stockItem bit, @orderMin smallint, @orderMax smallint,
		@reorderQty smallint, @status tinyint, @binLocation varchar(50), @RelatedItems varchar(1000)

DECLARE @counter tinyint, @SQL VARCHAR(MAX)

SET @tmpString = NULL
SET @status = 0

TRUNCATE TABLE dbo.NAVProductImport

DECLARE lagCursor CURSOR FOR 
SELECT [CODE], [NAVDesc], [katomUOM], [vendorID], [mpn], [WEIGHT], [shipAlone], [freightOnly], 
	   [freeShipping], [prodL], [prodW], [prodH], [cost], [listprice], CASE [ACTIVE] WHEN 1 THEN 0 ELSE 1 END, [isWeb], [IMAGE],  
	   [NAME], [keywords], [prodDesc], CASE WHEN [UPC] is Null THEN 0 ELSE [UPC] END as [UPC], [handlingCharge], [ICDiscountGrp], [EquivGrp], [stockItem], [thresholdMin], [thresholdMax],
	   [reorderQty], [binLocation], [RelatedItems]
FROM products_w
WHERE DATEDIFF(DAY, GETDATE(), addedDT) = 0

OPEN lagCursor
FETCH NEXT FROM lagCursor INTO @CODE,@shortDesc,@katomUOM,@vendorID,@mpn,@WEIGHT,@shipAlone,@freightOnly,
	   @freeShipping,@prodL,@prodW,@prodH,@cost,@listPrice,@ACTIVE,@isWeb,@IMAGE,
	   @NAME,@keywords,@prodDesc,@UPC,@handlingCharge, @discountGroup, @equivGroup, @stockItem, @orderMin, @orderMax,
	   @reorderQty, @binLocation, @RelatedItems

WHILE @@FETCH_STATUS = 0
   BEGIN
	SET @prodDesc = ltrim(rtrim(@prodDesc))
	SET @prodDesc = REPLACE(@prodDesc,'''','&#39;')

	IF (@stockItem = 1)
		SET @status = 1
	ELSE
		SET @status = 0

	IF NOT EXISTS (SELECT * FROM NAVProductImport WHERE [No] = @code)
	   BEGIN
		INSERT INTO NAVProductImport([No], [Description], [Base Unit of Measure], [Vendor No], [Vendor Item No], 
			[Gross Weight], [Ship Alone], [Freight Only], [Free Shipping], [Length], [Width], [Height], 
			[Last Direct Cost], [List Price 1], [List Price 2], [Blocked], [Web Item], [Image File Name], 
			[Product Name], [Keywords], [SKU], [UPC], [Add Handling Charge], [Item/Cust Disc Gr], [Equivalent Group], [Status], 
			[Reorder Point], [Maximum Inventory], [Reorder Quantity], [Shelf/Bin No.], [Costing Method], [Related Items])
		VALUES(@CODE, @shortDesc, @katomUOM, @vendorID, @mpn, @WEIGHT, @shipAlone, @freightOnly,
			   @freeShipping, @prodL, @prodW, @prodH, @cost, @listPrice, @listPrice, @ACTIVE, @isWeb, @IMAGE,
			   @NAME, @keywords, @MPN, @UPC,@handlingCharge,@discountGroup,@equivGroup, @status, @orderMin, @orderMax, @reorderQty, @binLocation, 3, @RelatedItems)
		
		--COST METHOD IS SET TO 3 FOR AVERAGE
		
		SET @counter = 1
		WHILE LEN(@prodDesc) > 0
		   BEGIN
			SET @tmpString = LEFT(@prodDesc, 50)
			
			IF LEN(@prodDesc) > 50
			   BEGIN
				IF RIGHT(@tmpString, 1) <> ' '
					BEGIN
						IF dbo.RCHARINDEX(' ', @tmpString, 1) > 0
							BEGIN
								SET @tmpString = LTRIM(RTRIM(LEFT(@tmpString, dbo.RCHARINDEX(' ', @tmpString, 1))))
							END
						SET @prodDesc = LTRIM(RIGHT(@prodDesc, LEN(@prodDesc)-LEN(@tmpString)))
					END
				ELSE
					BEGIN
						SET @tmpString = LTRIM(RTRIM(@tmpString))
						SET @prodDesc = RIGHT(@prodDesc, LEN(@prodDesc)-50)
					END
			   END
			ELSE
			   BEGIN
				SET @prodDesc = ''
			   END
			
			SET @SQL = ''
			SET @SQL = 'UPDATE NAVProductImport SET [Extended Description ' + CAST(@counter AS VARCHAR(10))
						+ '] = ''' + @tmpString + ''' WHERE [No] = ''' + @CODE + ''''
			EXEC (@SQL)
			SET @counter = @counter + 1
		   END
	   END
	FETCH NEXT FROM lagCursor INTO @CODE,@shortDesc,@katomUOM,@vendorID,@mpn,@WEIGHT,@shipAlone,@freightOnly,
	   @freeShipping,@prodL,@prodW,@prodH,@cost,@listPrice,@ACTIVE,@isWeb,@IMAGE,
	   @NAME,@keywords,@prodDesc,@UPC,@handlingCharge, @discountGroup, @equivGroup, @stockItem, @orderMin, @orderMax,
	   @reorderQty, @binLocation, @RelatedItems
   END

CLOSE lagCursor
DEALLOCATE lagCursor
END
GO
