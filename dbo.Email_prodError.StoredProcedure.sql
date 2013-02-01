USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[Email_prodError]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[Email_prodError]
AS
BEGIN
	DECLARE @code VARCHAR(100), @name SMALLINT, @image SMALLINT, @mpn SMALLINT,
		@price SMALLINT, @listprice SMALLINT, @prodDesc SMALLINT, 
		@uom SMALLINT, @weight SMALLINT, @vendorID SMALLINT, @discountGRP SMALLINT,
		@tname SMALLINT, @timage SMALLINT, @tmpn SMALLINT,
		@tprice SMALLINT, @tlistprice SMALLINT, @tprodDesc SMALLINT, 
		@tuom SMALLINT, @tweight SMALLINT, @tvendorID SMALLINT, @tdiscountGRP SMALLINT

	DECLARE @body1 VARCHAR(MAX), @email1 VARCHAR(500), @subject1 varchar(150), @bgcolor VARCHAR(500), @counter TINYINT, @subBody VARCHAR(MAX)
	 
	SET @tname = 0 
	SET @timage = 0 
	SET @tmpn = 0 
	SET @tprice = 0 
	SET @tlistprice = 0 
	SET @tprodDesc = 0 
	SET @tuom = 0 
	SET @tweight = 0 
	SET @tvendorID = 0 
	SET @tdiscountGRP = 0
	SET @subBody = ''

	--INITIATE TABLE
	SET @body1 = '<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"
	"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
	<html xmlns="http://www.w3.org/1999/xhtml">
	<head>
	<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
	<title>Error Report</title>

	<style type="text/css">
	body,td,th {font-family: Arial, Helvetica, sans-serif;font-size: 10px; vertical-align: top; }
	</style>

	</head>
	<body>
	'
	  
	DECLARE sCursor CURSOR FOR 
	SELECT [Code], 
		CASE 
			WHEN LEN(ISNULL([NAME], '')) = 0 THEN 1
			ELSE 0
		END,
		CASE 
			WHEN LEN(ISNULL([IMAGE], '')) = 0 THEN 1
			ELSE 0
		END,
		CASE 
			WHEN LEN(ISNULL([mpn], '')) = 0 THEN 1
			ELSE 0
		END,
		CASE 
			WHEN LEN(ISNULL([price], '')) = 0 THEN 1
			WHEN [price] = 0 THEN 1
			ELSE 0
		END,
		CASE 
			WHEN LEN(ISNULL([listPrice], '')) = 0 THEN 1
			WHEN [listPrice] = 0 THEN 1
			ELSE 0
		END,
		CASE 
			WHEN LEN(CAST(ISNULL([prodDesc], '') AS VARCHAR(MAX))) = 0 THEN 1
			ELSE 0
		END,
		CASE 
			WHEN LEN(ISNULL([UOM], '')) = 0 THEN 1
			ELSE 0
		END,
		CASE 
			WHEN LEN(ISNULL([WEIGHT], '')) = 0 THEN 1
			WHEN [WEIGHT] = 0 THEN 1
			ELSE 0
		END,
		CASE 
			WHEN LEN(ISNULL(vendorID, '')) = 0 THEN 1
			ELSE 0
		END,
		CASE 
			WHEN LEN(ISNULL(ICDiscountGrp, '')) = 0 THEN 1
			ELSE 0
		END
	FROM products
	WHERE active = 1 AND isWeb = 1
		AND CHARINDEX('-PART', ICDiscountGrp) = 0 and
		(LEN(ISNULL([IMAGE] , '')) = 0 OR
			LEN(ISNULL([listPrice] , '')) = 0 OR
			[listPrice] = 0 OR
			LEN(ISNULL([mpn] , '')) = 0 OR
			LEN(ISNULL([NAME] , '')) = 0 OR
			LEN(ISNULL([price] , '')) = 0 OR
			[price] = 0 OR
			LEN(ISNULL(CAST([prodDesc] AS VARCHAR(MAX)), '')) = 0 OR
			LEN(ISNULL([UOM] , '')) = 0 OR
			LEN(ISNULL([WEIGHT] , '')) = 0 OR
			[WEIGHT] = 0 OR
			LEN(ISNULL(vendorID, '')) = 0 OR
			LEN(ISNULL(ICDiscountGrp, '')) = 0)
	ORDER BY code

	OPEN sCursor
	FETCH NEXT FROM sCursor INTO @code, @name, @image, @mpn, @price, @listprice, @prodDesc, @uom, @weight, @vendorID, @discountGRP

	WHILE @@FETCH_STATUS = 0
	   BEGIN		
		SET @tname = @tname + @name 
		SET @timage = @timage + @image 
		SET @tmpn = @tmpn + @mpn
		SET @tprice = @tprice + @price 
		SET @tlistprice = @tlistprice + @listprice 
		SET @tprodDesc = @tprodDesc + @prodDesc 
		SET @tuom = @tuom + @uom 
		SET @tweight = @tweight + @weight 
		SET @tvendorID = @tvendorID + @vendorID 
		SET @tdiscountGRP = @tdiscountGRP + @discountGRP
				
		FETCH NEXT FROM sCursor INTO @code, @name, @image, @mpn, @price, @listprice, @prodDesc, @uom, @weight, @vendorID, @discountGRP
	   END

	CLOSE sCursor
	DEALLOCATE sCursor

	IF @tname > 0 
	   BEGIN
		SET @subBody = @subBody + 'The number of products with missing PRODNAME: ' + CAST(@tname AS VARCHAR(10)) + '<BR />'
	   END
	IF @timage > 0 
	   BEGIN
		SET @subBody = @subBody + 'The number of products with missing IMG: ' + CAST(@timage AS VARCHAR(10)) + '<BR />'
	   END
	IF @tmpn > 0 
	   BEGIN
		SET @subBody = @subBody + 'The number of products with missing MPN: ' + CAST(@tmpn AS VARCHAR(10)) + '<BR />'
	   END
	IF @tprice > 0 
	   BEGIN
		SET @subBody = @subBody + 'The number of products with missing PRICE: ' + CAST(@tprice AS VARCHAR(10)) + '<BR />'
	   END
	IF @tlistprice > 0 
	   BEGIN
		SET @subBody = @subBody + 'The number of products with missing LISTPRICE: ' + CAST(@tlistprice AS VARCHAR(10)) + '<BR />'
	   END
	IF @tprodDesc > 0 
	   BEGIN
		SET @subBody = @subBody + 'The number of products with missing PRODDESC: ' + CAST(@tprodDesc AS VARCHAR(10)) + '<BR />'
	   END
	IF @tuom > 0 
	   BEGIN
		SET @subBody = @subBody + 'The number of products with missing UOM: ' + CAST(@tuom AS VARCHAR(10)) + '<BR />'
	   END
	IF @tweight > 0 
	   BEGIN
		SET @subBody = @subBody + 'The number of products with missing WEIGHT: ' + CAST(@tweight AS VARCHAR(10)) + '<BR />'
	   END
	IF @tvendorID > 0 
	   BEGIN
		SET @subBody = @subBody + 'The number of products with missing VENDORID: ' + CAST(@tvendorID AS VARCHAR(10)) + '<BR />'
	   END
	IF @tdiscountGRP > 0 
	   BEGIN
		SET @subBody = @subBody + 'The number of products with missing DISC_GRP: ' + CAST(@tdiscountGRP AS VARCHAR(10)) + '<BR />'
	   END
		
	IF LEN(@subBody) > 0 
	   BEGIN
		SET @subBody = @subBody + '<BR /><A HREF="http://www.katom.local/producterrorreport.asp">Go to report!</A>'
	   END
		
	SET @body1 = @body1 + @subBody

	-- PREPPING THE EMAIL TO SEND
	SET @email1 = 'groberts@katom.com;'
	--	SET @email1 = 'david@katom.com'
	SET @subject1 = 'PRODUCT ERROR REPORT'

	EXEC msdb.dbo.sp_send_dbmail @profile_name='katom', @recipients=@email1,	@subject=@subject1,	@body=@body1, @body_format ='HTML',	@blind_copy_recipients='katom.archive@gmail.com'	

END
GO
