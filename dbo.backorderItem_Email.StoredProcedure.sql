USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[backorderItem_Email]    Script Date: 01/30/2013 16:26:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[backorderItem_Email]
AS
BEGIN
	DECLARE @code VARCHAR(100), @thresholdMin INT, @ThresholdMax INT, @qtyOnHand INT, 
			@numOfOrder INT, @numItem INT, @OldestOrderDT DATETIME, @counter INT, @availToSale INT
			
	DECLARE @body1 VARCHAR(MAX), @email1 varchar(500), @subject1 varchar(150), @bgcolor VARCHAR(500)
		
	--INITIATE TABLE
	SET @body1 = '<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"
	"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
	<html xmlns="http://www.w3.org/1999/xhtml">
	<head>
	<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
	<title>Backorder Item</title>

	<style type="text/css">
	body,td,th {font-family: Arial, Helvetica, sans-serif;font-size: 10px; vertical-align: top; }
	</style>

	</head>
	<body>
	<table border="0" cellspacing="0" cellpadding="3" style="border:solid 1px #000000;">
	  <tr style="background-color:#446fb7; color:#FFFFFF; font-weight: bold;">
		<td style="text-align:center; width: 100px;">Product Code</td>
		<td style="text-align:center; width: 100px;">Min</td>
		<td style="text-align:center; width: 100px;">Max</td>
		<td style="text-align:center; width: 100px;">Qty On Hand</td>
		<td style="text-align:center; width: 100px;">Number of<BR />Open Orders</td>
		<td style="text-align:center; width: 100px;">Number of Item<BR />on Open Orders</td>
		<td style="text-align:center; width: 100px;">Available to Sale</td>
		<td style="text-align:center; width: 100px;">Oldest Open Order</td>
	  </tr>'
	  
	DECLARE sCursor CURSOR FOR 
	SELECT [No_], thresholdMin, ThresholdMax, isnull(qtyOnHand, 0) qtyOnHand, 
			numOfOrder, numItem, availToSale, OldestOrderDT
	FROM backOrder_item
	WHERE DATEDIFF(day, [OldestOrderdt], getdate()) > 3
	ORDER BY [OldestOrderdt] ASC;

	OPEN sCursor
	FETCH NEXT FROM sCursor INTO @code, @thresholdMin, @ThresholdMax, @qtyOnHand, @numOfOrder, @numItem, @availToSale, @OldestOrderDT

	WHILE @@FETCH_STATUS = 0
	   BEGIN		
		IF @counter = 1
		   BEGIN
			SET @counter = 2
			SET @bgcolor = '#FFFFFF'
		   END
		ELSE
		   BEGIN
			SET @counter = 1
			SET @bgcolor = '#bed6e9'
		   END
		
		SET @body1 = @body1 + ' <tr style="background-color:' + @bgcolor + ';">
		<td>' + @code + '</td>
		<td align="center" >' + CAST(@thresholdMin AS VARCHAR(100)) + '</td>
		<td align="center" >' + CAST(@ThresholdMax AS VARCHAR(100))  + '</td>
		<td align="center" >' + CAST(@qtyOnHand AS VARCHAR(100)) + '</td>
		<td align="center" >' + CAST(@numOfOrder AS VARCHAR(100)) + '</td>
		<td align="center" >' + CAST(@numItem AS VARCHAR(100)) + '</td>
		<td align="center" >' + CAST(@availToSale AS VARCHAR(100)) + '</td>
		<td align="center" >' + CAST(@OldestOrderDT AS VARCHAR(11)) + '</td>
	  </tr>'
				
		FETCH NEXT FROM sCursor INTO @code, @thresholdMin, @ThresholdMax, @qtyOnHand, @numOfOrder, @numItem, @availToSale, @OldestOrderDT
	   END


	CLOSE sCursor
	DEALLOCATE sCursor

	SET @body1 = @body1 + '</table>'

	-- PREPPING THE EMAIL TO SEND
	SET @email1 = 'patricia@katom.com; mmeade@katom.com'
--	SET @email1 = 'david@katom.com'
	SET @subject1 = 'Daily Backorder Item Report'

	EXEC msdb.dbo.sp_send_dbmail @profile_name='katom', @recipients=@email1,	@subject=@subject1,	@body=@body1, @body_format ='HTML',	@blind_copy_recipients='katom.archive@gmail.com'	

END
GO
