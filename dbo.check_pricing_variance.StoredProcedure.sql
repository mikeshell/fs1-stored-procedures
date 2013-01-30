USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[check_pricing_variance]    Script Date: 01/30/2013 16:26:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[check_pricing_variance]
AS
BEGIN	
	drop table ProductsMIVA
	select * into ProductsMIVA from openquery(MYSQL, 'select * from katom_mm5.s01_Products')

	truncate table prodTemp

	insert into prodTemp([No_], [UnitPrice])
	select code, PRICE
	from ProductsMIVA 
	where active = 1

	delete from prodTemp
	where [No_] in
	(
	select code
	from prodTemp t join products p on t.[No_] = p.CODE
	where CAST([UnitPrice] as decimal(10, 2)) = CAST(p.price as decimal(10, 2))
	)

	delete from prodTemp
	where [No_] in
	(
	select code
	from prodTemp t join products p on t.[No_] = p.CODE
	where clearance = 1 and active = 1
	)
	
	IF exists(select [No_] from prodTemp t join products p on t.[No_] = p.CODE
										   left join trueSuperTrailer s on s.code = p.CODE
										   join CAFeed c on p.CODE = c.Model)
	   BEGIN	
		DECLARE @No VARCHAR(100), @mivaPrice money, @katomPrice money, @bbPrice money, @MAP bit, @mapProgram varchar(10), 
				@sellingPrice money
			
		DECLARE @body1 VARCHAR(MAX), @email1 varchar(500), @subject1 varchar(150), @bgcolor VARCHAR(500), @counter int
		
		SET @counter = 1	
		
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
			<td style="text-align:center; width: 100px;">MIVA KaTom Price</td>
			<td style="text-align:center; width: 100px;">NAV KaTom Price</td>
			<td style="text-align:center; width: 100px;">NAV BB Price</td>
			<td style="text-align:center; width: 100px;">MAP Item</td>
			<td style="text-align:center; width: 100px;">MAP Program</td>
			<td style="text-align:center; width: 100px;">Selling Price<BR />TrueSuperTrailer</td>
		  </tr>'	

		DECLARE pCursor CURSOR FOR 
		select [No_], [UnitPrice] mivaPrice, p.price, p.BBPrice, p.MAP, p.MAP_Program, s.sellingPrice PriceInTrueSuperTrailer
		from prodTemp t join products p on t.[No_] = p.CODE
						left join trueSuperTrailer s on s.code = p.CODE
						join CAFeed c on p.CODE = c.Model
		order by 1
		
		OPEN pCursor
		FETCH NEXT FROM pCursor INTO @No, @mivaPrice, @katomPrice, @bbPrice, @MAP, @mapProgram, @sellingPrice

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
			<td>' + @No + '</td>
			<td align="center" >' + CAST(ISNULL(@mivaPrice, '-1') AS VARCHAR(100))  + '</td>
			<td align="center" >' + CAST(ISNULL(@katomPrice, '-1') AS VARCHAR(100)) + '</td>
			<td align="center" >' + CAST(ISNULL(@bbPrice, '-1') AS VARCHAR(100)) + '</td>
			<td align="center" >' + CAST(ISNULL(@MAP, '') AS VARCHAR(100)) + '</td>
			<td align="center" >' + ISNULL(@mapProgram, '') + '</td>
			<td align="center" >' + CAST(ISNULL(@sellingPrice, '') AS VARCHAR(11)) + '</td>
		  </tr>'		
			
			FETCH NEXT FROM pCursor INTO @No, @mivaPrice, @katomPrice, @bbPrice, @MAP, @mapProgram, @sellingPrice
		   END
		   
		SET @body1 = @body1 + '</table>'
		
		CLOSE pCursor
		DEALLOCATE pCursor
		
		
		-- PREPPING THE EMAIL TO SEND
		SET @email1 = 'mike@katom.com; david@katom.com; paula@katom.com; beau@katom.com'
	--	SET @email1 = 'david@katom.com'
		SET @subject1 = 'Out of Synch Pricing'

		EXEC msdb.dbo.sp_send_dbmail @profile_name='katom', @recipients=@email1,	@subject=@subject1,	@body=@body1, @body_format ='HTML',	@blind_copy_recipients='katom.archive@gmail.com'	
	   END	
END
GO
