USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[RemoteOrderImportFAB]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[RemoteOrderImportFAB]
AS
BEGIN

--See what's Coming
--SELECT * FROM OPENQUERY (RDN, 'SELECT * FROM FA_Pending s where sent = 0 and approved =''A'' and FAmemo <> ''''') 


--Batch Unsent Files
UPDATE OPENQUERY (RDN, 'SELECT * FROM FA_Pending s where sent = 0 and approved =''A'' and FAmemo <> ''''') 
SET sent = '2';


/* Imports the Order Headers based on the un-batched headers */
		INSERT INTO order_hdr (customerpo, shiptocontact, shiptoemail, shiptocustomername, phoneno, faxno, 
		shiptoaddress, shiptoaddress2, shiptocity, shiptostate, shiptozip, shiptocountry, 
		selltocustomername, selltoemail, selltocontact, selltoaddress, selltoaddress2, 
		selltocity, selltostate, selltozip, ordertimestamp, batchtimestamp, 
		selltocountry, selltophone, CustomerPO2, shipmethod, shipamount, 
		pnref, respcode, ccnum, AmountSubmitted,avsaddr, avszip, cvv2match, ccexpmo, ccexpyr, paymenttermscode, CustomerSource, orderplacedby)


SELECT * FROM openquery(RDN, '
		SELECT concat(''FA'',cast(b.basket_id as char)) AS customerpo, '''' AS shiptocontact, Email AS shiptoemail,  ship_comp AS shiptocustomername, billphone AS phoneno, '''' AS faxno, 
		ship_addr AS shiptoaddress, ''''as shiptoaddress2, ship_city AS shiptocity, ship_state AS shiptostate, ship_zip AS shiptozip, ship_cntry AS shiptocountry, 
		CompanyName AS selltocustomername, Email AS selltoemail, login AS selltocontact, Address1 AS selltoaddress, '''' as selltoaddress2, 
		City AS selltocity, State AS selltostate, Zip AS selltozip, NOW() AS ordertimestamp, NOW() AS batchtimestamp, 
		''US'' AS selltocountry, billphone AS selltophone, memberPO AS CustomerPO2,
		(SELECT method FROM FA_BasketCharges c WHERE b.basket_id = c.basket_id LIMIT 1) AS shipmethod, 
		(SELECT cost FROM FA_BasketCharges c WHERE b.basket_id = c.basket_id LIMIT 1) AS shipamount,
		'''' AS pnref, 0 AS respcode, 1 AS ccnum, 0 AS AmountSubmitted, '''' as avsaddr, '''' as avszip, '''' as cvv2match,
		1 as ccexpmo, 1 as ccexpyr, ''NET 30'' as paymenttermscode, ''S'' as CustomerSource, ''FA'' as orderplacedby
		FROM FA_Pending p
		JOIN FAmembers m
		ON memberid = m.id
		JOIN FA_Baskets b
		ON CAST(p.ponum AS SIGNED) = CAST(b.ponum AS SIGNED)
		JOIN FAapprovals a
		ON m.memberGroup = a.memberGroup
		WHERE approved = ''A'' and sent = 2
		and b.basket_id not in (''66812'')
		')
		order by customerpo

  
	UPDATE [KatomDev].[dbo].[order_hdr] SET phoneNo = LEFT(dbo.StripNonNumeric(phoneNo),10)
	  
	UPDATE [KatomDev].[dbo].[order_hdr] SET selltophone = LEFT(dbo.StripNonNumeric(selltophone),10)
	   
	UPDATE [KatomDev].[dbo].[order_hdr] SET shiptoaddress = dbo.RemoveNonAlphaNumericCharacters([shiptoaddress])
	  
	UPDATE [KatomDev].[dbo].[order_hdr] SET selltoaddress = dbo.RemoveNonAlphaNumericCharacters([selltoaddress])
		
/* Imports the Order Line Items based on the un-batched headers */
--delete from order_items

	INSERT INTO order_items(order_id, line_id, product_id, code, name, price, weight, taxable, upsold, quantity)

	SELECT * FROM openquery(RDN, '
		SELECT CONCAT(''FA'',CAST(b.`basket_id` AS CHAR)) AS order_id, 
		line_id, product_id, CODE, NAME, price, weight, taxable, upsold, quantity
		FROM FA_BasketItems i
		JOIN FA_Baskets b
		ON b.basket_id = i.basket_id
		WHERE i.basket_id IN (SELECT b.`basket_id` FROM FA_Pending p JOIN FA_Baskets b ON CAST(p.ponum AS SIGNED) = CAST(b.ponum AS SIGNED) WHERE sent = 2)
		')
		
/* Imports the Order Charges based on the un-batched headers 
--delete from order_charges

	INSERT INTO order_charges(order_id, charge_id, module_id, type, descrip, amount, disp_amt, tax_exempt)

	SELECT * FROM openquery(RDN, '
		SELECT
		concat(''BB'',cast(order_id as char)) as order_id,
		0 as charge_id,
		0 as module_id,
		''SHIPPING'' as type,
		concat(''Shipping: '',method) as descrip,
		cost as amount,
		cost as disp_amt,
		'''' as tax_exempt
		FROM orderCharges
		where order_id in (SELECT id FROM orders where batch_id = 2)')
		*/

/*Pulls down the option information based on un-batched order headers
--delete from order_options

	INSERT INTO order_options(order_id, line_id, attr_id, attr_code, option_id, attmpat_id, opt_code, price, weight, data, data_long)

	SELECT * FROM openquery(RDN, '
		SELECT
		concat(''BB'',cast(order_id as char)) as order_id,
		line_id,
		attr_id,
		attr_code,
		option_id,
		attmpat_id,
		opt_code,
		price,
		weight,
		data,
		data_long	
		FROM orderOptions
		where order_id in (SELECT id FROM orders where batch_id = 2 and attr_code <> ''recip_email'')')

		
		update
		[KatomDev].[dbo].[order_options]
		set opt_code = data, data = ''
		where LEN(data) > 1
*/		
/* Shipping and coupons */
	INSERT INTO order_options(order_id, line_id, option_id, attr_id, attr_code, opt_code, price)

	SELECT * FROM openquery(RDN, '
		SELECT concat(''FA'',cast(basket_id as char)) as order_id,
		0 as line_id,
		420 as option_id,
		id as attr_id,
		''SHIPPING'' as attr_code,
		concat(''Shipping: '', method) as descrip,
		cost as price
		FROM FA_BasketCharges c
		WHERE c.basket_id IN (SELECT b.`basket_id` FROM FA_Pending p JOIN FA_Baskets b ON CAST(p.ponum AS SIGNED) = CAST(b.ponum AS SIGNED) WHERE sent = 2)')
		
	update order_options
	set option_id = 420
	where opt_code like 'Shipping:%'

/*Begins the loop to add comments and match products*/
--Insert Standard Comments
	DECLARE @ordernumber varchar(100)
	
	DECLARE faCursor CURSOR FOR 

	select * from openquery(RDN, 'SELECT CONCAT(''FA'',CAST(b.basket_id AS CHAR)) AS customerpo
		FROM FA_Pending p
		JOIN FA_Baskets b
		ON CAST(p.ponum AS SIGNED) = CAST(b.ponum AS SIGNED)
		WHERE approved = ''A'' AND sent = 2')

	OPEN faCursor

	FETCH NEXT FROM faCursor INTO @ordernumber
	
	WHILE @@FETCH_STATUS = 0
		BEGIN		
	
		insert into order_options (order_id, line_id, attr_id, attr_code, option_id, attmpat_id, opt_code, price)
		select @ordernumber, 0, 111, 'COMMENT', 999, null, 'BRING TO BOBBIEA', 0
		UNION ALL
		select @ordernumber, 0, 112, 'COMMENT', 999, null, 'BLIND SHIPMENT', 0
		UNION ALL
		select @ordernumber, 0, 113, 'COMMENT', 999, null, 'NO INVOICE NO FLYER', 0
		UNION ALL
		select @ordernumber, 0, 114, 'COMMENT', 999, null, 'NO KATOM BOX', 0
		UNION ALL
		select @ordernumber, 0, 115, 'COMMENT', 999, null, '***CHARGE SHIPPING AS ESTIMATED***', 0
		UNION ALL
		select @ordernumber, 0, 116, 'COMMENT', 999, null, 'DO NOT INVOICE BRING TO CASEY', 0
		UNION ALL
		select @ordernumber, 0, 117, 'COMMENT', 999, null, 'ONLY 1 INVOICE PER ORDER', 0
		
				--Product / Price Check
				DECLARE @incprice varchar(50)
				DECLARE @faprice varchar(50)
				DECLARE @ordercode varchar(50)
				DECLARE @prodcode varchar(50)
				DECLARE @message varchar(max)
				DECLARE @subject1 varchar(100)
				--DECLARE @ordernumber varchar(max)
				--SET @ordernumber = 'FA21730290'
				
				--if o.price <> p.fa_price = send an email
				--if p.code is null = send an email

				DECLARE ProdCursor CURSOR FOR 
				select o.code as OrderCode, p.code as ProdCode, o.price as IncPrice, p.fa_price as FAPrice
				from order_items o
				left outer join products p
				on o.code = p.code or RIGHT(p.code, len(p.code)-4) = o.code
				where product_id is null and order_id = @ordernumber
				--where order_id = 'FA21730290'

				OPEN ProdCursor

				print 'begin'
				SET @message = ''
				
				FETCH NEXT FROM ProdCursor INTO  @ordercode, @prodcode, @incprice, @faprice

					WHILE @@FETCH_STATUS = 0
					BEGIN
					
						print @incprice
						print @faprice
						print @ordercode
						print @prodcode
												
						/* Update Item Code */		
						IF @prodcode is not null 
							BEGIN
								print 'code found'
								Update order_items
								set code = @prodcode
								where code = @ordercode AND order_id = @ordernumber
							END
						
						IF @faprice <> @incprice
							BEGIN
								print 'price mismatch'
								SET @message += '<P>House Order for Merchants on Navision Order #'+@ordernumber+' has a price variance:<br/>'
								SET @message += @prodcode+' came in with price of '+@incprice+' while FAprice is set at '+@faprice+'<br/>'
						
							END
						
						IF @prodcode is null
							BEGIN
								print 'nullcode'
								SET @message += '<P>House Order for Merchants on Navision Order #'+@ordernumber+' has a product number issue:<br/>'
								SET @message += 'Product '+@ordercode+' does not exist<br/>'
								
								Update order_hdr
								set exported = 1
								where customerpo = @ordernumber
							END
						
						FETCH NEXT FROM ProdCursor INTO @ordercode, @prodcode, @incprice, @faprice
					END

				CLOSE ProdCursor

				DEALLOCATE ProdCursor
		
			IF @message <> ''
			BEGIN
				SET @subject1 = 'Attn: Bobbie - Order Issue for '+@ordernumber
				EXEC msdb.dbo.sp_send_dbmail @profile_name='katom', @recipients='bids@katom.com; mike@katom.com; dgower@bnbequip.com', @subject=@subject1, @body=@message, @body_format ='HTML'
			END
	
	FETCH NEXT FROM faCursor INTO @ordernumber

	END

	CLOSE faCursor

	DEALLOCATE faCursor

--exec CvcImport

UPDATE [KatomDev].[dbo].[order_hdr] SET ccnum = dbo.StripNonNumeric(ccnum)

--Pilot specifications update
update order_hdr
set orderplacedby = 'PI',
paymenttermscode = 'NET 30',
CustomerSource = 'S',
selltoemail = 'NA'
where ccname = 'Pilot'

update order_items
set order_id = replace(order_id, 'KT', 'PI')
where order_id in (select order_id from order_hdr where customerpo = 'PI'+RIGHT(order_id,6))

/*Changes Batch ID to indicate imported status*/
	UPDATE OPENQUERY (RDN, 'SELECT * FROM FA_Pending s where sent = 2') 
	SET sent = '1';


END


/*
select * from order_items
where order_id in (select customerpo from order_hdr
where exported = 0)

select order_id, o.code as OrderCode, p.code as ProdCode, o.name as OrdName, p.name as ProdName, o.price as IncPrice, p.fa_price as FAPrice
from order_items o
left outer join products p
on o.code = p.code or RIGHT(p.code, len(p.code)-4) = o.code
where product_id is null

select * from order_hdr
where selltocustomername like '%sweet%'

select * from order_hdr
where customerpo = 'FA21730290'
order by id desc

update order_hdr
set exported = 1 where id = 108044

select * from order_items
where order_id = 'FA21730290'

select * from order_options
where order_id = 'FA21730290'

*/
GO
