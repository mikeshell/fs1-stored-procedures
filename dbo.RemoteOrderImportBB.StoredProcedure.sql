USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[RemoteOrderImportBB]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[RemoteOrderImportBB]
AS
BEGIN
/**/

/* Archives the existing header information - Non-functioning as table was modified.
INSERT
INTO order_hdr_bkp
SELECT *, GETDATE() as lastupdate FROM  order_hdr
*/

--SELECT * FROM order_hdr_bkp
--DELETE FROM order_hdr_bkp

/*Empties the table to receive a new batch */
--DELETE FROM order_hdr

UPDATE OPENQUERY (RDN, 'SELECT * FROM orders s where batch_id = 0') 
	SET batch_id = '2';

/* Imports the Order Headers based on the un-batched headers */
INSERT INTO order_hdr (customerpo, shiptocontact, shiptoemail, shiptocustomername, phoneno, faxno, shiptoaddress, shiptocity, shiptostate, shiptozip, shiptocountry, selltocustomername, selltoemail, selltocontact, selltoaddress, selltocity, selltostate, selltozip, ordertimestamp, batchtimestamp, shipmethod, shipamount, selltocountry, selltophone, CustomerPO2, pnref, respcode, ccnum, AmountSubmitted)

SELECT * FROM openquery(RDN, '
		SELECT  concat(''BB'',cast(id as char)) as customerpo, 
		upper(concat(ship_fname,'' '',ship_lname)) as shiptocontact,
		(SELECT email FROM customers WHERE cust_id = id
		  LIMIT 1) AS shiptoemail,
		if (ship_comp <> '''', upper(ship_comp), upper(concat(ship_fname,'' '',ship_lname)))  as shiptocustomername,
		ship_phone as phoneno, 
		'' '' as faxno, 
		upper(ship_addr) as shiptoaddress,
		upper(ship_city) as shiptocity,
		upper(ship_state) as shiptostate,
		ship_zip as shiptozip,
		ship_cntry as shiptocountry,
		if (bill_comp <> '''', upper(bill_comp), upper(concat(bill_fname,'' '',bill_lname)))  as selltocustomername,
		(SELECT email FROM customers WHERE cust_id = id
		  LIMIT 1) as selltoemail,
		left(upper(concat(bill_fname,'' '',bill_lname)),50) as selltocontact,
		upper(bill_addr) as selltoaddress,
		upper(bill_city) as selltocity,
		upper(bill_state) as selltostate,
		bill_zip as selltozip,
		from_unixtime(orderdate) as ordertimestamp,
		now(),
		(SELECT descrip FROM orderCharges where order_id = id
         and type = ''SHIPPING'' limit 1) as method,
		(SELECT amount FROM orderCharges where order_id = id
		  limit 1) as amount,
		 bill_cntry,
		 bill_phone,
		 ponumber as po2,
		 /*,
		(select purchaseordernum from cxmlxref where orderid = o.id limit 1) as po2*/
		pay_secdat,
		resp_code,
		card_num as ccnum,
		total
		FROM rdn_mm5.orders o 
		where batch_id = 2')

  update order_hdr
  set paymenttermscode = 'NET 30'
  where customerpo like 'BB%'  and CustomerPO2 is not null
  
  UPDATE [KatomDev].[dbo].[order_hdr] SET phoneNo = LEFT(dbo.StripNonNumeric(phoneNo),10)
  
  UPDATE [KatomDev].[dbo].[order_hdr] SET selltophone = LEFT(dbo.StripNonNumeric(selltophone),10)
   
  UPDATE [KatomDev].[dbo].[order_hdr] SET shiptoaddress = dbo.RemoveNonAlphaNumericCharacters([shiptoaddress])
  
  UPDATE [KatomDev].[dbo].[order_hdr] SET selltoaddress = dbo.RemoveNonAlphaNumericCharacters([selltoaddress])
		
/* Imports the Order Line Items based on the un-batched headers */
--delete from order_items

	INSERT INTO order_items(order_id, line_id, product_id, code, name, price, weight, taxable, upsold, quantity)

	SELECT * FROM openquery(RDN, '
		SELECT
		concat(''BB'',cast(order_id as char)) as order_id,
		lineID,
		product_id,
		code,
		name,
		price,
		weight,
		taxable,
		0 as upsold,
		quantity	
		FROM orderItems
		where order_id in (SELECT id FROM orders where batch_id = 2)')
		


/* Imports the Order Charges based on the un-batched headers */
--delete from order_charges

	INSERT INTO order_charges(order_id, charge_id, module_id, type, descrip, amount, disp_amt, tax_exempt)

	SELECT * FROM openquery(RDN, '
		SELECT
		concat(''BB'',cast(order_id as char)) as order_id,
		0 as charge_id,
		0 as module_id,
		type,
		descrip,
		amount,
		amount as disp_amt,
		tax_exempt
		FROM orderCharges
		where order_id in (SELECT id FROM orders where batch_id = 2)')
		


/*Pulls down the option information based on un-batched order headers*/
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
		
/* Shipping and coupons */
	INSERT INTO order_options(order_id, line_id, option_id, attr_id, attr_code, opt_code, price)

	SELECT * FROM openquery(RDN, '
		SELECT concat(''BB'',cast(order_id as char)) as order_id,
		0 as line_id,
		999 as option_id,
		charge_id as attr_id,
		type,
		concat(''Shipping: '',descrip),
		amount
		FROM orderCharges
		where type <> ''TAX'' and order_id in (SELECT id FROM orders where batch_id = 2)')
		
	update order_options
	set option_id = 420
	where opt_code like 'Shipping:%'		

/*Changes Batch ID to indicate imported status*/
	UPDATE OPENQUERY (RDN, 'SELECT * FROM orders s where batch_id = 2') 
	SET batch_id = '1';

/*Begins the loop to pull down and parse payment information
	DECLARE @Order varchar(10)
	DECLARE @Country varchar(10)

	DECLARE DemoCursor CURSOR FOR 
	SELECT customerpo, shiptocountry FROM order_hdr WHERE exported = 0 and shiptocountry <> 'XX'

	OPEN DemoCursor

	FETCH NEXT FROM DemoCursor INTO @Order, @Country

	WHILE @@FETCH_STATUS = 0
	BEGIN
		exec paymentparse @Order
		
		/* International Order Handling */		
		IF @Country NOT IN ('CA', 'US', 'XX')
			BEGIN
				Update order_hdr
				set shiptocity = (shiptocity+', '+shiptostate),
					shiptostate = shiptocountry,
					selltocity = (selltocity+', '+selltostate),
					selltostate = shiptocountry,
					shiptocountry = 'XX'
				where customerpo = @Order
			
				/*insert into order_items([order_id], [line_id], [product_id], [code],[name], [price], [weight], [taxable], [upsold], [quantity])
				values (@Order, 1, 999999, '', 'INTERNATIONAL ORDER', '0.00', '0', 1, 0, 0)*/
			END
							
		FETCH NEXT FROM DemoCursor INTO @Order, @Country
	END

CLOSE DemoCursor

DEALLOCATE DemoCursor

exec CvcImport*/

UPDATE [KatomDev].[dbo].[order_hdr] SET ccnum = dbo.StripNonNumeric(ccnum)

--Pilot specifications update
update order_hdr
set orderplacedby = 'PI',
paymenttermscode = 'NET 30',
CustomerSource = 'S',
selltoemail = 'NA'
where ccname = 'Pilot'

END
GO
