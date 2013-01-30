USE [KatomDev]
GO

/****** Object:  StoredProcedure [dbo].[RemoteOrderImportKatomRack]    Script Date: 01/30/2013 15:51:12 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
ALTER PROCEDURE [dbo].[RemoteOrderImportKatomRack]
AS
BEGIN

declare @lenstring int
declare @orderid varchar(50)
declare @start int
declare @line int
declare @subcomment varchar(50)
declare @comment varchar(2000)	
declare @price decimal(10,2)

--SELECT * FROM OPENQUERY (KRACK, 'SELECT * FROM orders s where batch_id = 0') 

--Duplicate checker
DECLARE @duplicatebillphone varchar(100)
DECLARE @body varchar(1000)
SET @body = 'Potential Duplicate Orders = '
SET @duplicatebillphone = ''

DECLARE duplicateCursor CURSOR FOR 

SELECT 'KT'+cast(id as varchar) FROM OPENQUERY (KRACK, 'SELECT * FROM orders s where batch_id = 0') 
where bill_phone in (
SELECT bill_phone FROM OPENQUERY (KRACK, 'SELECT * FROM orders s where batch_id = 0') 
where bill_phone <> ''
group by bill_phone
having COUNT(*) > 1
)

OPEN duplicateCursor

FETCH NEXT FROM duplicateCursor INTO @duplicatebillphone
	
	WHILE @@FETCH_STATUS = 0
		BEGIN
		
		SET @body = @body+' '+@duplicatebillphone
	
		FETCH NEXT FROM duplicateCursor INTO @duplicatebillphone

		END

CLOSE duplicateCursor
DEALLOCATE duplicateCursor

IF @body != 'Potential Duplicate Orders = ' BEGIN EXEC msdb.dbo.sp_send_dbmail @profile_name='katom', @recipients='pchesworth@katom.com; jeslinger@katom.com',	@subject='Potential Duplicate Order',	@body=@body, @body_format ='HTML', @blind_copy_recipients='katom.archive@gmail.com' END

--Fix for LL expiration year
UPDATE OPENQUERY (KRACK, 'SELECT exp_date FROM orders s where exp_date like ''%ll%''') 
		SET exp_date = replace(exp_date, 'll', '11');


UPDATE OPENQUERY (KRACK, 'SELECT * FROM orders s where batch_id = 0') 
	SET batch_id = '2';

/* Imports the Order Headers based on the un-batched headers */
INSERT INTO order_hdr (customerpo, shiptocontact, shiptoemail, shiptocustomername, phoneno, faxno, shiptoaddress, shiptoaddress2, shiptocity, shiptostate, shiptozip, shiptocountry, selltocustomername, selltoemail, selltocontact, selltoaddress, selltoaddress2, selltocity, selltostate, selltozip, ordertimestamp, batchtimestamp, selltocountry, selltophone, CustomerPO2, shipmethod, shipamount, pnref, respcode, ccnum, AmountSubmitted,avsaddr, avszip, cvv2match, ccexpmo, ccexpyr)

--shipmethod, shipamount,
/*		(SELECT descrip FROM orderCharges where order_id = o.id
         and type = ''SHIPPING'' limit 1) as method,
		(SELECT amount FROM orderCharges where order_id = o.id
		  limit 1) as amount,*/

SELECT * FROM openquery(KRACK, '
		SELECT  concat(''KT'',cast(o.id as char)) as customerpo, 
		upper(concat(ship_fname,'' '',ship_lname)) as shiptocontact,
		email as shiptoemail,
		if (ship_comp <> '''', upper(ship_comp), upper(concat(ship_fname,'' '',ship_lname)))  as shiptocustomername,
		ship_phone as phoneno, 
		'' '' as faxno, 
		upper(ship_addr) as shiptoaddress,
		upper(ship_addr2) as shiptoaddress2,
		upper(ship_city) as shiptocity,
		upper(ship_state) as shiptostate,
		ship_zip as shiptozip,
		ship_cntry as shiptocountry,
		if (o.bill_comp <> '''', upper(o.bill_comp), upper(concat(o.bill_fname,'' '',o.bill_lname)))  as selltocustomername,
		email as selltoemail,
		left(upper(concat(o.bill_fname,'' '',o.bill_lname)),50) as selltocontact,
		upper(o.bill_addr) as selltoaddress,
		upper(o.bill_addr2) as selltoaddress2,
		upper(o.bill_city) as selltocity,
		upper(o.bill_state) as selltostate,
		o.bill_zip as selltozip,
		from_unixtime(orderdate) as ordertimestamp,
		now(),
		 o.bill_cntry,
		 o.bill_phone,
		 '''' as po2,
		 (SELECT descrip FROM orderCharges where order_id = o.id
         and type = ''SHIPPING'' limit 1) as method,
		(SELECT amount FROM orderCharges where order_id = o.id
		  limit 1) as amount,
		 /*,
		(select purchaseordernum from cxmlxref where orderid = o.id limit 1) as po2*/
		pay_secdat,
		resp_code,
		REPLACE(card_num, ''*'', '''') as ccnum,
		total,
		avsaddr, 
		avszip, 
		cvv2match,
		LEFT(exp_date, 2) AS expmo, 
		CONCAT(''20'', RIGHT(exp_date, 2)) AS expyear
		FROM rdn_mm5.orders o 
		where batch_id = 2')
  
--  UPDATE [KatomDev].[dbo].[order_hdr] SET phoneNo = LEFT(dbo.StripNonNumeric(phoneNo),10)
  
--  UPDATE [KatomDev].[dbo].[order_hdr] SET selltophone = LEFT(dbo.StripNonNumeric(selltophone),10)
   
UPDATE [KatomDev].[dbo].[order_hdr] SET shiptoaddress = dbo.RemoveNonAlphaNumericCharacters([shiptoaddress])
  
UPDATE [KatomDev].[dbo].[order_hdr] SET selltoaddress = dbo.RemoveNonAlphaNumericCharacters([selltoaddress])
		
/* Imports the Order Line Items based on the un-batched headers */
--delete from order_items

INSERT INTO order_items(order_id, line_id, product_id, code, name, price, weight, taxable, upsold, quantity)

SELECT * FROM openquery(KRACK, '
		SELECT
		concat(''KT'',cast(order_id as char)) as order_id,
		lineID,
		product_id,
		code,
		name,
		price,
		weight,
		1 as taxable,
		0 as upsold,
		quantity	
		FROM orderItems
		where order_id in (SELECT id FROM orders where batch_id = 2)')

/* Imports the Order Charges based on the un-batched headers */
--delete from order_charges

INSERT INTO order_charges(order_id, charge_id, module_id, type, descrip, amount, disp_amt, tax_exempt)

SELECT * FROM openquery(KRACK, '
		SELECT
		concat(''KT'',cast(order_id as char)) as order_id,
		charge_id as charge_id,
		0 as module_id,
		descrip as type,
		descrip,
		amount,
		amount as disp_amt,
		tax_exempt
		FROM orderCharges
		where order_id in (SELECT id FROM orders where batch_id = 2)
		and type <> ''SHIPPING''')
		


/*Pulls down the option information based on un-batched order headers*/
--delete from order_options

	INSERT INTO order_options(order_id, line_id, attr_id, attr_code, option_id, attmpat_id, opt_code, price, weight, data, data_long)

	SELECT * FROM openquery(KRACK, '
		SELECT
		CONCAT(''KT'',cast(orderid as char)) as orderid,
		lineid,
		attrid,
		a.code as attr_code,
		cast(ifnull(s.id, o.attrid) as char) as option_id,
		0 as attmpat_id,
		left(ifnull(LEFT(CAST(CONCAT(a.prompt, '': '' ,s.code, '' $'',s.price) AS CHAR),50),concat(a.prompt, '': '' ,value)), 50) AS opt_code,
		s.price as price,
		0 as weight,
		'''' as data,
		'''' as data_long	
		FROM orderOptions o
		LEFT OUTER JOIN s01_Options s
		ON o.value = s.id
		LEFT OUTER JOIN s01_Attributes a
		ON a.id = attrid
		where orderid in (SELECT id FROM orders where batch_id = 2)
		')
		
	UPDATE
		[KatomDev].[dbo].[order_options]
		set opt_code = data, data = ''
		where LEN(data) > 1
		
/* Shipping and coupons */
	INSERT INTO order_options(order_id, line_id, option_id, attr_id, attr_code, opt_code, price)

	SELECT * FROM openquery(KRACK, '
		SELECT concat(''KT'',cast(order_id as char)) as order_id,
		0 as line_id,
		999 as option_id,
		charge_id as attr_id,
		type,
		descrip,
		amount
		FROM orderCharges
		where type <> ''TAX'' and order_id in (SELECT id FROM orders where batch_id = 2)')
		
	update order_options
	set option_id = 420
	where (opt_code like 'Shipping:%'
	or opt_code like '%gate%'
	or opt_code like '%Arrival Notification%'
	or opt_code like '%residential fee%')
	and option_id <> 420
	
	/* Verbage Fix */
	update order_hdr
	set shipmethod = 'Shipping: Free Shipping on Select Items'
	where shipmethod = 'Shipping: Free Shipping'
	
/* Comments */
DECLARE comCursor CURSOR FOR 

	SELECT * FROM openquery(KRACK, '
		SELECT CONCAT(''KT'',CAST(orderID AS CHAR)) AS order_id,
		COMMENT,
		0 as price
		FROM orderComments
		WHERE orderID IN (SELECT id FROM orders WHERE batch_id = 2) 
		and comment <> '''' 
		')

	OPEN comCursor

	FETCH NEXT FROM comCursor INTO @orderid, @comment, @price
	
	WHILE @@FETCH_STATUS = 0
	
	BEGIN
	
	SET @lenstring = LEN(@comment)
	
	SET @start = 0
	SET @LINE = 8000
	

	
	print @lenstring
		while @lenstring > 50
			begin
				set @subcomment = (substring(@comment,@start, 50))
				SET @lenstring = @lenstring-50
				SET @start = @start+50
				SET @LINE = @LINE+1
				
				INSERT INTO order_options(order_id, line_id, option_id, attr_id, attr_code, opt_code, price)
				values (@orderid, 0, 0, @line, 'COMMENT', @subcomment, @price)
				
			end
			
			SET @comment = (substring(@comment,@start, 50))
			
				IF LEN(@comment) < 50
				SET @LINE = @LINE+1
					BEGIN 
						INSERT INTO order_options(order_id, line_id, option_id, attr_id, attr_code, opt_code, price)
						values (@orderid, 0, 0, @line, 'COMMENT', @comment, @price)
					END

	
	FETCH NEXT FROM comCursor INTO @orderid, @comment, @price

	END

CLOSE comCursor

DEALLOCATE comCursor

/*Changes Batch ID to indicate imported status*/
	UPDATE OPENQUERY (KRACK, 'SELECT * FROM orders s where batch_id = 2') 
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


--Test Order Handlers  TS1024962
update order_hdr
set customerpo = replace(customerpo, 'KT', 'TS')
where respcode = 99

update order_items
set order_id = replace(order_id, 'KT', 'TS')
where order_id in (select 'KT'+right(customerpo, 7) from order_hdr where respcode = 99)

update order_options
set order_id = replace(order_id, 'KT', 'TS')
where order_id in (select 'KT'+right(customerpo, 7) from order_hdr where respcode = 99)

insert into order_options(order_id, line_id, attr_id, attr_code, option_id, opt_code, price)
select customerpo, 0, 8999, 'COMMENT', 0, 'TEST ORDER DO NOT PROCESS', 0 from order_hdr where respcode = 99
and customerpo not in (select order_id from order_options where opt_code = 'TEST ORDER DO NOT PROCESS')

--Pilot specifications update
update order_hdr
set orderplacedby = 'PI',
paymenttermscode = 'NET 30',
CustomerSource = 'S',
selltoemail = 'NA',
selltophone = '8655887488',
customerpo = REPLACE(customerpo, 'KT', 'PI'),
CustomerPO2 = shiptoaddress2,
shiptoaddress2 = ''
where shiptocustomername = 'PILOT'
and orderplacedby <> 'PI'

update order_items
set order_id = replace(order_id, 'KT', 'PI')
where order_id in (select order_id from order_hdr where customerpo = 'PI'+RIGHT(order_id,6) or customerpo = 'PI'+RIGHT(order_id,7))

update order_hdr
set exported = 1
where customerpo in (
select order_id-- ,line_id, count(*) 
from order_items
where order_id in (
select customerpo from order_hdr
where exported = 0
)
group by order_id, line_id
having count(*) > 1
)

update order_hdr
set exported = 1
where customerpo in (
select order_id-- attr_id, count(*)
from order_options
where order_id in (
select customerpo from order_hdr
where exported = 0
)
group by order_id, attr_id
having count(*) > 1
)

--Shipping Method syntax fix added 10-17-12 MS
update order_hdr
set shipmethod = replace(shipmethod, 'Shipping: ', '')
where shipmethod not in (
'Shipping: UPS Next Day Air',
'Shipping: UPS Ground',
'Shipping: UPS 3 Day Select',
'Shipping: UPS 2nd Day Air',
'Shipping: Free Shipping on Select Items'
)
and shipmethod like '%Shipping: %'

update order_hdr
set shipmethod = 'FREIGHT TRUCK'
where shipmethod = 'FREIGHT'

update order_hdr
set shipmethod = 'TO BE DETERMINED'
where shipmethod = 'TBD'

update order_hdr
set shipmethod = 'UPS Standard Canada'
where shipmethod = 'UPS Standard'

update order_hdr
set shipmethod = 'UPS Expedited'
where shipmethod = 'UPS Worldwide Expedited'

END

GO

