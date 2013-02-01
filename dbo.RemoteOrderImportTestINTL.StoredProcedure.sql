USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[RemoteOrderImportTestINTL]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[RemoteOrderImportTestINTL]
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

UPDATE OPENQUERY (MYSQL, 'SELECT * FROM katom_mm5.s01_Orders s where batch_id = 0') 
	SET batch_id = '2';

/* Imports the Order Headers based on the un-batched headers */
INSERT INTO order_hdr (customerpo, shiptocontact, shiptoemail,shiptocustomername, phoneno, faxno, shiptoaddress, shiptocity, shiptostate, shiptozip, shiptocountry, selltocustomername, selltoemail, selltocontact, selltoaddress, selltocity, selltostate, selltozip, ordertimestamp, batchtimestamp, shipmethod, shipamount)

SELECT * FROM openquery(MYSQL, '
		SELECT  id as customerpo, 
		upper(concat(ship_fname,'' '',ship_lname)) as shiptocontact,
		ship_email as shiptoemail,
		if (ship_comp <> '''', upper(ship_comp), upper(concat(ship_fname,'' '',ship_lname)))  as shiptocustomername,
		ship_phone as phoneno, 
		ship_fax as faxno, 
		upper(ship_addr) as shiptoaddress,
		upper(ship_city) as shiptocity,
		upper(ship_state) as shiptostate,
		ship_zip as shiptozip,
		ship_cntry as shiptocountry,
		if (bill_comp <> '''', upper(bill_comp), upper(concat(bill_fname,'' '',bill_lname)))  as selltocustomername,
		upper(bill_email) as selltoemail,
		upper(concat(bill_fname,'' '',bill_lname)) as selltocontact,
		upper(bill_addr) as selltoaddress,
		upper(bill_city) as selltocity,
		upper(bill_state) as selltostate,
		bill_zip as selltozip,
		from_unixtime(orderdate) as ordertimestamp,
		now(),
		(SELECT descrip FROM katom_mm5.s01_OrderCharges where order_id = customerpo
         and module_id <> ''90'' AND module_id <> ''1001'' AND module_id <> ''186'' limit 1) as method,
		(SELECT amount FROM katom_mm5.s01_OrderCharges where order_id = customerpo
		 and module_id <> ''90'' AND module_id <> ''1001'' AND module_id <> ''186'' limit 1) as amount		
		FROM katom_mm5.s01_Orders where batch_id = 2')

  
  UPDATE [KatomDev].[dbo].[order_hdr] SET phoneNo = RIGHT(dbo.StripNonNumeric(phoneNo),10)
  
  UPDATE [KatomDev].[dbo].[order_hdr] SET ccnum = dbo.StripNonNumeric(ccnum)
  
  UPDATE [KatomDev].[dbo].[order_hdr] SET shiptoaddress = dbo.RemoveNonAlphaNumericCharacters([shiptoaddress])
  
  UPDATE [KatomDev].[dbo].[order_hdr] SET selltoaddress = dbo.RemoveNonAlphaNumericCharacters([selltoaddress])
		
/* Imports the Order Line Items based on the un-batched headers */
--delete from order_items

	INSERT INTO order_items(order_id, line_id, product_id, code, name, price, weight, taxable, upsold, quantity)

	SELECT * FROM openquery(MYSQL, '
		SELECT
		order_id,
		line_id,
		product_id,
		code,
		name,
		price,
		weight,
		taxable,
		upsold,
		quantity	
		FROM katom_mm5.s01_OrderItems
		where order_id in (SELECT id FROM katom_mm5.s01_Orders where batch_id = 2)')

/* Imports the Order Charges based on the un-batched headers */
--delete from order_charges

	INSERT INTO order_charges(order_id, charge_id, module_id, type, descrip, amount, disp_amt, tax_exempt)

	SELECT * FROM openquery(MYSQL, '
		SELECT
		order_id,
		charge_id,
		module_id,
		type,
		descrip,
		amount,
		disp_amt,
		tax_exempt
		FROM katom_mm5.s01_OrderCharges
		where order_id in (SELECT id FROM katom_mm5.s01_Orders where batch_id = 2)')


/*Pulls down the option information based on un-batched order headers*/
--delete from order_options

	INSERT INTO order_options(order_id, line_id, attr_id, attr_code, option_id, attmpat_id, opt_code, price, weight, data, data_long)

	SELECT * FROM openquery(MYSQL, '
		SELECT
		order_id,
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
		FROM katom_mm5.s01_OrderOptions
		where order_id in (SELECT id FROM katom_mm5.s01_Orders where batch_id = 2 and attr_code <> ''recip_email'')')
		
		update
		[KatomDev].[dbo].[order_options]
		set opt_code = data, data = ''
		where LEN(data) > 1
		
/* Shipping and coupons */
	INSERT INTO order_options(order_id, line_id, option_id, attr_id, attr_code, opt_code, price)

	SELECT * FROM openquery(MYSQL, '
		SELECT order_id,
		charge_id as line_id,
		charge_id,
		module_id,
		type,
		descrip,
		amount
		FROM katom_mm5.s01_OrderCharges
		where module_id in (1001, 186)
		and order_id in (SELECT id FROM katom_mm5.s01_Orders where batch_id = 2)')
	

/*Changes Batch ID to indicate imported status*/
	UPDATE OPENQUERY (MYSQL, 'SELECT * FROM katom_mm5.s01_Orders s where batch_id = 2') 
	SET batch_id = '1';

/*Begins the loop to pull down and parse payment information*/
	DECLARE @Order varchar(10)

	DECLARE DemoCursor CURSOR FOR 
	SELECT customerpo FROM order_hdr WHERE exported = 0

	OPEN DemoCursor

	FETCH NEXT FROM DemoCursor INTO @Order

	WHILE @@FETCH_STATUS = 0
	BEGIN
		exec paymentparse @Order	
		FETCH NEXT FROM DemoCursor INTO @Order
	END

CLOSE DemoCursor

DEALLOCATE DemoCursor

/*Display the gathered header information*/
SELECT * FROM order_hdr

/*Display the gathered line item information with customer info*/
SELECT * FROM order_hdr
join order_items
ON order_id = customerpo
order by order_id

select name, attr_code, opt_code from order_options o
join order_items i 
on i.order_id = o.order_id and i.line_id = o.line_id

exec CvcImport

END
GO
