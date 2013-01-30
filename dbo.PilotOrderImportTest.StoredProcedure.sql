USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[PilotOrderImportTest]    Script Date: 01/30/2013 16:26:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[PilotOrderImportTest]
AS
BEGIN
/**/

/* Archives the existing header information - Non-functioning as table was modified.
INSERT
INTO order_hdrtest_bkp
SELECT *, GETDATE() as lastupdate FROM  order_hdrtest
*/

--SELECT * FROM order_hdrtest_bkp
--DELETE FROM order_hdrtest_bkp

/*Empties the table to receive a new batch */
--DELETE FROM order_hdrtest

UPDATE OPENQUERY (MYSQL, 'SELECT * FROM katom_mm5.s01_Orders s where batch_id = 3') 
	SET batch_id = '4';

/* Imports the Order Headers based on the un-batched headers */
INSERT INTO order_hdrtest (customerpo, shiptocontact, shiptoemail, shiptocustomername, phoneno, faxno, shiptoaddress, shiptocity, shiptostate, shiptozip, shiptocountry, selltocustomername, selltoemail, selltocontact, selltoaddress, selltocity, selltostate, selltozip, ordertimestamp, batchtimestamp, shipmethod, shipamount, selltocountry, selltophone)

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
		left(upper(concat(bill_fname,'' '',bill_lname)),50) as selltocontact,
		upper(bill_addr) as selltoaddress,
		upper(bill_city) as selltocity,
		upper(bill_state) as selltostate,
		bill_zip as selltozip,
		from_unixtime(orderdate) as ordertimestamp,
		now(),
		(SELECT descrip FROM katom_mm5.s01_OrderCharges where order_id = customerpo
         and module_id <> ''90'' AND module_id <> ''1001'' AND module_id <> ''186'' limit 1) as method,
		(SELECT amount FROM katom_mm5.s01_OrderCharges where order_id = customerpo
		 and module_id <> ''90'' AND module_id <> ''1001'' AND module_id <> ''186'' limit 1) as amount,
		 bill_cntry,
		 bill_phone
		FROM katom_mm5.s01_Orders where batch_id = 4')

  
  UPDATE [KatomDev].[dbo].[order_hdrtest] SET phoneNo = LEFT(dbo.StripNonNumeric(phoneNo),10)
  
  UPDATE [KatomDev].[dbo].[order_hdrtest] SET selltophone = LEFT(dbo.StripNonNumeric(selltophone),10)
   
  UPDATE [KatomDev].[dbo].[order_hdrtest] SET shiptoaddress = dbo.RemoveNonAlphaNumericCharacters([shiptoaddress])
  
  UPDATE [KatomDev].[dbo].[order_hdrtest] SET selltoaddress = dbo.RemoveNonAlphaNumericCharacters([selltoaddress])
		
/* Imports the Order Line Items based on the un-batched headers */
--delete from order_itemstest

	INSERT INTO order_itemstest(order_id, line_id, product_id, code, name, price, weight, taxable, upsold, quantity)

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
		where order_id in (SELECT id FROM katom_mm5.s01_Orders where batch_id = 4)')

/* Imports the Order Charges based on the un-batched headers */
--delete from order_chargestest

	INSERT INTO order_chargestest(order_id, charge_id, module_id, type, descrip, amount, disp_amt, tax_exempt)

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
		where order_id in (SELECT id FROM katom_mm5.s01_Orders where batch_id = 4)')


/*Pulls down the option information based on un-batched order headers*/
--delete from order_optionstest

	INSERT INTO order_optionstest(order_id, line_id, attr_id, attr_code, option_id, attmpat_id, opt_code, price, weight, data, data_long)

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
		where order_id in (SELECT id FROM katom_mm5.s01_Orders where batch_id = 4 and attr_code <> ''recip_email'')')
		
		update
		[KatomDev].[dbo].[order_optionstest]
		set opt_code = data, data = ''
		where LEN(data) > 1
		
/* Shipping and coupons */
	INSERT INTO order_optionstest(order_id, line_id, option_id, attr_id, attr_code, opt_code, price)

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
		and order_id in (SELECT id FROM katom_mm5.s01_Orders where batch_id = 4)')
	

/*Changes Batch ID to indicate imported status*/
	UPDATE OPENQUERY (MYSQL, 'SELECT * FROM katom_mm5.s01_Orders s where batch_id = 4') 
	SET batch_id = '1';

/*Begins the loop to pull down and parse payment information*/
	DECLARE @Order varchar(10)
	DECLARE @Country varchar(10)

	DECLARE DemoCursor CURSOR FOR 
	SELECT customerpo, shiptocountry FROM order_hdrtest WHERE exported = 0 and shiptocountry <> 'XX'

	OPEN DemoCursor

	FETCH NEXT FROM DemoCursor INTO @Order, @Country

	WHILE @@FETCH_STATUS = 0
	BEGIN
		exec paymentparsetest @Order
		
		/* International Order Handling */		
		IF @Country NOT IN ('XX', 'US')
			BEGIN
				Update order_hdrtest
				set shiptocity = (shiptocity+', '+shiptostate),
					shiptostate = shiptocountry,
					selltocity = (selltocity+', '+selltostate),
					selltostate = shiptocountry,
					shiptocountry = 'XX'
				where customerpo = @Order
			
				/*insert into order_itemstest([order_id], [line_id], [product_id], [code],[name], [price], [weight], [taxable], [upsold], [quantity])
				values (@Order, 1, 999999, '', 'INTERNATIONAL ORDER', '0.00', '0', 1, 0, 0)*/
			END
							
		FETCH NEXT FROM DemoCursor INTO @Order, @Country
	END

CLOSE DemoCursor

DEALLOCATE DemoCursor
/*Display the gathered header information*/
SELECT * FROM order_hdrtest

/*Display the gathered line item information with customer info*/
SELECT * FROM order_hdrtest
join order_itemstest
ON order_id = customerpo
order by order_id

select name, attr_code, opt_code from order_optionstest o
join order_itemstest i 
on i.order_id = o.order_id and i.line_id = o.line_id

--exec CvcImport

UPDATE [KatomDev].[dbo].[order_hdrtest] SET ccnum = dbo.StripNonNumeric(ccnum)

END
GO
