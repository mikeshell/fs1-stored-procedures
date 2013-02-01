USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[ebayOrderImport]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[ebayOrderImport] 
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

--Fire the DTS package that pulls any text files into the ebay orders table

/*Begins the loop to pull down and parse payment information*/
	DECLARE @ebayOrder varchar(20)
	
	DECLARE ebayCursor CURSOR FOR 
	select distinct([invoice id]) from ebayorders where imported <> 1

	OPEN ebayCursor

	FETCH NEXT FROM ebayCursor INTO @ebayOrder

	WHILE @@FETCH_STATUS = 0
	BEGIN
	
	print @ebayOrder
	
	--Insert Header
	insert order_hdr
	([customerpo],[CustomerPO2],[selltoemail],[shiptoaddress],[shiptocity],[shiptostate],[shiptozip],
	[shiptocountry],[selltocustomername],[selltophone],[shipamount],[shiptocontact],[shiptoemail],[shiptocustomername],
	[phoneno],[faxno],[avsaddr],[avszip],[cvv2match],[respcode],
	[orderplacedby],[paymenttermscode],[CustomerSource], [importValidated], [exported], [batchtimestamp],
	ccnum, ccname, ccexpmo, ccexpyr, cvc,
	[selltocontact],[selltoaddress],[selltocity],[selltostate],[selltozip],[shipmethod],[ordertimestamp],[selltocountry], [shiptoaddress2], pnref, AmountSubmitted
	)
	select top 1 [invoice id], [site auction id], [buyer e-mail address], [shipping addr 1],
	[shipping city], [shipping region], [shipping postal code], [shipping country], [buyer first name]+' '+[buyer last name] as buyername, 
	[buyer day phone],[Shipping Cost], [buyer first name]+' '+[buyer last name] as shiptocontact,[buyer e-mail address], 
	[buyer first name]+' '+[buyer last name] as shiptocustmername, [buyer day phone] as phoneno, '' as faxno, 
	null as avsaddr, null as avszip, null as cvv2match, 0 as respcode, 
	'EB' as orderplacedby, CASE when [external Payment transaction id] <> '' THEN 'PP' ELSE 'CC' END as paymenttermscode, 'E' as customersource, 0 as [importValidated], 0 as exported,
	getdate() as batchtimestamp, null as [ccnum], 1 as [ccname], 1 as [ccexpmo], 2011 as [ccexpyr], 1 as [cvc],
	[buyer first name]+' '+[buyer last name] as selltocontact, [shipping addr 1] as selltoaddress,
	[shipping city] as selltocity, [shipping region] as selltostate, [shipping postal code] as selltozip,
	'Shipping: '+[Shipping Carrier]+' '+[Shipping Class Code] as shipmethod, [Invoice Date],
	[shipping country] as selltocountry, [shipping addr 2] as shippingaddress2, [External Payment Transaction ID] as pnref, [Order Total] as amount
	from ebayOrders
	where [invoice id] = @ebayOrder
	
	
	--Insert Items
	insert order_items (order_id, code, name, quantity, price, taxable, upsold, line_id, product_id, weight)
	select [invoice ID], sku, Title, Quantity, [Unit Price], 1 as taxable, 0 as upsold,
	right([Site Auction ID],5) as line_id,
	(select mivaprodid from products p where p.CODE = e.sku) as product_id,
	(select weight from products p where p.CODE = e.sku) as weight
	from ebayOrders e
	where [invoice id] = @ebayOrder
	
	--Insert Shipping Charge		
	insert into order_charges(order_id, charge_id, module_id, type, descrip, amount, disp_amt, tax_exempt)
	select top 1 [invoice id] as order_id, [invoice id] as charge_id, '160' as module_id, 'SHIPPING' as type, 'Shipping: '+[shipping carrier]+' '+[shipping class] as descrip, [shipping cost] as amount, [shipping cost] as disp_amt, 0 as tax_exempt  from ebayOrders
	where [invoice id] = @ebayOrder
	
	insert into order_options(order_id, line_id, attr_id, attr_code, opt_code, price, option_id, attmpat_id,  weight, data, data_long)
	select top 1 [invoice id] as order_id, 0 as line_id, '160' as attr_id, 'SHIPPING' as attr_code, 'Shipping: '+[shipping carrier]+' '+[shipping class] as opt_code, [shipping cost] as amount, '420', null, null, null, null
		 from ebayOrders
	where [invoice id] = @ebayOrder
	
	--Set imported value to 1
	update ebayorders 
	set imported = 1
	where [invoice id] = @ebayOrder
	
	
				
	FETCH NEXT FROM ebayCursor INTO @ebayOrder
	END

	CLOSE ebayCursor

	DEALLOCATE ebayCursor
	
	UPDATE order_hdr set ccexpmo = 1, ccexpyr = 2011 where orderplacedby = 'EB' and paymenttermscode = 'CC'

END
GO
