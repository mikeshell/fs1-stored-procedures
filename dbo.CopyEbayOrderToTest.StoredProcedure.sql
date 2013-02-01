USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[CopyEbayOrderToTest]    Script Date: 02/01/2013 11:09:50 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[CopyEbayOrderToTest]
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

DECLARE @ORDERID varchar(50)

SET @ORDERID = '13151350'

	insert into order_hdrtest(
	[customerpo]
      ,[shiptocontact]
      ,[shiptoemail]
      ,[shiptocustomername]
      ,[phoneno]
      ,[faxno]
      ,[shiptoaddress]
      ,[shiptoaddress2]
      ,[shiptocity]
      ,[shiptostate]
      ,[shiptozip]
      ,[shiptocountry]
      ,[selltocustomername]
      ,[selltoemail]
      ,[selltocontact]
      ,[selltoaddress]
      ,[selltoaddress2]
      ,[selltocity]
      ,[selltostate]
      ,[selltozip]
      ,[ccnum]
      ,[ccname]
      ,[ccexpmo]
      ,[ccexpyr]
      ,[cvc]
      ,[exported]
      ,[shipmethod]
      ,[shipamount]
      ,[ordertimestamp]
      ,[batchtimestamp]
      ,[importValidated]
      ,[avsaddr]
      ,[avszip]
      ,[cvv2match]
      ,[pnref]
      ,[respcode]
      ,[selltocountry]
      ,[sellto1]
      ,[selltophone]
      ,[orderplacedby]
      ,[paymenttermscode]
      ,[CustomerPO2]
      ,[CustomerSource]
      ,[AmountSubmitted])
      select [customerpo]
      ,[shiptocontact]
      ,[shiptoemail]
      ,[shiptocustomername]
      ,[phoneno]
      ,[faxno]
      ,[shiptoaddress]
      ,[shiptoaddress2]
      ,[shiptocity]
      ,[shiptostate]
      ,[shiptozip]
      ,[shiptocountry]
      ,[selltocustomername]
      ,[selltoemail]
      ,[selltocontact]
      ,[selltoaddress]
      ,[selltoaddress2]
      ,[selltocity]
      ,[selltostate]
      ,[selltozip]
      ,[ccnum]
      ,[ccname]
      ,[ccexpmo]
      ,[ccexpyr]
      ,[cvc]
      ,[exported]
      ,[shipmethod]
      ,[shipamount]
      ,[ordertimestamp]
      ,[batchtimestamp]
      ,[importValidated]
      ,[avsaddr]
      ,[avszip]
      ,[cvv2match]
      ,[pnref]
      ,[respcode]
      ,[selltocountry]
      ,[sellto1]
      ,[selltophone]
      ,[orderplacedby]
      ,[paymenttermscode]
      ,[CustomerPO2]
      ,[CustomerSource]
      ,[AmountSubmitted] from order_hdr where customerpo = @ORDERID
	

	update order_hdrtest
	set paymenttermscode = 'PP',
	 exported = 0
	where customerpo = @ORDERID

insert into order_itemstest(
	  [order_id]
      ,[line_id]
      ,[product_id]
      ,[code]
      ,[name]
      ,[price]
      ,[weight]
      ,[taxable]
      ,[upsold]
      ,[quantity])
      select 
      [order_id]
      ,[line_id]
      ,[product_id]
      ,[code]
      ,[name]
      ,[price]
      ,[weight]
      ,[taxable]
      ,[upsold]
      ,[quantity]
  FROM [KatomDev].[dbo].[order_items]
	where order_id = @ORDERID

insert into order_chargestest(
      [charge_id]
      ,[module_id]
      ,[type]
      ,[descrip]
      ,[amount]
      ,[disp_amt]
      ,[tax_exempt])
select [charge_id]
      ,[module_id]
      ,[type]
      ,[descrip]
      ,[amount]
      ,[disp_amt]
      ,[tax_exempt] from order_charges
where order_id = @ORDERID

insert into order_optionstest
      ([order_id]
      ,[line_id]
      ,[attr_id]
      ,[attr_code]
      ,[option_id]
      ,[attmpat_id]
      ,[opt_code]
      ,[price]
      ,[weight]
      ,[data]
      ,[data_long])
select [order_id]
      ,[line_id]
      ,[attr_id]
      ,[attr_code]
      ,[option_id]
      ,[attmpat_id]
      ,[opt_code]
      ,[price]
      ,[weight]
      ,[data]
      ,[data_long]
      from order_options
where order_id = @ORDERID
      
END
GO
