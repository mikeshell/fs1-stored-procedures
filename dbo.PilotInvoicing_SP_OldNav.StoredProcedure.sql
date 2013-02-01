USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[PilotInvoicing_SP_OldNav]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[PilotInvoicing_SP_OldNav]
AS
BEGIN
	/*Upload header information for associated invoices*/
	INSERT OPENQUERY(MYSQL,
	'select invoicenum, selltoname, selltoaddress, selltocity, selltostate, selltozipcode,  shiptocontact, shiptoaddress, shiptocity, shiptostate, shiptozipcode, shipdate, duedate, trackingcode, shippingagent, orderid, email, weborderid, orderamount, trackingnum from katom_mm5.cXMLinvoiceheader')
	--selltocountry,

	SELECT --convert(varchar(20),shipmentdt,100)
	top 100
	No_ as InvoiceNo,
	[Sell-to Customer Name] as selltoname, [Sell-to Address] as selltoaddress, [Sell-to City] as selltocity, [Sell-to State] as selltostate, [Sell-to ZIP Code] as selltozipcode,
	[Ship-to Contact] as shiptocontact, [Ship-to Address] as shiptoaddress, [Ship-to City] as shiptocity, [Ship-to State] as shiptostate, [Ship-to ZIP Code] as shiptozipcode, [Shipment Date], [Due Date], [Package Tracking No_] as trackingcode, [Shipping Agent Code] as shippingagentcode, [Your Reference] as orderid,
	[E-mail] as email, CASE WHEN (CharINDEX('/', [Your Reference])-2) > 0 THEN left([your reference],CharINDEX('/', [your reference])-2) ELSE [your reference] END as weborderid, [Amount Authorized] as orderamount, [Package Tracking No_] as trackingnum
	FROM fs1.[KatomDev].[dbo].[B&B Equipment & Supply Inc_$Sales Invoice Header] a
	where [sell-to customer no_] = '270665'
	and no_ COLLATE Latin1_General_CI_AS  not in (select id from pilotxref)
	order by a.[Shipment Date] desc

	/*Upload items for associated invoices*/
	INSERT OPENQUERY(MYSQL, 
	'select invoicenum, linenum, code, description, uom, quantity, unitprice, subtotal
	from katom_mm5.cXMLinvoiceline')

	SELECT [Document No_], [Line No_], No_ , Description, [Unit of Measure], cast(Quantity as float) as qty, cast([Unit Price] as float) as unitprice, cast(Amount as float) as amount
	  FROM fs1.[KatomDev].[dbo].[B&B Equipment & Supply Inc_$Sales Invoice Line]
	  where [Document No_] in(
	 
	SELECT No_
	FROM fs1.[KatomDev].[dbo].[B&B Equipment & Supply Inc_$Sales Invoice Header] a
	where [sell-to customer no_] = '270665'
	and no_ COLLATE Latin1_General_CI_AS  not in (select id from pilotxref)
	 )

	/*Update cross reference table so that these are not uploaded again*/
	insert into pilotxref
	select no_, [your reference]
	from fs1.[KatomDev].[dbo].[B&B Equipment & Supply Inc_$Sales Invoice Header] a
	where [sell-to customer no_] = '270665' and
		no_ COLLATE Latin1_General_CI_AS  not in (select id from pilotxref)


END
GO
