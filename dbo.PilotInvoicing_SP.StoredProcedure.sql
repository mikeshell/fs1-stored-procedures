USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[PilotInvoicing_SP]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[PilotInvoicing_SP]
AS
BEGIN
	/*Upload header information for associated invoices*/
	INSERT OPENQUERY(KRACK,
	'select invoicenum, selltoname, selltoaddress, selltocity, selltostate, selltozipcode,  shiptocontact, shiptoaddress, shiptocity, shiptostate, shiptozipcode, shipdate, duedate, trackingcode, shippingagent, orderid, email, weborderid, orderamount, trackingnum from cXMLinvoiceheader')
	--selltocountry,

	SELECT --convert(varchar(20),shipmentdt,100)
	top 100
	No_ as InvoiceNo,
	[Sell-to Customer Name] as selltoname, [Sell-to Address] as selltoaddress, [Sell-to City] as selltocity, [Sell-to County] as selltostate, [Sell-to Post Code] as selltozipcode,
	[Ship-to Contact] as shiptocontact, [Ship-to Address] as shiptoaddress, [Ship-to City] as shiptocity, [Ship-to County] as shiptostate, [Ship-to Post Code] as shiptozipcode, [Shipment Date], [Due Date], [Package Tracking No_] as trackingcode, [Shipping Agent Code] as shippingagentcode, [Your Reference] as orderid,
	[E-mail] as email, CASE WHEN (CharINDEX('/', [Your Reference])-2) > 0 THEN left([your reference],CharINDEX('/', [your reference])-2) ELSE [your reference] END as weborderid, [Amount Authorized] as orderamount, [Package Tracking No_] as trackingnum
	FROM fs1.[katom2009].[dbo].[B&B Equipment & Supply Inc_$Sales Invoice Header] a
	where [sell-to customer no_] = '270665'
	and no_ COLLATE Latin1_General_CI_AS  not in (select * from openquery(KRACK, 'select invoicenum from cXMLinvoiceheader'))
	and ([Package Tracking No_] <> ''
	or datediff(d, [Shipment Date], getdate()) > 4)
	
	order by a.[Shipment Date] desc

	/*Upload items for associated invoices*/
	INSERT OPENQUERY(KRACK, 
	'select invoicenum, linenum, code, description, uom, quantity, unitprice, subtotal
	from cXMLinvoiceline')

	SELECT [Document No_], [Line No_], No_ , Description, [Unit of Measure], cast(Quantity as float) as qty, cast([Unit Price] as float) as unitprice, cast(Amount as float) as amount
	  FROM fs1.[katom2009].[dbo].[B&B Equipment & Supply Inc_$Sales Invoice Line]
	  where [Document No_] in(
	 
	SELECT No_
	FROM fs1.[katom2009].[dbo].[B&B Equipment & Supply Inc_$Sales Invoice Header] a
	where [sell-to customer no_] = '270665'
	and no_ COLLATE Latin1_General_CI_AS  not in (select * from openquery(KRACK, 'select invoicenum from cXMLinvoiceline'))
	and ([Package Tracking No_] <> ''
	or datediff(d, [Shipment Date], getdate()) > 4)
	 )

	/*Update cross reference table so that these are not uploaded again*/
	insert into pilotxref
	select no_, [your reference]
	from fs1.[katom2009].[dbo].[B&B Equipment & Supply Inc_$Sales Invoice Header] a
	where [sell-to customer no_] = '270665' and	no_ COLLATE Latin1_General_CI_AS  not in (select id from pilotxref)
	and ([Package Tracking No_] <> ''
	or datediff(d, [Shipment Date], getdate()) > 4)
		
		---This section inserts orders into the table on KRACK to be processed by the cronjob
		BEGIN

		DECLARE @pilotOrder varchar(30)
		DECLARE @pilotOrders varchar(MAX)


		DECLARE pilotCursor CURSOR FOR 

				SELECT No_ FROM fs1.[KaTom2009].[dbo].[B&B Equipment & Supply Inc_$Sales Invoice Header]
				where [Bill-to Name] = 'PILOT'
				and datediff(d, getdate(), [Posting Date]) > -14
				and No_ collate SQL_Latin1_General_CP1_CI_AS not in (
				select * from openquery(KRACK, 'select invoicenum from cXMLinvoicetest'))
				and ([Package Tracking No_] <> ''
				or datediff(d, [Shipment Date], getdate()) > 4)

		OPEN pilotCursor

		FETCH NEXT FROM pilotCursor INTO @pilotOrder
			

			WHILE @@FETCH_STATUS = 0

			BEGIN
			
			IF @pilotOrders <> '' 
				BEGIN 
					SET @pilotOrders = @pilotOrders+','+@pilotOrder 
				END
			ELSE 
				BEGIN
					SET @pilotOrders = @pilotOrder
				END
				
			FETCH NEXT FROM pilotCursor INTO @pilotOrder
			
			END

			CLOSE pilotCursor

			DEALLOCATE pilotCursor
			
			print @pilotOrders


		insert into openquery(KRACK, 'select invoicenum from cXMLinvoicetest')
		SELECT No_ FROM fs1.[KaTom2009].[dbo].[B&B Equipment & Supply Inc_$Sales Invoice Header]
		where [Bill-to Name] = 'PILOT'
		and datediff(d, getdate(), [Shipment Date]) > -14
		and No_ collate SQL_Latin1_General_CP1_CI_AS not in (
		select * from openquery(KRACK, 'select invoicenum from cXMLinvoicetest'))
		and ([Package Tracking No_] <> '' or datediff(d, [Shipment Date], getdate()) > 4)


		EXEC msdb.dbo.sp_send_dbmail @profile_name='katom', @recipients='mike@katom.com', @subject='Pilot Invoices Inserted', @body=@pilotOrders, @body_format ='HTML'

		END

		---

END
GO
