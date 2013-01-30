USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[ConfirmationEmailerbkp12-4-11]    Script Date: 01/30/2013 16:26:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[ConfirmationEmailerbkp12-4-11] 
	-- Add the parameters for the stored procedure here
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;


/*Begins the loop to pull down and parse payment information*/
	DECLARE @selltoname varchar(30)
	DECLARE @selltoaddress varchar(30)
	DECLARE @selltoaddress2 varchar(30)
	DECLARE @selltocity varchar(30)
	DECLARE @selltostate varchar(10)
	DECLARE @selltozip varchar(10)
	DECLARE @shiptoname varchar(30)
	DECLARE @shiptoaddress varchar(30)
	DECLARE @shiptoaddress2 varchar(30)
	DECLARE @shiptocity varchar(30)
	DECLARE @shiptostate varchar(10)
	DECLARE @shiptozip varchar(10)
	DECLARE @orderdate varchar(255)
	DECLARE @shipmethod varchar(200)
	DECLARE @phone varchar(20)
	DECLARE @email varchar(50)
	DECLARE @confnum varchar(20)
	DECLARE @email1 varchar(500)
	DECLARE @Navorder varchar(500)
	DECLARE @order varchar(500)
	DECLARE @subject1 varchar(150)
	DECLARE @body1 varchar(MAX)
	DECLARE @prodcode varchar(50)
	DECLARE @prodqty varchar(50)
	DECLARE @prodimg varchar(100)
	DECLARE @prodname varchar(200)
	DECLARE @prodprice varchar(100)
	DECLARE @prodtotal varchar(100)
	DECLARE @total varchar(100)
	DECLARE @shipping varchar(100)
	DECLARE @tax varchar(100)
	DECLARE @ordertotal varchar(100)

	
	DECLARE OrderCursor CURSOR FOR 
	
	select
		h.No_ as NavOrdNo, 
		[sell-to customer name] as selltoname, 
		[sell-to address] as selltoaddress, 
		[sell-to address 2] as selltoaddress2, 
		[sell-to city] as selltocity, 
		[sell-to county] as selltostate, 
		[sell-to post code] as selltozip, 
		[ship-to name] as shiptoname, 
		[Ship-to Address] as shiptoaddress, 
		[Ship-to Address 2] as shiptoaddress2, 
		[Ship-to City] as shiptocity, 
		[ship-to county] as shiptostate, 
		[ship-to post code] as shiptozip, 
		[order date] as orderdate, 
		[Shipment Method Code] as shipmethod, 
		[Phone No_] as phone, 
		[e-mail] as email, 
			Confnum = 
			CASE [Web Order No_]
				WHEN '' THEN 'NAV-'+h.No_
				ELSE [Web Order No_]
			END,
		(select cast(sum(quantity*[unit price]) as decimal(8,2))
		FROM fs1.[katom2009].[dbo].[B&B Equipment & Supply Inc_$Sales Line]
					join products
					on No_ COLLATE SQL_Latin1_General_CP1_CI_AS = code COLLATE SQL_Latin1_General_CP1_CI_AS
					--where [Document No_] = the order
					where [Document No_] = h.No_) as total,
		(SELECT cast(sum(amount ) as decimal(8,2))
		FROM fs1.[katom2009].[dbo].[B&B Equipment & Supply Inc_$Sales Line]
		where [Document No_] = h.No_ 
		and No_ in ('420', '100-SHIPPING')) as shipping,
		Tax =
		CASE [Tax Area Code]
		WHEN 'TN TAXABLE' THEN
		(select cast(sum([amount including VAT])-sum(amount) as decimal(8,2))
		FROM fs1.[katom2009].[dbo].[B&B Equipment & Supply Inc_$Sales Line]
					--where [Document No_] = the order
					where [Document No_] = h.No_)
		ELSE 0
		END
		FROM fs1.[katom2009].[dbo].[B&B Equipment & Supply Inc_$Sales Header] h
		where [shortcut dimension 2 code] = 'KATOM'
		and h.No_ not in (select confno COLLATE Latin1_General_CI_AS from ordconfxref)
		and [e-mail] like '%@%'
		and [no_ printed] > 0
		--and [web order no_] = ''
		and (SELECT COUNT(*)
					FROM fs1.[katom2009].[dbo].[B&B Equipment & Supply Inc_$Sales Line]
					join products
					on No_ COLLATE SQL_Latin1_General_CP1_CI_AS = code COLLATE SQL_Latin1_General_CP1_CI_AS
					--where [Document No_] = the order
					where [Document No_] = h.No_
					and No_ not in ('', '420', '100-SHIPPING')) > 0
					and [No_ Series] <> 'S-QUO'
					and h.No_ not like '%-%'
		AND [e-mail] <> 'mccolunga@novanthealth.org'
		order by h.No_ desc

	OPEN OrderCursor

	FETCH NEXT FROM OrderCursor INTO @Navorder, @selltoname, @selltoaddress, @selltoaddress2, @selltocity, @selltostate, @selltozip, @shiptoname, 
		@shiptoaddress, @shiptoaddress2, @shiptocity, @shiptostate, @shiptozip, @orderdate, @shipmethod, @phone, @email, @confnum, @total, @shipping, @tax
	
	WHILE @@FETCH_STATUS = 0
	BEGIN
	
			print @selltoname
			print @confnum
			SET @orderdate = CONVERT(varchar(11), @orderdate, 101)
			IF @orderdate like '%1753%' SET @orderdate = CONVERT(varchar(11), getdate(), 101)
			SET @ordertotal = cast(@total as decimal(8,2)) + cast(@shipping as decimal(8,2)) + cast(@tax as decimal(8,2))
			SET @email1 = @email
			SET @subject1 = 'KaTom.com Order #'+@confnum+' Confirmation'	
			SET @body1='<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"
							"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd"><html xmlns="http://www.w3.org/1999/xhtml"><head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
							<title>E-Mail</title>

							<style type="text/css">
							html{font-family:Lucida Grande;}
							</style>

							</head>
<body>

<table width="710" border="0" style="font-family:sans-serif;background-color:#fff;">
  <tr>
    <td colspan="3"><p style="color:#aaaaaa;font-size:12px;">
<!--Are you having difficulty viewing our HTML email? <a title="View this email in a browser window" href="http://www.katom.com/Merchant2/email_price_browser_window.php?a=$a">View this email in a browser window</a>-->
</p>
</td>
  </tr>
  <tr>
    <td colspan="3">
	<h1><a href="http://www.katom.com" title="KaTom Restaurant Supply"><img src="http://www.katom.com/Merchant2/images/KaTom_Seasonal_Logo_FNL_72dpi.jpg" 	width="200" height="75"	border="none" alt="KaTom Restaurant Supply Inc." title="KaTom Restaurant Supply Inc."/></a></h1>
	<a style="font-size:10px;color: black;font-weight:bold;text-decoration: none;" href="http://www.katom.com/Restaurant-equipment.html" title="Restauant-Equipment">Restaurant Equipment</a>&nbsp;|&nbsp;
	<a style="font-size:10px;color: black;font-weight:bold;text-decoration: none;" href="http://www.katom.com/countertop.html" title="Countertop">Countertop</a>&nbsp;|&nbsp;
	<a style="font-size:10px;color: black;font-weight:bold;text-decoration: none;" href="http://www.katom.com/janitorial.html" title="Janitorial">Janitorial</a>&nbsp;|&nbsp;
	<a style="font-size:10px;color: black;font-weight:bold;text-decoration: none;" href="http://www.katom.com/kitchen-supplies.html" title="Kitchen Supplies">Kitchen Supplies</a>&nbsp;|&nbsp;
	<a style="font-size:10px;color: black;font-weight:bold;text-decoration: none;" href="http://www.katom.com/tabletop.html" title="Tabletop">Tabletop</a>&nbsp;|&nbsp;
	<a style="font-size:10px;color: black;font-weight:bold;text-decoration: none;" href="http://www.katom.com/furniture.html" title="Furniture">Furniture</a>&nbsp;|&nbsp;
	<a style="font-size:10px;color: black;font-weight:bold;text-decoration: none;" href="http://www.katom.com/bar-supplies.html" title="Bar-Supplies">Bar Supplies</a>&nbsp;|&nbsp;
	<a style="font-size:10px;color: #a91c20;font-weight:bold;text-decoration: none;" href="http://www.katom.com/clearance-sale.html"  title="clearance-sale">Clearance Sale</a>&nbsp;|&nbsp;
	<a style="font-size:10px;color: #025eaa;font-weight:bold;text-decoration: none;" href="http://www.katom.com/residential.html"  title="Residential">Residential</a>

	
</td>
  </tr>
<tr>
	<td colspan="4">
    <table width="710" border="0" style="border-width: 1px;
	padding: 1px;
	border-style:solid;
	border-color: gray;">
          <tr>
			  <td width="10" colspan="1"></td>
              <td width="429" colspan="1">
                    <span style="font-family:Tahoma, Geneva, sans-serif; font-size:18px; color:#06C">Dear '+@selltoname+'</span>
              </td>
                <td width="349" colspan="2" align="right" valign="middle">
                    <img src="http://www.katom.com/images/mailericons/confirmation.png" alt="Confirmation" border="none" align="absmiddle" title="Order Confirmed"/>
              </td>
              <td>
              </td>
          </tr>
          <tr>
            <td width="10" colspan="1"></td>
          	<td colspan="3">
            <span style="font-family:Tahoma, Geneva, sans-serif; font-size:12px;" >
              <p>Thank you for shopping at KaTom Restaurant Supply.              </p>
              <p>We have received your order.  Please allow 24 - 48 hours to have it processed.  We greatly appreciate your business and patience.  
                Please keep a copy for your Records.            </p>
              <p>If you have any questions, you can reach our Customer Appreciation Team through email at sales@katom.com, Live Chat with us, 
                or call us at 1-800-541-8683 Monday - Friday, 8:00 am - 8:00 pm.                </p>
              <p>Order Confirmation Number: <span style="font-size:12px; font-weight:bold;">'+@confnum+'</span><br />
                Sales Order Date: <span style="font-size:12px; font-weight:bold;">'+@orderdate+' </span></p>
			</span>
            </td>
            <td>
            </td>
          </tr>
          <tr>
			<td width="10" colspan="1"></td>
          	<td colspan="4" bgcolor="#2E59CB" height="29">
            <span style="font-size:16px; color:#FFF; font-weight:bold">&nbsp;Contact Information</span>
            </td>
          </tr>
          <tr>
			<td width="10" colspan="1"></td>
          	<td height="103" colspan="4" valign="top" align="left">
            <table width="625" border="0" cellpadding="0" cellspacing="0" style="font-family:Verdana, Geneva, sans-serif; font-size:12px">
				<tr>
				<td height="9px">
				</td>
				</tr>
				<tr>
				<td width="9px"></td>
				<td style="font-size:12px; font-weight:600">
				&nbsp;Billing Address:
				</td>
				<td style="font-size:12px; font-weight:600">
				Shipping Address:
				</td>
				</tr>
				<tr>
				<td width="9px"></td>
				<td>
				<table cellpadding="0" cellspacing="0" style="font-family:Verdana, Geneva, sans-serif; font-size:12px">
				<tr><td>&nbsp;'+@selltoname+'</td></tr>
				<tr><td>&nbsp;'+@selltoaddress+'</td></tr>
				<tr><td>&nbsp;'+@selltocity+', '+@selltostate+' '+@selltozip+'</td></tr>
				<tr><td>&nbsp;Primary Phone: '+@phone+'</td></tr>
				<tr><td>&nbsp;Email: '+@email+'</td></tr>
				</table>
				</td>
				<td valign="top">
				<table cellpadding="0" cellspacing="0" style="font-family:Verdana, Geneva, sans-serif; font-size:12px">
				<tr><td>'+@shiptoname+'</td></tr>
				<tr><td>'+@shiptoaddress+'</td></tr>
				<tr><td>'+@shiptocity+', '+@shiptostate+' '+@shiptozip+'</td></tr>
				</table>
				</td>
				</tr>
				</table>
            </td>
          </tr>
          
          <tr style="font-family:Tahoma, Geneva, sans-serif; font-size:12px;">
          	<td width="10" colspan="1"></td>
          	<td colspan="4">
				At KaTom Restaurant Supply, we believe “It’s About You”.  Our dedicated team thrives to ensure that your shopping experience at KaTom.com should be awesome!<br /><br />
				Please <a href="mailto:sales@katom.com">let us know</a> if we can make your shopping experience any better!<br /><br />
            </td>
          </tr>
          <tr>
			<td width="10" colspan="1"></td>
          	<td style="font-family:Tahoma, Geneva, sans-serif; font-size:12px; font-weight:800">
            	Contact The KaTom Customer Appreciation Team:<br />
            </td>
          </tr>
          <tr>
				<td width="10" colspan="1"></td>
                <td colspan="3">
                    <Table style="font-family:Tahoma, Geneva, sans-serif; font-size:12px;">
                        <tr>
                            <td style="font-weight:600">
                                Email:
                            </td>
                            <td>
                                sales@katom.com
                            </td>
                        </tr>
                        <tr>
                            <td>&nbsp;
                                
                            </td>
                            <td>
                                Live Chat Monday - Friday, 8:00 am - 8:00 pm
                        	</td>
                        </tr>
                        <tr>
                            <td style="font-weight:600">
                                Phone
                            </td>
                            <td>
                                1-800-541-8683 Monday - Friday, 8:00 am - 8:00 pm
                        	</td>
                        </tr>
                        <tr>
                            <td style="font-weight:600">
                                Website
                            </td>
                            <td>
                                http://www.katom.com
                        	</td>
                        </tr>
   		           </Table>
              </td>
          </tr>
		</table>
    </td>
</tr>
</table>
</body>
				</html>'

	
	PRINT @Navorder+'on '+@orderdate
	
	IF @body1 <> '' BEGIN Insert into ordconfxref (confno, orderdate, [type]) values (@Navorder, @orderdate, 'NAV') PRINT 'Confirmation Email sent-------' END

	IF @body1 <> '' EXEC msdb.dbo.sp_send_dbmail @profile_name='katom', @recipients=@email1,	@subject=@subject1,	@body=@body1, @body_format ='HTML',	@blind_copy_recipients='katom.archive@gmail.com'	

	FETCH NEXT FROM OrderCursor INTO @Navorder, @selltoname, @selltoaddress, @selltoaddress2, @selltocity, @selltostate, @selltozip, @shiptoname, 
		@shiptoaddress, @shiptoaddress2, @shiptocity, @shiptostate, @shiptozip, @orderdate, @shipmethod, @phone, @email, @confnum, @total, @shipping, @tax

	END

	CLOSE OrderCursor

	DEALLOCATE OrderCursor

END
GO
