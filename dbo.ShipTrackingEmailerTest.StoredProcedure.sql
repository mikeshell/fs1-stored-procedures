USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[ShipTrackingEmailerTest]    Script Date: 01/30/2013 16:26:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[ShipTrackingEmailerTest] 
	-- Add the parameters for the stored procedure here
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    -- Insert statements for procedure here
/*select [Order No_], [Ship-to name], [Ship-to Address], [Ship-to City], [Ship-to ZIP Code],  TrackingNo, x.[Shipping Agent Code], Carrier, webAddress, [tracking URL]
FROM [KatomDev].[dbo].[invoice_backup] i
join [KatomDev].[dbo].[ship_backup] s
on [Order No_] COLLATE Latin1_General_CI_AS = [OrderNo] COLLATE Latin1_General_CI_AS
join shipurl x
on s.[ShippingAgentCode] = x.[Shipping Agent Code]
where [Order No_] <> ''
order by OrderNo desc


select * from shipurl

select * from [KatomDev].[dbo].[invoice_backup]
*/
/*
select top 100 * from [KatomDev].[dbo].[invoice_backup]
where [E-Mail] <> '' and [E-Mail] not in ('N/A', 'NA', '0')
*/

/*Copies information from FS1 DB and Shiptracking DB to WEBSQL db for use*/
exec invoicebackupcopy

/*Begins the loop to pull down and parse payment information*/
	DECLARE @Order varchar(20)
	DECLARE @email varchar(50)
	DECLARE @shiptoname varchar(30)
	DECLARE @shiptoadd varchar(30)
	DECLARE @shiptocity varchar(30)
	DECLARE @shiptostate varchar(10)
	DECLARE @shiptozip varchar(10)
	DECLARE @billtoname varchar(30)
	DECLARE @billtoadd varchar(30)
	DECLARE @billtocity varchar(30)
	DECLARE @billtostate varchar(10)
	DECLARE @billtozip varchar(10)
	DECLARE @phone varchar(20)
	DECLARE @trackno varchar(50)
	DECLARE @carrier varchar(50)
	DECLARE @webadd varchar(100)
	DECLARE @trackurl varchar(200)
	DECLARE @subject1 varchar(100)
	DECLARE @email1 varchar(500)
	DECLARE @body1 varchar(8000)

	DECLARE DemoCursor CURSOR FOR 
	/*select [Order No_], [E-mail], [Ship-to name], [Ship-to Address], [Ship-to City], [ship-to state], [Ship-to ZIP Code], 
	[Sell-to customer name], [Sell-to Address], [Sell-to City], [sell-to state], [Sell-to ZIP Code], [phone No_],
			TrackingNo, Carrier, webAddress, [tracking URL]*/
	SELECT [E-mail]		
	FROM [KatomDev].[dbo].[invoice_backup] i
	join [KatomDev].[dbo].[ship_backup] s
	ON [Order No_] COLLATE Latin1_General_CI_AS = [OrderNo] COLLATE Latin1_General_CI_AS
	join [KatomDev].[dbo].shipurl x
	on s.[ShippingAgentCode] = x.[Shipping Agent Code]
	WHERE [Order No_] <> '' and [E-Mail] like '%@%'
	and TrackingNo not in (select trackno from [KatomDev].[dbo].shipxref r)
	GROUP BY [e-mail]

	OPEN DemoCursor

	FETCH NEXT FROM DemoCursor INTO @email
	
	WHILE @@FETCH_STATUS = 0
	BEGIN
	
	DECLARE DemoCursor2 CURSOR FOR
	
	select [Order No_], [Ship-to name], [Ship-to Address], [Ship-to City], [ship-to state], [Ship-to ZIP Code], 
	[Sell-to customer name], [Sell-to Address], [Sell-to City], [sell-to state], [Sell-to ZIP Code], [phone No_],
			TrackingNo, Carrier, webAddress, [tracking URL]
	FROM [KatomDev].[dbo].[invoice_backup] i
	join [KatomDev].[dbo].[ship_backup] s
	ON [Order No_] COLLATE Latin1_General_CI_AS = [OrderNo] COLLATE Latin1_General_CI_AS
	join [KatomDev].[dbo].shipurl x
	on s.[ShippingAgentCode] = x.[Shipping Agent Code]
	WHERE [Order No_] <> '' and [E-Mail] = @email
	and TrackingNo not in (select trackno from [KatomDev].[dbo].shipxref r)
	
	
	OPEN DemoCursor2
	
	FETCH NEXT FROM DemoCursor2 INTO @Order, @shiptoname, @shiptoadd, @shiptocity, @shiptostate, @shiptozip,
	@billtoname, @billtoadd, @billtocity, @billtostate, @billtozip, @phone, @trackno, @carrier, @webadd, @trackURL


	print @Order
	print @email


	WHILE @@FETCH_STATUS = 0
	BEGIN
			SET @email1 = 'clutchwow@gmail.com'
			SET @subject1 = 'KaTom.com Order #'+@order+' Shipment Notification'	
			SET @body1='<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"
"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
<title>E-Mail</title>

<style type="text/css">
html{font-family:Lucida Grande;}
</style>

</head>
<body>
<a href="http://www.katom.com">
<img width="194" height="60" title="KaTom Restaurant Supply Inc." alt="KaTom Restaurant Supply Inc." src="http://www.katom.com/Merchant2/css/sterile/seasonallogo.gif" border="0"><br />
<span style="
font-family:Arial;
font-size:11px;
font-variant:small-caps;
font-weight:400;
left:4px;
letter-spacing:2px;
margin:0;
padding:0 0 0 1px;
position:relative;
top:-1px;
text-decoration:underline
">
RESTAURANT SUPPLY, INC.
</span>
</a>
<table border="0" width="632" style="font-family:Lucida Grande; font-weight:100; background:#585757;color:#fff;" cellpadding="2">
<tr>
<td width="10px;"></td>
<td colspan="2">
<br />
<span style="font-size:72px; color:#959595;">ON ITS WAY...</span>
</td>
</tr>
<tr>
<td width="10px;"></td>
<td>
<span style="color:#e9ba20; font-size:30px; font-weight:600px">Good news &mdash; your package has shipped.</span>
<p style="color:#fff; font-size:10px; font-weight:100; font-family:verdana;">Your package from <a href="http://www.katom.com" style="color:#FFF">Katom Restaurant Supply</a> is on its way.  Here''s your shipping confirmation:</p>
</td>
</tr>
<tr>
<td width="10px;"></td>
<td colspan="2"><span style="color:#e9ba20;
font-family:lucida grande;
font-weight:600;">ORDER NUMBER.</span> <span style="color:#fff; font-size:12px; font-weight:100; font-family:verdana;">'+@order+'</span></td>
</tr>
<tr>
<td width="10px;"></td>
<td colspan="2"><span style="color:#e9ba20;
font-family:lucida grande;
font-weight:600;">SHIPPING DATE.</span> <span style="color:#fff; font-size:12px; font-weight:100; font-family:verdana;">'+convert(varchar,datepart(mm,getdate())) + '.' + convert(varchar,datepart(dd,getdate())) + '.' + convert(varchar,datepart(yyyy,getdate()))+'</span></td>
</tr>
<tr>
<td width="10px;"></td>
<td colspan="2"><span style="color:#e9ba20;
font-family:lucida grande;
font-weight:600;">TRACKING NUMBER.</span> <a href="'+REPLACE(@trackurl, '<%trackingNum%>', @trackno)+'" style="color:#FFF">'+@trackno+'</a>
<br />
<br />
</td>
</tr>
</table>
<table cellspacing="0">
<tr height="8">
<td>
</td>
</tr>
<tr>
<td>
<a href="http://www.katom.com/blog/of-interest/3767/katoms-having-a-recipe-contest">
<img src="http://www.katom.com/images/shunGiveaway.gif" />
</a>
</td>
<td width="9">
</td>
<td>
<a href="http://www.katom.com/vendor/shun.html?intcmp=trkeml">
<img src="http://www.katom.com/images/shunSpecial.gif" />
</a>
</td>
</tr>
<table>
<tr>
<td height="4px">
</td>
</tr>
</table>
</table>
<table width="625" style="border-color:#000; border-style:solid; border-width:1px;">
<tr>
<td>
<table width="625" border="0" cellpadding="0" cellspacing="0" style="font-family:Verdana, Geneva, sans-serif; font-size:10px">
<tr>
<td width="9px" style="background:#999; font-weight:600; color:#666">
</td>
<td colspan="2" style="background:#999; font-weight:600; color:#666; font-size:14px">
ORDER INFORMATION
</td>
</tr>
<tr>
<td height="9px">
</td>
</tr>
<tr>
<td width="9px"></td>
<td style="font-size:12px; font-weight:600">
BILLING INFORMATION:
</td>
<td style="font-size:12px; font-weight:600">
SHIPPING INFORMATION:
</td>
</tr>
<tr>
<td width="9px"></td>
<td>
<table cellpadding="0" cellspacing="0" style="font-family:Verdana, Geneva, sans-serif; font-size:10px">
<tr><td>'+@billtoname+'</td></tr>
<tr><td>'+@billtoadd+'</td></tr>
<tr><td>'+@billtocity+', '+@billtostate+' '+@billtozip+'</td></tr>
<tr><td>Primary Phone: '+@phone+'</td></tr>
<tr><td>Email: '+@email+'</td></tr>
</table>
</td>
<td valign="top">
<table cellpadding="0" cellspacing="0" style="font-family:Verdana, Geneva, sans-serif; font-size:10px">
<tr><td>'+@shiptoname+'</td></tr>
<tr><td>'+@shiptoadd+'</td></tr>
<tr><td>'+@shiptocity+', '+@shiptostate+' '+@shiptozip+'</td></tr>
</table>
</td>
</tr>
</table>
</td>
</tr>
</tr>
<tr>
<td height="4px" style="font-family:Verdana, Geneva, sans-serif; font-size:10px">
Note:  Tracking information may not be available for up to 48 hours after an item is shipped.  Please do not reply to this email as it is delivered from an unmonitored address.  If you need further assistance, please <a href="http://www.katom.com/trackmyorder.php">click here</a> for help.
</td>
</tr>
</table>
</body>
</html>'
			
			--SET @body1 = 'Click this '+@trackurl+REPLACE(@trackno, '<%trackingNum%>', @Order)
	/*		EXEC msdb.dbo.sp_send_dbmail @profile_name='katom',
				@recipients=@email2, --CHANGED FOR TEST
				@subject=@subject1,
				@body=@body1,
				@body_format ='HTML',
				@blind_copy_recipients='mike@katom.com'
				
			insert into shipxref (trackno, sent) values (@trackno, 1)
	*/
	
	FETCH NEXT FROM DemoCursor2 INTO @Order, @shiptoname, @shiptoadd, @shiptocity, @shiptostate, @shiptozip,
	@billtoname, @billtoadd, @billtocity, @billtostate, @billtozip, @phone, @trackno, @carrier, @webadd, @trackURL
	
	--Print @Order+' '+@shiptoname+' '+@shiptoadd+' '+@shiptocity+' '+@shiptostate+' '+@shiptozip+' '+
	--@billtoname+' '+@billtoadd+' '+@billtocity+' '+@billtostate+' '+@billtozip+' '+@phone+' '+@trackno+' '+
	--@carrier+' '+@webadd+' '+@trackURL

	END
	
	CLOSE DemoCursor2
	
	DEALLOCATE DemoCursor2
	
	FETCH NEXT FROM DemoCursor INTO @email

	END
	
	
	CLOSE DemoCursor

	
	
	DEALLOCATE DemoCursor

/*Reset
select * from shipxref where trackno = '053260560918289 '
delete from shipxref where id = 15628
*/
END
GO
