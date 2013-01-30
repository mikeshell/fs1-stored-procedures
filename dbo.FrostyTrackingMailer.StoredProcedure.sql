USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[FrostyTrackingMailer]    Script Date: 01/30/2013 16:26:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[FrostyTrackingMailer] 
	-- Add the parameters for the stored procedure here
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

/*Begins the loop to pull down and parse payment information*/
	DECLARE @OuterOrder varchar(50)
	DECLARE @Order varchar(50)
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
	DECLARE @delivereddate varchar(255)
	DECLARE @shipdate varchar(255)
	DECLARE @delivered varchar(255)
	DECLARE @emailupdate bit
	DECLARE @trackingscan varchar(255)
	DECLARE @signby varchar(255)
	DECLARE @company varchar(255)
	DECLARE @FAmemo varchar(255)
	DECLARE @FAemail varchar(255)
	
	DECLARE OuterEmailCursor CURSOR FOR
	
	select top 100 [Order No_]
	FROM [KatomDev].[dbo].[invoice_backup] i
	join [KatomDev].[dbo].[trackingInfo] s
	on katomorderid COLLATE Latin1_General_CI_AS = [Order No_] COLLATE Latin1_General_CI_AS
	left outer join [KatomDev].[dbo].shipurl x
	on x.[shipping agent code] = s.carrier
	where katomorderid <> ''
	and NOT ([Delivered] = 'True' and emailUpdate = 0)
	and emailupdate = 1
	and datediff(d, getdate(), [ship date]) > -14
	and [Bill-to Name] like 'FROSTY ACRES%'
	group by [Order No_]
	order by [Order No_] desc
		
	OPEN OuterEmailCursor
	
	FETCH NEXT FROM OuterEmailCursor INTO @OuterOrder

	WHILE @@FETCH_STATUS = 0
	BEGIN

			DECLARE DemoCursor CURSOR FOR 
			
			select top 1 [Order No_], i.[E-mail], [Ship-to name], [Ship-to Address], [Ship-to City], [ship-to county], [Ship-to post Code], 
			[Sell-to customer name], [Sell-to Address], [Sell-to City], [sell-to county], [Sell-to post Code], [phone No_], 
			x.Carrier, webAddress, [tracking URL], [Delivered Date], [Ship Date], Delivered, 
			emailupdate, [tracking scan], [sign by], [Shortcut Dimension 2 Code], [Your Reference]
			FROM [KatomDev].[dbo].[invoice_backup] i
			join [KatomDev].[dbo].[trackingInfo] s
			on katomorderid COLLATE Latin1_General_CI_AS = [Order No_] COLLATE Latin1_General_CI_AS
			left outer join [KatomDev].[dbo].shipurl x
			on x.[shipping agent code] = s.carrier
			where [Order No_] = @OuterOrder
			order by [Order No_] desc

			OPEN DemoCursor

			FETCH NEXT FROM DemoCursor INTO @Order, @email, @shiptoname, @shiptoadd, @shiptocity, @shiptostate, @shiptozip,
			@billtoname, @billtoadd, @billtocity, @billtostate, @billtozip, @phone, @carrier, @webadd, @trackURL, @delivereddate,
			@shipdate, @delivered, @emailupdate, @trackingscan, @signby, @company, @FAmemo

			WHILE @@FETCH_STATUS = 0
			BEGIN
			
			
					print @Order
					print @email
					SET @email1 = @email
					IF @delivered = 'FALSE' BEGIN SET @subject1 = 'Order #'+@order+' Shipment Notification'	END
					ELSE IF @delivered = 'TRUE' BEGIN SET @subject1 = 'Order #'+@order+' Delivery Notification'	END
					ELSE IF @delivered is null BEGIN SET @subject1 = 'Order #'+@order+' Tracking Notification'	END
					ELSE BEGIN SET @subject1 = 'X' END
					print @delivered+' '+cast(@emailupdate as varchar(1))+' = '+@subject1
					
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
									<body>'
					IF @company = 'KATOM' 
							BEGIN 
								SET @body1=@body1+'<a href="http://www.katom.com">
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
									</a>' 
									
							END
					SET @body1=@body1+'<table border="0" width="632" style="font-family:Lucida Grande; font-weight:100; background:#585757;color:#fff;" cellpadding="2">
									<tr>
									<td width="10px;"></td>
									<td colspan="2">
									<br />'
			
			IF (@delivered = 'FALSE' or @delivered is null) and @emailupdate = 1 SET @body1 = @body1+'<span style="font-size:72px; color:#959595;">ON ITS WAY...</span>'
			IF @delivered = 'TRUE' and @emailupdate = 1 SET @body1 = @body1+'<span style="font-size:72px; color:#959595;">CONGRATULATIONS...</span>'
			
					SET @body1 = @body1+'</td>
						</tr>
						<tr>
						<td width="10px;"></td>
						<td>'
						
			IF (@delivered = 'FALSE' or @delivered is null) and @emailupdate = 1 and @company = 'KATOM' SET @body1 = @body1+'			
						<span style="color:#e9ba20; font-size:30px; font-weight:600px">Good news &mdash; your package has shipped.</span>
						<p style="color:#fff; font-size:10px; font-weight:100; font-family:verdana;">Your package from <a href="http://www.katom.com" style="color:#FFF">Katom Restaurant Supply</a> is on its way.  Here''s your shipping confirmation:</p>'

			IF @delivered = 'TRUE' and @emailupdate = 1 and @company = 'KATOM' SET @body1 = @body1+'			
						<span style="color:#e9ba20; font-size:18px; font-weight:600px">The carrier has indicated that your package has been delivered.</span>'
			
			IF (@signby <> 'N/A' and @signby is not null) and @company = 'KATOM' SET @body1 = @body1+'			
						<p style="color:#fff; font-size:10px; font-weight:100; font-family:verdana;">Your package was signed for by: '+@signby+'</p>'
						
					SET @body1 = @body1+'
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
						font-weight:600;">SHIPPING DATE.</span> <span style="color:#fff; font-size:12px; font-weight:100; font-family:verdana;">'+convert(varchar,datepart(mm,@shipdate)) + '.' + convert(varchar,datepart(dd,@shipdate)) + '.' + convert(varchar,datepart(yyyy,@shipdate))+'</span></td>
						</tr>'
						
							DECLARE TrackingCursor CURSOR FOR
							
							select [Tracking Number], [tracking url] from trackingInfo i
							left outer join [KatomDev].[dbo].shipurl x
							on x.[shipping agent code] = i.carrier
							where KatomOrderID = @order
							
							OPEN TrackingCursor
							
							FETCH NEXT FROM TrackingCursor INTO @trackno, @trackURL

							WHILE @@FETCH_STATUS = 0
							BEGIN
							
							print @order+'-'+@trackno
								
							SET @body1 = @body1+'<tr>
								<td width="10px;"></td>
								<td colspan="2"><span style="color:#e9ba20;
								font-family:lucida grande;
								font-weight:600;">TRACKING NUMBER.</span> <a href="'+REPLACE(@trackurl, '<%trackingNum%>', @trackno)+'" style="color:#FFF">'+@trackno+'</a>
								</td>
								</tr>'
								
							IF @subject1 <> 'X' Update trackingInfo set emailUpdate = 0 where [Tracking Number] = @trackno

							FETCH NEXT FROM TrackingCursor INTO @trackno, @trackurl

							END

							CLOSE TrackingCursor

							DEALLOCATE TrackingCursor
						
						
						
					SET @body1 = @body1+'</table>
						<table cellspacing="0">
						<tr height="8">
						<td>
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
						Note:  Tracking information may not be available for up to 48 hours after an item is shipped.  Please do not reply to this email as it is delivered from an unmonitored address. '
						
						IF @company = 'KATOM' SET @body1 = @body1+'If you need further assistance, please <a href="http://www.katom.com/trackmyorder.php">click here</a> for help.'
						
						SET @body1 = @body1+'</td>
						</tr>
						</table>
						</body>
						</html>'
					
			print @subject1

			select top 1 @email1 = approval from openquery(RDN, 'SELECT Email, approval, FAmemo, memberPO
												FROM FA_Pending p
												JOIN FAmembers m
												ON memberid = m.id
												JOIN FAapprovals a
												ON m.membergroup = a.membergroup')
							where FAmemo = @FAmemo
							or memberPO = @FAmemo
				
			IF @billtoname like '%merchants%' BEGIN SET @email1 = 'tchandler@merchantsfoodservice.com' END
	
			IF @subject1 <> 'X' and @body1 <> ''
				BEGIN
						IF (upper(@delivered) = 'FALSE' OR @delivered is null) and @emailupdate = 1 and @company = 'KATOM' BEGIN EXEC msdb.dbo.sp_send_dbmail @profile_name='katom', @recipients=@email1,	@subject=@subject1,	@body=@body1, @body_format ='HTML',	@blind_copy_recipients='katom.archive@gmail.com' PRINT 'First Email sent' END
						IF upper(@delivered) = 'TRUE' and @emailupdate = 1 and @company = 'KATOM' BEGIN EXEC msdb.dbo.sp_send_dbmail @profile_name='katom', @recipients=@email1,	@subject=@subject1,	@body=@body1, @body_format ='HTML',	@blind_copy_recipients='katom.archive@gmail.com' PRINT 'Delivered status Email Sent' END
						IF (upper(@delivered) = 'FALSE' OR @delivered is null) and @emailupdate = 1 and @company <> 'KATOM' BEGIN EXEC msdb.dbo.sp_send_dbmail @profile_name='B&B', @recipients=@email1,	@subject=@subject1,	@body=@body1, @body_format ='HTML',	@blind_copy_recipients='katom.archive@gmail.com' PRINT 'First Email sent' END
						IF upper(@delivered) = 'TRUE' and @emailupdate = 1 and @company <> 'KATOM' BEGIN EXEC msdb.dbo.sp_send_dbmail @profile_name='B&B', @recipients=@email1,	@subject=@subject1,	@body=@body1, @body_format ='HTML',	@blind_copy_recipients='katom.archive@gmail.com' PRINT 'Delivered status Email Sent' END
					ELSE print 'no update'
				END
			
			FETCH NEXT FROM DemoCursor INTO @Order, @email, @shiptoname, @shiptoadd, @shiptocity, @shiptostate, @shiptozip,
			@billtoname, @billtoadd, @billtocity, @billtostate, @billtozip, @phone, @carrier, @webadd, @trackURL, @delivereddate,
			@shipdate, @delivered, @emailupdate, @trackingscan, @signby, @company, @FAmemo

			END

			CLOSE DemoCursor

			DEALLOCATE DemoCursor
	
	FETCH NEXT FROM OuterEmailCursor INTO @OuterOrder

	END

	CLOSE OuterEmailCursor

	DEALLOCATE OuterEmailCursor

END
GO
