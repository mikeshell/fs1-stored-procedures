USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[catPerformanceWeekly_EMAIL]    Script Date: 01/30/2013 16:26:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[catPerformanceWeekly_EMAIL]
AS
BEGIN
	DECLARE @catname VARCHAR(50), @w1 MONEY, @w2 MONEY, @w3 MONEY, @w4 MONEY, @w5 MONEY, @w6 MONEY, 
			@o1 INT, @o2 INT, @o3 INT, @o4 INT, @o5 INT, @o6 INT, 
			@wa MONEY, @wao INT, @m MONEY, @mo INT, @mp MONEY, @l MONEY, @lo INT,
			@w1tt MONEY, @w2tt MONEY, @w3tt MONEY, @w4tt MONEY, @w5tt MONEY, @w6tt MONEY, 
			@o1tt INT, @o2tt INT, @o3tt INT, @o4tt INT, @o5tt INT, @o6tt INT, @prodcountTT INT,
			@watt MONEY, @waott INT, @mtt MONEY, @mott INT, @mptt MONEY, @prodcount INT, @startDT datetime,
			@today DATETIME, @yyyy SMALLINT, @dd TINYINT, @mm TINYINT, @counter SMALLINT, @bgcolor VARCHAR(50), @fontcolor VARCHAR(50)
			
	DECLARE @body1 VARCHAR(MAX), @email1 varchar(500), @subject1 varchar(150)
				
	SET @w1tt = 0 
	SET @w2tt = 0 
	SET @w3tt = 0 
	SET @w4tt = 0 
	SET @w5tt = 0 
	SET @w6tt = 0 
	SET @o1tt = 0
	SET @o2tt = 0
	SET @o3tt = 0
	SET @o4tt = 0
	SET @o5tt = 0
	SET @o6tt = 0
	SET @watt = 0
	SET @waott = 0
	SET @mtt = 0
	SET @mott = 0
	SET @mptt = 0	
	SET @prodcountTT = 0		
	
	SET @counter = 1
	SET @bgcolor = '#bed6e9'

	--INITIATING DATE
	SET @today = GETDATE()
	SET @yyyy = YEAR(@today)
	SET @mm = MONTH(@today)
	SET @dd = DAY(@today)
	SET @startDT = CAST(CAST(DATEADD(DAY, -6-DATEPART(WEEKDAY, @today), @today) AS VARCHAR(11)) AS DATETIME)

	--INITIATE TABLE
	SET @body1 = '<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"
	"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
	<html xmlns="http://www.w3.org/1999/xhtml">
	<head>
	<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
	<title>E-Mail</title>

	<style type="text/css">
	body,td,th {font-family: Arial, Helvetica, sans-serif;font-size: 10px; vertical-align: top; }
	</style>

	</head>
	<body>
	<table border="0" cellspacing="0" cellpadding="3" style="border:solid 1px #000000;">
	  <tr style="background-color:#446fb7; color:#FFFFFF; font-weight: bold;">
		<td style="width: 100px;">catname</td>
		<td style="text-align:center; width: 100px;">ProdCount</td>
		<td style="text-align:center; width: 140px;" colspan="2">'+ CAST(DATEADD(WEEK, -5, @startDT) AS VARCHAR(11)) + '</td>
		<td style="text-align:center; width: 140px;" colspan="2">'+ CAST(DATEADD(WEEK, -4, @startDT) AS VARCHAR(11)) + '</td>
		<td style="text-align:center; width: 140px;" colspan="2">'+ CAST(DATEADD(WEEK, -3, @startDT) AS VARCHAR(11)) + '</td>
		<td style="text-align:center; width: 140px;" colspan="2">'+ CAST(DATEADD(WEEK, -2, @startDT) AS VARCHAR(11)) + '</td>
		<td style="text-align:center; width: 140px;" colspan="2">'+ CAST(DATEADD(WEEK, -1, @startDT) AS VARCHAR(11)) + '</td>
		<td style="text-align:center; width: 140px;" colspan="2">'+ CAST(@startDT AS VARCHAR(11)) + '</td>
		<td style="text-align:center; width: 140px;" colspan="2">6 WK AVG</td>
		<td style="text-align:center; width: 140px;" colspan="2">MTD</td>
		<td style="text-align:center; width: 70px;">Monthly Goal</td>
	  </tr>
	  <tr style="background-color:#446fb7; color:#FFFFFF; font-weight: bold;">
		<td style="text-align:center; width: 100px;">&nbsp;</td>
		<td style="text-align:center; width: 100px;">&nbsp;</td>
		<td style="text-align:center; width: 70px;">Sales</td>
		<td style="text-align:center; width: 70px;">Order</td>
		<td style="text-align:center; width: 70px;">Sales</td>
		<td style="text-align:center; width: 70px;">Order</td>
		<td style="text-align:center; width: 70px;">Sales</td>
		<td style="text-align:center; width: 70px;">Order</td>
		<td style="text-align:center; width: 70px;">Sales</td>
		<td style="text-align:center; width: 70px;">Order</td>
		<td style="text-align:center; width: 70px;">Sales</td>
		<td style="text-align:center; width: 70px;">Order</td>
		<td style="text-align:center; width: 70px;">Sales</td>
		<td style="text-align:center; width: 70px;">Order</td>
		<td style="text-align:center; width: 70px;">Sales</td>
		<td style="text-align:center; width: 70px;">Order</td>
		<td style="text-align:center; width: 70px;">Sales</td>
		<td style="text-align:center; width: 70px;">Order</td>
		<td style="text-align:center; width: 70px;">&nbsp;</td>
	  </tr>'
	  
	DECLARE sCursor CURSOR FOR 
	SELECT * FROM catPerformance_RPT WHERE catName <> 'Others'

	OPEN sCursor
	FETCH NEXT FROM sCursor INTO @catName, @prodcount, @w1,  @o1, @w2, @o2, @w3, @o3, @w4, @o4, @w5, @o5, @w6, @o6, @wa, @wao, @m, @mo, @mp, @l, @lo

	WHILE @@FETCH_STATUS = 0
	   BEGIN		
		SET @w1tt =  @w1tt + @w1
		SET @w2tt =  @w2tt + @w2
		SET @w3tt =  @w3tt + @w3
		SET @w4tt =  @w4tt + @w4
		SET @w5tt =  @w5tt + @w5
		SET @w6tt =  @w6tt + @w6
		SET @o1tt =  @o1tt + @o1
		SET @o2tt =  @o2tt + @o2
		SET @o3tt =  @o3tt + @o3
		SET @o4tt =  @o4tt + @o4
		SET @o5tt =  @o5tt + @o5
		SET @o6tt =  @o6tt + @o6
		SET @watt = @watt + @wa
		SET @waott = @waott + @wao
		SET @mtt = @mtt + @m
		SET @mott = @mott + @mo
		SET @mptt = @mptt + @mp
		SET @prodcountTT = @prodcountTT + @prodcount
		
		IF @counter = 1
		   BEGIN
			SET @counter = 2
			SET @bgcolor = '#FFFFFF'
		   END
		ELSE
		   BEGIN
			SET @counter = 1
			SET @bgcolor = '#bed6e9'
		   END
		
		SET @body1 = @body1 + ' <tr style="background-color:' + @bgcolor + ';">
		<td>' + @catName + '</td>
		<td align="center" >' + cast(@prodcount as varchar(100)) + '</td>'
		IF (@w1) < @wa
		   BEGIN
			SET @fontcolor = '#FF0000'
		   END
		ELSE
		   BEGIN
			SET @fontcolor = '#000000'
		   END
		
		SET @body1 = @body1 + '
		<td align="center" ><font color="' + @fontcolor + '">$' + CONVERT(varchar(50), ISNULL(@w1, 0), 1) + '</font></td>
		<td align="center" ><font color="' + @fontcolor + '">' + CONVERT(varchar(50), ISNULL(@o1, 0), 1) + '</font></td>'
		IF (@w2) < @wa
		   BEGIN
			SET @fontcolor = '#FF0000'
		   END
		ELSE
		   BEGIN
			SET @fontcolor = '#000000'
		   END
		
		SET @body1 = @body1 + '
		<td align="center" ><font color="' + @fontcolor + '">$' + CONVERT(varchar(50), ISNULL(@w2, 0), 1) + '</font></td>
		<td align="center" ><font color="' + @fontcolor + '">' + CONVERT(varchar(50), ISNULL(@o2, 0), 1) + '</font></td>'
		IF (@w3) < @wa
		   BEGIN
			SET @fontcolor = '#FF0000'
		   END
		ELSE
		   BEGIN
			SET @fontcolor = '#000000'
		   END
		
		SET @body1 = @body1 + '
		<td align="center" ><font color="' + @fontcolor + '">$' + CONVERT(varchar(50), ISNULL(@w3, 0), 1) + '</font></td>
		<td align="center" ><font color="' + @fontcolor + '">' + CONVERT(varchar(50), ISNULL(@o3, 0), 1) + '</font></td>'
		IF (@w4) < @wa
		   BEGIN
			SET @fontcolor = '#FF0000'
		   END
		ELSE
		   BEGIN
			SET @fontcolor = '#000000'
		   END
		
		SET @body1 = @body1 + '
		<td align="center" ><font color="' + @fontcolor + '">$' + CONVERT(varchar(50), ISNULL(@w4, 0), 1) + '</font></td>
		<td align="center" ><font color="' + @fontcolor + '">' + CONVERT(varchar(50), ISNULL(@o4, 0), 1) + '</font></td>'
		IF (@w5) < @wa
		   BEGIN
			SET @fontcolor = '#FF0000'
		   END
		ELSE
		   BEGIN
			SET @fontcolor = '#000000'
		   END
		
		SET @body1 = @body1 + '
		<td align="center" ><font color="' + @fontcolor + '">$' + CONVERT(varchar(50), ISNULL(@w5, 0), 1) + '</font></td>
		<td align="center" ><font color="' + @fontcolor + '">' + CONVERT(varchar(50), ISNULL(@o5, 0), 1) + '</font></td>'
		IF (@w6) < @wa
		   BEGIN
			SET @fontcolor = '#FF0000'
		   END
		ELSE
		   BEGIN
			SET @fontcolor = '#000000'
		   END
		
		SET @body1 = @body1 + '
		<td align="center" ><font color="' + @fontcolor + '">$' + CONVERT(varchar(50), ISNULL(@w6, 0), 1) + '</font></td>
		<td align="center" ><font color="' + @fontcolor + '">' + CONVERT(varchar(50), ISNULL(@o6, 0), 1) + '</font></td>
		<td align="center" >$' + CONVERT(varchar(50), ISNULL(@wa, 0), 1) + '</td>
		<td align="center">' + CONVERT(varchar(50), ISNULL(@wao, 0), 1) + '</td>
		'
		IF (@w6+@w5+@w4+@w3) < @mp
		   BEGIN
			SET @fontcolor = '#FF0000'
		   END
		ELSE
		   BEGIN
			SET @fontcolor = '#000000'
		   END
		
		SET @body1 = @body1 + '<td align="center" ><font color="' + @fontcolor + '">$' + CONVERT(varchar(50), ISNULL(@m, 0), 1) + '</font></td>
		<td align="center" ><font color="' + @fontcolor + '">' + CONVERT(varchar(50), ISNULL(@mo, 0), 1) + '</font></td>
		<td align="center" >$' + CONVERT(varchar(50), ISNULL(@mp, 0), 1) + '</td>
	  </tr>'
				
		FETCH NEXT FROM sCursor INTO @catName, @prodcount, @w1, @o1, @w2, @o2, @w3, @o3, @w4, @o4, @w5, @o5, @w6, @o6, @wa, @wao, @m, @mo, @mp, @l, @lo
	   END

	CLOSE sCursor
	DEALLOCATE sCursor
	
	IF (@w6tt+@w5tt+@w4tt+@w3tt) < @mptt
	   BEGIN
		SET @fontcolor = '#FF0000'
	   END
	ELSE
	   BEGIN
		SET @fontcolor = '#000000'
	   END
	
	SET @body1 = @body1 + ' <tr style="background-color:#cccccc; font-weight: bold;">
		<td>Total</td>
		<td align="center" >' + CAST(@prodcountTT AS VARCHAR(100)) + '</td>
		<td align="center" >$' + CONVERT(varchar(50), ISNULL(@w1tt, 0), 1) + '</td>
		<td align="center" >' + CONVERT(varchar(50), ISNULL(@o1tt, 0), 1) + '</td>
		<td align="center" >$' + CONVERT(varchar(50), ISNULL(@w2tt, 0), 1) + '</td>
		<td align="center" >' + CONVERT(varchar(50), ISNULL(@o2tt, 0), 1) + '</td>
		<td align="center" >$' + CONVERT(varchar(50), ISNULL(@w3tt, 0), 1) + '</td>
		<td align="center" >' + CONVERT(varchar(50), ISNULL(@o3tt, 0), 1) + '</td>
		<td align="center" >$' + CONVERT(varchar(50), ISNULL(@w4tt, 0), 1) + '</td>
		<td align="center" >' + CONVERT(varchar(50), ISNULL(@o4tt, 0), 1) + '</td>
		<td align="center" >$' + CONVERT(varchar(50), ISNULL(@w5tt, 0), 1) + '</td>
		<td align="center" >' + CONVERT(varchar(50), ISNULL(@o5tt, 0), 1) + '</td>
		<td align="center" >$' + CONVERT(varchar(50), ISNULL(@w6tt, 0), 1) + '</td>
		<td align="center" >' + CONVERT(varchar(50), ISNULL(@o6tt, 0), 1) + '</td>
		<td align="center" >$' + CONVERT(varchar(50), ISNULL(@watt, 0), 1) + '</td>
		<td align="center" >' + CONVERT(varchar(50), ISNULL(@waott, 0), 1) + '</td>
		<td align="center" ><font color="' + @fontcolor + '">$' + CONVERT(varchar(50), ISNULL(@mtt, 0), 1) + '</font></td>
		<td align="center" ><font color="' + @fontcolor + '">' + CONVERT(varchar(50), ISNULL(@mott, 0), 1) + '</font></td>
		<td align="center" >$' + CONVERT(varchar(50), ISNULL(@mptt, 0), 1) + '</td>
	  </tr>'	

	SET @body1 = @body1 + '</table>'
	
	SET @body1 = @body1 + '	</body>
	</html>'
	
	-- PREPPING THE EMAIL TO SEND
	SET @email1 = 'dlu@katom.com; pchesworth@katom.com; sara@roirevolution.com; matt@roirevolution.com; pbible@katom.comdlu@katom.com; pbible@katom.com; pchesworth@katom.com; cbible@katom.com; jchesworth@katom.com; jrogers@katom.com'
--	SET @email1 = 'david@katom.com'
	SET @subject1 = 'Weekly Sales Performance'

	EXEC msdb.dbo.sp_send_dbmail @profile_name='katom', @recipients=@email1,	@subject=@subject1,	@body=@body1, @body_format ='HTML',	@blind_copy_recipients='katom.archive@gmail.com'	

END
GO
