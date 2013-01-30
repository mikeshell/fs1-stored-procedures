USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[webtolocaldatasynch]    Script Date: 01/30/2013 16:26:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[webtolocaldatasynch]
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;
	
	--Drop and Copy paypalresponse, orderError, katomCoupon

	drop table orderError
	
	select * 
	into orderError
	from openquery(KRACK, 'select * from orderError')
	
	drop table paypalresponse
	
	select * 
	into paypalresponse
	from openquery(KRACK, 'select * from paypalresponse')

	drop table katomCoupon
	
	select * 
	into katomCoupon
	from openquery(KRACK, 'select * from katomCoupon')

	drop table trueTracker
	
	select * 
	into trueTracker
	from openquery(KRACK, 'select * from trueTracker')

END
GO
