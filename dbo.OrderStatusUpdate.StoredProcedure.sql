USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[OrderStatusUpdate]    Script Date: 01/30/2013 16:26:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[OrderStatusUpdate]
	-- Add the parameters for the stored procedure here
AS
BEGIN
	
	delete from order_status

	insert into order_status ([Web Order No_], status)
	select
	[Web Order No_],
	CASE
	WHEN [Source Code] = 'DELETE' THEN 'X'
	WHEN [Delivered] = 'True' THEN 'D'
	WHEN [Delivered] = 'False' THEN 'S'
	WHEN [Tracking Number] IS NULL THEN 'W'
	WHEN [Delivered] IS NULL THEN 'S'
	END as status
	from invoice_backup
	left outer join trackingInfo
	on [Order No_] COLLATE Latin1_General_CS_AS = KatomOrderID
	where [Web Order No_] like 'KT%'
	
	insert into order_status ([Web Order No_], status)
	select [Web Order No_], 'P' from fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Header]
	where [Web Order No_] like 'KT%'
	and [Web Order No_] not in (select [Web Order No_] from invoice_backup)


END
GO
