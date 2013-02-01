USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[returns_synch_SP]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[returns_synch_SP]
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	insert into web_return_requests
	select * from openquery(KRACK, 'select * from ci_return_request r')
	where id not in (select return_id from web_return_requests)
	
	insert into web_return_requests_items
	select * from openquery(KRACK, 'select * from ci_return_request_items')
	where return_id not in (select return_id from web_return_requests_items)

END
GO
