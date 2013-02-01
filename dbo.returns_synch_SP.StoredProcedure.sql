USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[returns_synch_SP]    Script Date: 02/01/2013 11:32:51 ******/
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
	--Selects all new header information from remote and inserts to local
	insert into web_return_requests
	select * from openquery(KRACK, 'select * from ci_return_request r')
	where id not in (select return_id from web_return_requests)
	
	--Selects all new item information from remote and inserts to local
	insert into web_return_requests_items
	select * from openquery(KRACK, 'select * from ci_return_request_items')
	where return_id not in (select return_id from web_return_requests_items)

END
GO
