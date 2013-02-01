USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[webtrackinginfo]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Mike Shell
-- Create date: 3-4-11
-- Description:	Tracking Info Push to Web
-- =============================================
CREATE PROCEDURE [dbo].[webtrackinginfo]
AS
BEGIN
INSERT OPENQUERY(MYSQL, 'select trackingnum, 
								delivered, 
								shipdate, 
								deliverdate, 
								servicetype, 
								signby, 
								company, 
								contact, 
								address1, 
								address2, 
								city, 
								state, 
								zip, 
								country, 
								phone, 
								email, 
								carrier,
								trackingscan, 
								katomorderid, 
								katominvoiceid, 
								lastupdate,
								weborderID,
								selltoname,
								selltoaddress,
								selltoaddress2,
								selltocity,
								selltostate,
								selltozip,
								selltocountry
								FROM katom_mm5.tracking_fedex')

select [Tracking Number]
      ,[Delivered]
      ,[Ship Date]
      ,[Delivered Date]
      ,[Service Type]
      ,[Sign by]
      ,[Company]
      ,[Contact]
      ,[Address 1]
      ,[Address 2]
      ,[City]
      ,[State]
      ,[Zip]
      ,[Country]
      ,[Phone]
      ,[E-Mail]
      ,[carrier]
      ,[Tracking Scan]
      ,[KatomOrderID]
      ,[katomInvoiceID]
      ,[lastUpdateDT]
      ,[weborderid]
      ,[selltoname]
      ,[selltoaddress]
      ,[selltoaddress2]
      ,[selltocity]
      ,[selltostate]
      ,[selltozip]
      ,[selltocountry]
from trackingInfo
where sendToWEB = 1
--or [tracking number] in (select * from openquery(MYSQL,'select trackingnum from katom_mm5.tracking_fedex'))
and lastUpdateDT > (select * from openquery(MYSQL,'select date_add(max(lastupdate), interval 1 second) from katom_mm5.tracking_fedex'))

END
GO
