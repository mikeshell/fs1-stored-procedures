USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[OrderTotalUpdate]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[OrderTotalUpdate]

AS

BEGIN

DECLARE @jobID INT
INSERT INTO jobHistory(jobName, startdT) values('MIVA_TBL', GETDATE())
SET @jobID = @@identity

DECLARE @day INT

SET @day = -1

/* Update SQL Table based on Miva Sales Information */
insert into order_totalarchive
select
order_id, total, orderdate
from
 openquery(MYSQL, '
		SELECT order_id, sum(price*quantity) as total, from_unixtime(orderdate) as orderdate
		FROM katom_mm5.s01_Orders o
		join katom_mm5.s01_OrderItems i
		on o.id = i.order_id
		group by order_id	
		')
where order_id not in (select order_id from order_totalarchive)

/* Update Miva Totals */
INSERT OPENQUERY(MYSQL, 'select orderdate, range1, range2, range3, range4, range5, range6, range7, range8, range9  from katom_mm5.ordertotalarchive')
SELECT distinct(CONVERT(VARCHAR(10), orderdate, 110)), 
(select COUNT(*)
from order_totalarchive
where CONVERT(VARCHAR(10), orderdate, 110) = CONVERT(VARCHAR(10), dateadd(dd, @day, GETDATE()), 110)
and total < 250) as range1,
(select COUNT(*)
from order_totalarchive
where CONVERT(VARCHAR(10), orderdate, 110) = CONVERT(VARCHAR(10), dateadd(dd, @day, GETDATE()), 110)
and total < 501 and total > 250) as range2,
(select COUNT(*)
from order_totalarchive
where CONVERT(VARCHAR(10), orderdate, 110) = CONVERT(VARCHAR(10), dateadd(dd, @day, GETDATE()), 110)
and total < 1001 and total > 500) as range3,
(select COUNT(*)
from order_totalarchive
where CONVERT(VARCHAR(10), orderdate, 110) = CONVERT(VARCHAR(10), dateadd(dd, @day, GETDATE()), 110)
and total < 2001 and total > 1000) as range4,
(select COUNT(*)
from order_totalarchive
where CONVERT(VARCHAR(10), orderdate, 110) = CONVERT(VARCHAR(10), dateadd(dd, -@day, GETDATE()), 110)
and total < 3001 and total > 2000) as range5,
(select COUNT(*)
from order_totalarchive
where CONVERT(VARCHAR(10), orderdate, 110) = CONVERT(VARCHAR(10), dateadd(dd, @day, GETDATE()), 110)
and total < 5001 and total > 3000) as range6,
(select COUNT(*)
from order_totalarchive
where CONVERT(VARCHAR(10), orderdate, 110) = CONVERT(VARCHAR(10), dateadd(dd, @day, GETDATE()), 110)
and total > 5000) as range7,
(select COUNT(*)
from order_totalarchive
where CONVERT(VARCHAR(10), orderdate, 110) = CONVERT(VARCHAR(10), dateadd(dd, @day, GETDATE()), 110)
and total < 10000 and total > 7000) as range8,
(select COUNT(*)
from order_totalarchive
where CONVERT(VARCHAR(10), orderdate, 110) = CONVERT(VARCHAR(10), dateadd(dd, @day, GETDATE()), 110)
and total >= 10000) as range9
FROM order_totalarchive
where CONVERT(VARCHAR(10), orderdate, 110) = CONVERT(VARCHAR(10), dateadd(dd, @day, GETDATE()), 110)

UPDATE jobHistory SET endDT = GETDATE() WHERE jobID = @jobID

END
GO
