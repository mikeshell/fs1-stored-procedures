USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[CvcImporttest]    Script Date: 02/01/2013 11:09:50 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
Create PROCEDURE [dbo].[CvcImporttest]

AS
BEGIN
/*
load data local infile 'H:/file.txt'
into table test.info
FIELDS TERMINATED BY '~~~'
lines terminated by '~!~!'
ignore 1 lines
;*/
/*
delete from filetest

BULK
INSERT filetest
from 'C:\sqljobs\file.txt'
WITH
(
FIELDTERMINATOR = '~/~/',
ROWTERMINATOR = '~!~!'
)

select * from filetest
/*
INSERT OPENQUERY(MYSQLLOCAL, 'select refnum, secnum from test.info')
select refnum, SecNum 
from filetest
--where refnum is null
*/
*/

insert
into filetestx (refnum, secnum)
select * from openquery(MySQL, 'select refnum, secnum from ordersec')
where secnum not in (select secnum from filetestx)

update order_hdr
set cvc = refnum
from filetestx join order_hdr
on secnum = customerpo
where cvc = '1'


END
GO
