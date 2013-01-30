USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[updateSalesRank]    Script Date: 01/30/2013 16:26:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[updateSalesRank]
AS
BEGIN
--Ran once a week on Saturday

DECLARE @jobID INT
INSERT INTO jobHistory(jobName, startdT) values('PROD_SALESRANK_UPD_FULL', GETDATE())
SET @jobID = @@identity

--UPDATE SALESRANK WITH NUM OF PROD. SOLD WITHIN 90 DAYS
TRUNCATE TABLE prodTemp

INSERT INTO prodTemp([No_], [UnitPrice])
SELECT code, sum(quantity) numSold
FROM order_items
WHERE order_id IN (SELECT customerpo FROM order_hdr WHERE DATEDIFF(DAY, ISNULL(ordertimestamp, '1/1/2009'), GETDATE()) < 91)
GROUP BY code

UPDATE products SET salesRank = NULL

UPDATE p
SET p.salesRank = t.[UnitPrice]
FROM products p JOIN prodTemp t on p.code = t.[No_] COLLATE Latin1_General_CS_AS

TRUNCATE TABLE prodTemp

INSERT INTO prodTemp([No_], [UnitPrice])
SELECT [No_] code, SUM([Quantity]) numsold
FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line]
WHERE [Document No_] in 
			(SELECT [No_]
			FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header]
			WHERE DATEDIFF(DAY, [Order Date], GETDATE()) < 91
				AND [Customer Source] = 'I')
GROUP BY [No_]

UPDATE p
SET p.salesRank = ISNULL(p.salesRank, 0) + t.[UnitPrice]
FROM products p JOIN prodTemp t on p.code = t.[No_] COLLATE Latin1_General_CS_AS

TRUNCATE TABLE prodTemp

UPDATE jobHistory SET endDT = GETDATE() WHERE jobID = @jobID
END
GO
