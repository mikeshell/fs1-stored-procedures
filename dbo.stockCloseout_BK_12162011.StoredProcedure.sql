USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[stockCloseout_BK_12162011]    Script Date: 01/30/2013 16:26:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[stockCloseout_BK_12162011]
AS
BEGIN
	--IDENTIFYING ALL STOCKED PRODUCTS NOT SOLD IN THE LAST 18 MONTHS
	insert into closeout
	select [Item No_], SUM([Quantity]) qtyOnHand, MAX([Posting Date]) lastSoldDT,
			CASE 
				WHEN cost < 10 THEN CAST(cost/.9 as decimal(10, 2))
				WHEN cost BETWEEN 10 AND 100 THEN CAST(cost/.94 as decimal(10, 2))
				ELSE CAST(cost/.3 as decimal(10, 2))
			END sellingPrice
	from fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item Ledger Entry] i
			JOIN products p ON p.CODE = i.[Item No_] collate Latin1_General_CS_AS
	WHERE CODE NOT LIKE '%-cat%'
	group by [Item No_], cost
	Having SUM([Quantity]) > 0 and DATEDIFF(MONTH, MAX([Posting Date]), GETDATE()) > 17

	--UPDATE PRODUCTS TABLE AND MARK THEM TO CLEARANCE
	update p
	set p.clearance = 1,
		p.qtyOnHand = c.qtyOnHand,
		p.lastSold = c.lastSoldDT,
		p.salesPrice = 
			CASE
				WHEN p.price > c.sellingPrice THEN c.sellingPrice
				ELSE p.price
			END
	from products p join closeout c on p.CODE = c.code COLLATE Latin1_General_CS_AS
	where ISNULL(p.clearance, 0) = 0

	--UPDATE PRODUCTS TABLE WITH CORRECT INVENTORY COUNT
	update p
	set p.qtyOnHand = c.qtyOnHand
	from products p join closeout c on p.CODE = c.code COLLATE Latin1_General_CS_AS
	where p.qtyOnHand <> c.qtyOnHand AND ISNULL(p.clearance, 0) = 1

	--REMOVE FROM CLEARANCE IF INVENTORY IS DEPLETED
	update products 
	set clearance = 0, lastSold = NULL, salesPrice = NULL
	WHERE clearance = 1 AND qtyOnHand = 0

	--EMAIL PATRICIA A LIST OF CLEARANCE ITEMS WITH MAX AND MIN
	exec dbo.email_StockCloseout
END
GO
