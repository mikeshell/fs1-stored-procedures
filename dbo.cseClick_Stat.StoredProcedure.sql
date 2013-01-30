USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[cseClick_Stat]    Script Date: 01/30/2013 16:26:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[cseClick_Stat]
AS
BEGIN
	DECLARE @numClick INT
	
	SET @numClick = 9
	
	DELETE FROM cseclick WHERE DATEDIFF(DAY, insertDT, GETDATE()) > 90
			
	TRUNCATE TABLE prodTemp
	
	INSERT INTO prodTemp([No_], [Product Name], Blocked, UnitPrice, stockItem)
	SELECT sku, dbo.[returnCSEName]([channel_Name]),
		SUM(CAST(clicks AS INT)) clickCount, 
		SUM(CAST(click_cost AS DECIMAL(10, 2))) costTT, 
		SUM(CAST(qty_sold AS INT)) soldQTY
	FROM cseClick_worktable
	GROUP BY sku, dbo.[returnCSEName]([channel_Name])
	HAVING SUM(CAST(clicks AS INT)) > @numClick
	
	DELETE pt FROM prodTemp pt JOIN cseClick c ON pt.[No_] = c.SKU AND pt.[Product Name] = c.Channel_Name
	
	INSERT INTO cseclick (SKU, CHANNEL_NAME, CLICKS, CLICK_COST, QTY_SOLD)
	SELECT [No_], [Product Name], Blocked, UnitPrice, stockItem
	FROM prodTemp
	WHERE [Product Name] NOT IN ('Bing', 'Google Shopping PLA')
	
	UPDATE c
	SET c.profit = 
		CASE 
			WHEN p.price < 250 THEN c.qty_sold*(p.price - p.cost)
			ELSE p.sales30*(p.price - p.cost)
		END, 
		c.price = p.price
	FROM cseclick c JOIN products p ON c.sku = p.CODE
	WHERE DATEDIFF(DAY, insertDT, GETDATE()) = 0
		
	DELETE FROM cseclick WHERE profit >= Click_Cost

END
GO
