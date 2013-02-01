USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[feedHistory_SP]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[feedHistory_SP]
 @cse VARCHAR(100) -- MIN  VALUE WE WANT TO INCLUDE IN THE FEED
AS
BEGIN
	DECLARE @prodCount INT, @price MONEY
	
	IF @cse = 'Amazon'
	   BEGIN
	    SET @price = 49.99
	    
		SELECT @prodCount = COUNT(CODE)
		FROM products p LEFT JOIN cseCatMapping c ON p.primaryCatCode = c.catCode
		WHERE p.active = 1 AND p.isWeb = 1 AND p.price > @price AND p.image NOT LIKE '%logo'
			AND p.CODE NOT IN (select [No_] COLLATE Latin1_General_CS_AS from backOrder_item where DATEDIFF(day, [OldestOrderdt], getdate()) > 15 and price < 250)   
	   END
	   
	INSERT INTO feedHistory(cseName, numProd, feedDT)  VALUES(@cse, @prodCount, GETDATE())
END
GO
