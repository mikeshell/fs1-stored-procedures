USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[category_holiday]    Script Date: 02/01/2013 11:09:50 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
-- Modified: 4-14-2011 by Mike S., Removed references to Merchant2 per Beau D.


CREATE PROCEDURE [dbo].[category_holiday]
AS
BEGIN
	DELETE FROM catxprod WHERE catID IN (10754, 10755, 10756)
	
	INSERT INTO catxprod(catID, catCode, prodID, prodCode, primaryCat)		
	SELECT 10754, 'gift-ideas-for-the-ones-you-love', prodid, code, 0 
	FROM products
	WHERE mfgid in('449', '103', '174', '165', '194', '645', '168', '261', '164', '063', '177', '416', '579', 
					'317', '141', '186', '162', '075')
		and active = 1
		and price > 250
		and qtyonhand > 0
	UNION
	SELECT 10755, 'gift-ideas-for-the-ones-you-like', prodid, code, 0
	FROM  products
	WHERE mfgid in('449', '103', '174', '165', '194', '645', '168', '261', '164', '063', '177', '416', '579', 
					'317', '141', '186', '162', '075')
		and active = 1
		and price between 50 and 249.99	
		and qtyonhand > 0
	UNION
	SELECT 10756, 'stocking-ideas', prodid, code, 0
	FROM  products
	WHERE mfgid in('449', '103', '174', '165', '194', '645', '168', '261', '164', '063', '177', '416', '579', 
					'317', '141', '186', '162', '075')
		and active = 1
		and price < 50
		and qtyonhand > 0	
END
GO
