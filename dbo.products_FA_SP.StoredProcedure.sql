USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[products_FA_SP]    Script Date: 01/30/2013 16:26:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[products_FA_SP]
AS
BEGIN
	EXEC dbo.[categories_FA_SP]
	
	SELECT
		p.prodid, mfgID, mfgName, mpn, CODE, NAME, IMAGE,fa_price,listPrice, 
		ISNULL(metaDescription, '') metaDescription, prodDesc, 
		WEIGHT, prodL, prodW, prodH, CUBE,  freightOnly, UOM, 
		CASE
			WHEN leadTime IS NULL THEN leadTime_mfg
			ELSE leadTime
		END	leadTime, 
		ISNULL(energyStar, 0) energyStar, EquivGrp, shipAlone, 
		ISNULL(cx.catID, '') primaryCatID, ISNULL(cx.catCode, '') primaryCatCode,
		dbo.[getProdCateogrySLI](CODE, 'catCode') categories,
		ISNULL(relatedItems, '') relatedItems, 
		qtyOnHand, sales30, packsize, stackable, ISNULL(clearance,0) clearance,
		CASE
			--Modified 12-31 by MS - changed the null conditional from '' to a default breadcrumb
			WHEN cx.catCode IS NULL THEN '<div class="breadcrumb"><a href="http://www.fabeqsupply.com" title="FAB EQ Supply">FAB EQ Supply</a>&nbsp;>&nbsp;<strong>'+NAME+'</strong></div>'
			ELSE dbo.[getBreadcrumb](cx.catCode, NAME)
		END breadcrumb
	FROM products p LEFT JOIN catxprod_FA cx ON p.CODE = cx.prodCode AND cx.primaryCat = 1
	WHERE ACTIVE = 1 AND isWeb = 1 AND fa = 1
	
END
GO
