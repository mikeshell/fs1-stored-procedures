USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[categoryVirtual_SP]    Script Date: 02/01/2013 11:09:50 ******/
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


CREATE PROCEDURE [dbo].[categoryVirtual_SP]
AS
BEGIN
	--REMOVE ALL CATEGORIES WHERE IT IS NO LONGER ACTIVE
	DELETE FROM MIVAVirtualCat WHERE subcat_id NOT IN (SELECT id FROM categories2 WHERE ACTIVE = 1)

	--REMOVE ALL CATEGORIES WHERE PARENT_CAT IS NO LONGER ACTIVE
	DELETE FROM MIVAVirtualCat WHERE parent_id NOT IN (SELECT id FROM categories2 WHERE ACTIVE = 1)

	--COPYING ACTIVE CATEGORIES TO WORKTABLE
	TRUNCATE TABLE categoryVirtualWorktable

	INSERT INTO categoryVirtualWorktable
	SELECT parent_ID, subcat_id, 0 FROM MIVAVirtualCat

	--SETTING REMOVE FLAG EQUAL TO 1 WHERE THE RELATIONSHIP ALREADY EXIST IN THE CATEGORY TABLE
	UPDATE m SET remove = 1 FROM categoryVirtualWorktable m JOIN categories c ON m.subcat_id = c.ID and m.parent_id = c.PARENT_ID

	DELETE categoryVirtualWorktable WHERE remove = 1

	INSERT INTO categories(ID, PARENT_ID, CODE, CATNAME, catDescription, catOrder, metaDescription, url, endleaf, featuredProd1, featuredProd2, ACTIVE, primaryCat)
	SELECT v.subcat_id, v.parent_ID, c.CODE,c.CATNAME, c.catDescription, v.subcat_id, 
			c.metaDescription, c.url, c.endleaf, c.featuredProd1, c.featuredProd2, ACTIVE, 0
	FROM categoryVirtualWorktable v JOIN categories2 c on v.subcat_id = c.ID
END
GO
