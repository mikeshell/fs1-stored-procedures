USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[categories_SuperCat]    Script Date: 02/01/2013 11:09:50 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[categories_SuperCat]
AS
BEGIN
	UPDATE categories SET superCat = NULL, superCatID = NULL

	UPDATE categories SET superCat = CATNAME, superCatID = ID WHERE PARENT_ID = 0

	WHILE EXISTS(SELECT * FROM categories WHERE superCat IS NULL and ACTIVE = 1)
	   BEGIN
			UPDATE c
			SET c.superCat = c2.superCat,
				c.superCatID = c2.superCatID
			FROM categories c JOIN categories c2 ON c.PARENT_ID = c2.ID AND c.primaryCat = c2.primaryCat
			WHERE c.superCat IS NULL AND c2.superCat IS NOT NULL
			
			UPDATE c
			SET c.superCat = c2.superCat,
				c.superCatID = c2.superCatID
			FROM categories c JOIN categories c2 ON c.PARENT_ID = c2.ID
			WHERE c.superCat IS NULL AND c2.superCat IS NOT NULL
	   END
	   
END
GO
