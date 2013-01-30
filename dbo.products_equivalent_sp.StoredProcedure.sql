USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[products_equivalent_sp]    Script Date: 01/30/2013 16:26:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[products_equivalent_sp]
AS
BEGIN
	truncate table products_equivalent
	insert into products_equivalent(code, equivGroup)
	SELECT CODE, EquivGrp
	FROM products
	WHERE active = 0 and len(isnull(EquivGrp, '')) > 0 and price > 500

	truncate table prodTemp
	insert into prodTemp([Equivalent Grp_], Blocked)
	select distinct equivGroup, (select COUNT(*) from products where active = 1 and EquivGrp = e.equivGroup)
	from products_equivalent e

	--REMOVING ALL EQUIVALENT PRODUCTS WHERE THERE IS NO EQUIVALENT
	delete from products_equivalent
	where equivGroup in (select [Equivalent Grp_] from prodTemp where Blocked = 0)


	--SET THE EQUIVALENT TO PRODUCT WITH ONLY ONE EQUIVALENT
	UPDATE e
	SET e.eCode = p.CODE
	FROM prodTemp pt join products p on pt.[Equivalent Grp_] = p.EquivGrp 
					 join products_equivalent e on pt.[Equivalent Grp_] = e.equivGroup
	WHERE pt.Blocked = 1 and p.ACTIVE = 1


	--SET EQUIVALENT TO THE REST OF THE PRODUCTS
	DECLARE @equivGroup VARCHAR(10), @code VARCHAR(100)

	DECLARE eCursor CURSOR FOR 
	SELECT DISTINCT equivGroup FROM products_equivalent WHERE ecode IS NULL

	OPEN eCursor
	FETCH NEXT FROM eCursor INTO @equivGroup

	WHILE @@FETCH_STATUS = 0
	   BEGIN
		SELECT TOP 1 @code = code FROM products WHERE EquivGrp = @equivGroup AND ACTIVE = 1 ORDER BY sales_last_24MO
		
		UPDATE products_equivalent SET eCode = @code WHERE equivGroup = @equivGroup
		
		FETCH NEXT FROM eCursor INTO @equivGroup
	   END

	CLOSE eCursor
	DEALLOCATE eCursor

END
GO
