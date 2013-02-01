USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[SuperSaverUpdate_SP]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[SuperSaverUpdate_SP] 

AS
BEGIN
	
	delete from openquery(MySQL, 'select * from TrueSuperSaver')

	insert openquery(MySQL, 'select MPN, listprice, sellingprice, code, mapprice from TrueSuperSaver')
	select MPN, listprice, sellingprice, code, displayprice from trueSuperTrailer
	
	
		BEGIN

				DECLARE @code varchar(30)
				DECLARE @price varchar(MAX)


				DECLARE trueCursor CURSOR FOR 

						SELECT code, [sellingprice]
						FROM [KatomDev].[dbo].[trueSuperTrailer]
						
				OPEN trueCursor

				FETCH NEXT FROM trueCursor INTO @code, @price
					

					WHILE @@FETCH_STATUS = 0

					BEGIN
					
						update openquery (mysql, 'select price, code from s01_Products')
						set price = @price
						where code = @code

					FETCH NEXT FROM trueCursor INTO @code, @price
					
					END

					CLOSE trueCursor

					DEALLOCATE trueCursor

		END

		---

END
GO
