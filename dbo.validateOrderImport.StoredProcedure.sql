USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[validateOrderImport]    Script Date: 01/30/2013 16:26:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[validateOrderImport]
AS
BEGIN


	UPDATE order_Hdr
	SET importValidated = 1
	WHERE customerpo COLLATE Latin1_General_CS_AS in 
				(SELECT [Web Order No_]
				FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Header] 
				WHERE charindex('Order ', [Posting Description]) > 0 and len([Web Order No_]) > 0)

	UPDATE order_Hdr
	SET importValidated = 1
	WHERE customerpo COLLATE Latin1_General_CS_AS in 
				(SELECT [Web Order No_]
				FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] 
				WHERE charindex('Order ', [Posting Description]) > 0 and len([Web Order No_]) > 0)
	
	
	---Notification
	BEGIN

		DECLARE @customerpo varchar(30)
		DECLARE @name varchar(MAX)
		DECLARE @age varchar(20)
		DECLARE @body varchar(MAX)

		SET @body = 'The following orders have failed to import in the past 3 hours:</br>'
		
		DECLARE valCursor CURSOR FOR 

			select customerpo, shiptocustomername, DATEDIFF(hh, batchtimestamp, GETDATE()) from order_hdr
			where  DATEDIFF(hh, batchtimestamp, GETDATE()) > 3
			and importValidated = 0
			order by batchtimestamp desc

		OPEN valCursor

		FETCH NEXT FROM valCursor INTO @customerpo, @name, @age
			

			WHILE @@FETCH_STATUS = 0

			BEGIN
			/*
			IF @pilotOrders <> '' 
				BEGIN 
					SET @pilotOrders = @pilotOrders+','+@pilotOrder 
				END
			ELSE 
				BEGIN
					SET @pilotOrders = @pilotOrder
				END
			*/

			SET @body = @body+@customerpo+'</br>'
			
			print @body
				
			FETCH NEXT FROM valCursor INTO @customerpo, @name, @age
			
			END

			CLOSE valCursor

			DEALLOCATE valCursor
						

			IF @customerpo <> '' 
				BEGIN
					EXEC msdb.dbo.sp_send_dbmail @profile_name='katom', @recipients='navision@katom.com', @copy_recipients='paula@katom.com; david@katom.com; mike@katom.com', @subject='Orders Failed to Import', @body=@body, @body_format ='HTML'
				END
		END
		
		--Order History Process
			insert into order_hdr_history ([customerpo],[shiptocontact],[shiptoemail],[shiptocustomername],[phoneno],[faxno],[shiptoaddress],[shiptoaddress2],[shiptocity],[shiptostate],[shiptozip],[shiptocountry],[selltocustomername],[selltoemail],[selltocontact],[selltoaddress],[selltoaddress2],[selltocity],[selltostate],[selltozip],[ccnum],[ccname],[ccexpmo],[ccexpyr],[cvc],[exported],[shipmethod],[shipamount],[ordertimestamp],[batchtimestamp],[importValidated],[avsaddr],[avszip],[cvv2match],[pnref],[respcode],[selltocountry],[sellto1],[selltophone],[orderplacedby],[paymenttermscode],[CustomerPO2],[CustomerSource],[AmountSubmitted])			
			select [customerpo],[shiptocontact],[shiptoemail],[shiptocustomername],[phoneno],[faxno],[shiptoaddress],[shiptoaddress2],[shiptocity],[shiptostate],[shiptozip],[shiptocountry],[selltocustomername],[selltoemail],[selltocontact],[selltoaddress],[selltoaddress2],[selltocity],[selltostate],[selltozip],[ccnum],[ccname],[ccexpmo],[ccexpyr],[cvc],[exported],[shipmethod],[shipamount],[ordertimestamp],[batchtimestamp],[importValidated],[avsaddr],[avszip],[cvv2match],[pnref],[respcode],[selltocountry],[sellto1],[selltophone],[orderplacedby],[paymenttermscode],[CustomerPO2],[CustomerSource],[AmountSubmitted] 
			from order_hdr
			where importValidated = 1
			
			delete from order_hdr
			where importValidated = 1
			
			insert into order_items_history([order_id],[line_id],[product_id],[code],[name],[price],[weight],[taxable],[upsold],[quantity])
			select [order_id],[line_id],[product_id],[code],[name],[price],[weight],[taxable],[upsold],[quantity] 
			from order_items
			where order_id in (select customerpo from order_hdr_history)
			
			delete from order_items
			where order_id in (select order_id from order_items_history)
			
			insert into order_options_history([order_id],[line_id],[attr_id],[attr_code],[option_id],[attmpat_id],[opt_code],[price],[weight],[data],[data_long])
			select [order_id],[line_id],[attr_id],[attr_code],[option_id],[attmpat_id],[opt_code],[price],[weight],[data],[data_long] 
			from order_options
			where order_id in (select customerpo from order_hdr_history)
			
			delete from order_options
			where order_id in (select order_id from order_options_history)

END
GO
