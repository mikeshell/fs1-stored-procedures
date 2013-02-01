USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[openOrderRPT_SP]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[openOrderRPT_SP]
	@pageNum INT, @pageSize INT, @filterBy VARCHAR(50), @toRemove VARCHAR(100), @orderType varchar(10)
AS
BEGIN
	DECLARE @startRowIndex INT, @recTT INT, @query VARCHAR(MAX), @queryToAdd VARCHAR(MAX)
	
	SELECT salesperson, COUNT(*) orderTT
	FROM openOrder
	GROUP BY salesperson
		
	IF @pageNum = 1
		SET @startRowIndex = @pageNum
	ELSE
		SET @startRowIndex = (@pageSize*(@pageNum-1)) + 1
	
	IF @filterBy = 'all'
	   BEGIN
		SET @queryToAdd = ''
		
		IF @toRemove = 'backorder'
		   BEGIN
			SELECT @recTT = COUNT(*) FROM openOrder WHERE (LOWER(orderStatus) <> 'backorder' OR CHARINDEX('b/o', LOWER(orderStatus)) > 0)			
			SET @queryToAdd = ' WHERE (LOWER(orderStatus) <> ''backorder'' OR CHARINDEX(''b/o'', LOWER(orderStatus)) > 0) '
		   END 		  
		   
		IF @toRemove = 'backorder, ccd'
		   BEGIN
			SELECT @recTT = COUNT(*) 
			FROM openOrder 
			WHERE (LOWER(orderStatus) <> 'backorder' OR CHARINDEX('b/o', LOWER(orderStatus)) > 0) 
					AND (paymentType = 'CC' AND CCAprObtained = 1)
			SET @queryToAdd = ' WHERE (LOWER(orderStatus) <> ''backorder'' OR CHARINDEX(''b/o'', LOWER(orderStatus)) > 0) 
									AND (paymentType = ''CC'' AND CCAprObtained = 1) '
		   END
		   
		IF @toRemove = 'ccd'
		   BEGIN
			SELECT @recTT = COUNT(*) FROM openOrder WHERE (paymentType = 'CC' AND CCAprObtained = 1) 
			SET @queryToAdd = ' WHERE (paymentType = ''CC'' AND CCAprObtained = 1) '
		   END
		   
		IF LEN(@toRemove) = 0
			SELECT @recTT = COUNT(*) FROM openOrder
			
		SET @query = 'SELECT *, ' + CAST(@recTT AS VARCHAR(10)) + ' recTT 
					  FROM (select o.*, ROW_NUMBER() OVER (order by ticketPrinted asc, salesTT desc) AS RowNum 
							from openOrder o' + @queryToAdd + ') as openOrder
					  WHERE RowNum BETWEEN ' + CAST(@startRowIndex AS VARCHAR(10)) + ' AND ' + CAST((@pageNum*@pageSize) AS VARCHAR(10))		
	   END 	
	ELSE
	   BEGIN
		SET @query = 'SELECT * FROM openOrder WHERE '
		SET @queryToAdd = 'Salesperson = ''' + @filterBy + ''' '
		
		IF @filterBy = 'shipDirect'
		   SET @queryToAdd = 'shipDirect = 1 '
		
		IF @filterBy = 'unprinted'
		   SET @queryToAdd = 'ticketPrinted = 0 '
		   		     
		IF @filterBy = 'ready2ship'
		   SET @queryToAdd = 'ReadyToShip = 1 '
		   
		IF @filterBy = 'intl'
		   SET @queryToAdd = 'orderStatus = ''International'' OR INTL = 1'
		   
		IF @filterBy = 'ccd'
		   SET @queryToAdd = '(paymentType = ''CC'' AND CCAprObtained = 0 AND CHARINDEX(''-'', orderNum) = 0) '
		   
		IF CHARINDEX('backorder', @toRemove) > 0
		   SET @queryToAdd = @queryToAdd + 'AND (LOWER(orderStatus) <> ''backorder'' OR CHARINDEX(''b/o'', LOWER(orderStatus)) > 0) '
		   
		IF CHARINDEX('ccd', @toRemove) > 0
		   SET @queryToAdd = @queryToAdd + 'AND (paymentType = ''CC'' AND CCAprObtained = 1) '
		   
		IF CHARINDEX('julianne', @toRemove) > 0
		   SET @queryToAdd = @queryToAdd + 'AND (paymentType = ''CC'' AND CCAprObtained = 1) ' +
					'AND orderStatus IN (''CANCEL B/O TOO LONG'', ''Cancelled'', ''IN PROGRESS'', ' +
					'''Items on Backorder'', ''Must Go Today'', ''Ready to Ship'', ''Ship Direct'', ' +
					'''Ship What We Have'', ''Waiting for Info.'', ''Waiting on Customer'', ' +
					'''Waiting on Freight'', ''Waiting on Taxes'')'
		   
		SET @query = @query + @queryToAdd
	   END 	
	   
	IF @orderType = 'dt'
		SET @query = @query + 'ORDER BY orderDT ASC'
	ELSE
		SET @query = @query + 'ORDER BY salesTT DESC'
				
	EXEC(@query)
END
GO
