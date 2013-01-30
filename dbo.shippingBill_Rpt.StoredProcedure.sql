USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[shippingBill_Rpt]    Script Date: 01/30/2013 16:26:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[shippingBill_Rpt]
	@ups TINYINT, @fb VARCHAR(5), @p SMALLINT, @ot VARCHAR(30)
AS
BEGIN
	DECLARE @sql VARCHAR(MAX)	
	
	IF @p = 0 
	   BEGIN
		SET @p = 31
	   END
	
	SET @sql = 'SELECT upsDT, orderType, 
					CASE orderType
						WHEN ''kt'' THEN ''KaTom Web''
						WHEN ''ph'' THEN ''Phone''
						WHEN ''bb'' THEN ''B&B''
						WHEN ''eb'' THEN ''Ebay''
						WHEN ''fa'' THEN ''Frosty Acres''
						ELSE ''Pilot''
					END orderTypeName,
					COUNT(*) rcCount
				FROM upsBill_RPT
				WHERE DATEDIFF(DAY, [Order Date], GETDATE()) < ' + CAST(@p AS VARCHAR(5)) + '  
						AND orderType in (''' + REPLACE(@ot, ', ', ''', ''') + ''') '
	IF @ups = 1 OR LEN(@fb) > 0
	   BEGIN
		SET @sql = 	@sql + 'AND BilledFromUPS > 0 '
	   END	
	   
	IF @fb = 'a'
	   BEGIN
		SET @sql = 	@sql + 'AND chargedInNAV < estimatedFromWeb '
	   END
	ELSE IF @fb = 'b'
	   BEGIN
		SET @sql = 	@sql + 'AND BilledFromUPS > chargedInNAV '
	   END
	ELSE IF @fb = 'c'
	   BEGIN
		SET @sql = 	@sql + 'AND BilledFromUPS > estimatedFromWeb '
	   END 
	SET @sql = 	@sql + 'GROUP BY upsDT, orderType, 
							CASE orderType
								WHEN ''kt'' THEN ''KaTom Web''
								WHEN ''ph'' THEN ''Phone''
								WHEN ''bb'' THEN ''B&B''
								WHEN ''eb'' THEN ''Ebay''
								WHEN ''fa'' THEN ''Frosty Acres''
								ELSE ''Pilot''
							END
						 ORDER BY 4 DESC; 
						'
	
	
	SET @sql = 	@sql + '
				SELECT * 
				FROM upsBill_RPT
				WHERE DATEDIFF(DAY, [Order Date], GETDATE()) < ' + CAST(@p AS VARCHAR(5)) + ' 
						AND orderType in (''' + REPLACE(@ot, ', ', ''', ''') + ''') '
	
	IF @ups = 1 OR LEN(@fb) > 0
	   BEGIN
		SET @sql = 	@sql + 'AND BilledFromUPS > 0 '
	   END	
	   
	IF @fb = 'a'
	   BEGIN
		SET @sql = 	@sql + 'AND chargedInNAV < estimatedFromWeb '
	   END
	ELSE IF @fb = 'b'
	   BEGIN
		SET @sql = 	@sql + 'AND BilledFromUPS > chargedInNAV '
	   END
	ELSE IF @fb = 'c'
	   BEGIN
		SET @sql = 	@sql + 'AND BilledFromUPS > estimatedFromWeb '
	   END
	
	SET @sql = 	@sql + 'ORDER BY 5 DESC;
					'			
					
	SET @sql = 	@sql + ' 
						SELECT 30 period, orderType, SUM(chargedInNAV) chargedInNAV, SUM(estimatedFromWeb) estimatedFromWeb, SUM(BilledFromUPS) BilledFromUPS
						FROM upsBill_RPT
						WHERE DATEDIFF(day, [Posting Date], GETDATE()) < 31
								AND BilledFromUPS > 0 
						GROUP BY orderType
						UNION
						SELECT 60 period, orderType, SUM(chargedInNAV) chargedInNAV, SUM(estimatedFromWeb) estimatedFromWeb, SUM(BilledFromUPS) BilledFromUPS
						FROM upsBill_RPT
						WHERE DATEDIFF(day, [Posting Date], GETDATE()) BETWEEN 30 AND 60
								AND BilledFromUPS > 0 
						GROUP BY orderType
						UNION
						SELECT 90 period, orderType, SUM(chargedInNAV) chargedInNAV, SUM(estimatedFromWeb) estimatedFromWeb, SUM(BilledFromUPS) BilledFromUPS
						FROM upsBill_RPT
						WHERE DATEDIFF(day, [Posting Date], GETDATE()) BETWEEN 60 AND 90
								AND BilledFromUPS > 0 
						GROUP BY orderType
						ORDER BY orderType, period'
	exec(@sql)
END
GO
