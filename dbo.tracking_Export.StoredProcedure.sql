USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[tracking_Export]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[tracking_Export]
AS
BEGIN

SELECT katomOrderID, ltrim(rtrim([Tracking Number])) [Tracking Number],
    CASE 
        WHEN CHARINDEX('overnight', [Service Type]) > 0 THEN 'OVERNIGHT'
        WHEN [Service Type] = 'FedEx Home Delivery' THEN 'GROUND'
        WHEN [Service Type] = 'FEDEX EXPRESS SAVER' THEN 'EXPRESS'  
        WHEN [Service Type] = 'ROADRUNNER' THEN 'OUR TRUCK'                                                        
        ELSE LTRIM(UPPER(REPLACE([Service Type], 'FEDEX', '')))
    END [SERVICE TYPE], 
    CASE
		WHEN CHARINDEX('FEDEX', [CARRIER]) > 0 THEN 'FXGROUND'
		WHEN CHARINDEX('UPS', [CARRIER]) > 0 THEN 'UPS'
		WHEN CHARINDEX('VOLLRATH % S.E. ', [CARRIER]) > 0 THEN 'SOUTH'
		ELSE [CARRIER]
	END [CARRIER]    
FROM trackinginfo
WHERE NAVimported = 0 AND LEN(ISNULL(katomOrderID, '')) > 0
END
GO
