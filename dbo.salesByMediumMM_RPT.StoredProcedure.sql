USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[salesByMediumMM_RPT]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		David Lu
-- Create date: 03/28/2011
-- Description:	Pulling Sales Data by category or Mfg
-- =============================================
CREATE PROCEDURE [dbo].[salesByMediumMM_RPT]
AS
BEGIN	
	-- SALES BY MEDIUM	
	SELECT orderMedium, 
		ISNULL([M1os], 0) [M1os], ISNULL([M1oo], 0) [M1oo], ISNULL([M1is], 0) [M1is], ISNULL([M1io], 0) [M1io],
		ISNULL([M2os], 0) [M2os], ISNULL([M2oo], 0) [M2oo], ISNULL([M2is], 0) [M2is], ISNULL([M2io], 0) [M2io],
		ISNULL([M3os], 0) [M3os], ISNULL([M3oo], 0) [M3oo], ISNULL([M3is], 0) [M3is], ISNULL([M3io], 0) [M3io],
		ISNULL([M4os], 0) [M4os], ISNULL([M4oo], 0) [M4oo], ISNULL([M4is], 0) [M4is], ISNULL([M4io], 0) [M4io],
		ISNULL([M5os], 0) [M5os], ISNULL([M5oo], 0) [M5oo], ISNULL([M5is], 0) [M5is], ISNULL([M5io], 0) [M5io],
		ISNULL([M6os], 0) [M6os], ISNULL([M6oo], 0) [M6oo],	ISNULL([M6is], 0) [M6is], ISNULL([M6io], 0) [M6io],
		ISNULL([M7os], 0) [M7os], ISNULL([M7oo], 0) [M7oo],	ISNULL([M7is], 0) [M7is], ISNULL([M7io], 0) [M7io],
		ISNULL([M8os], 0) [M8os], ISNULL([M8oo], 0) [M8oo],	ISNULL([M8is], 0) [M8is], ISNULL([M8io], 0) [M8io],
		ISNULL([M9os], 0) [M9os], ISNULL([M9oo], 0) [M9oo],	ISNULL([M9is], 0) [M9is], ISNULL([M9io], 0) [M9io],
		ISNULL([M10os], 0) [M10os], ISNULL([M10oo], 0) [M10oo],	ISNULL([M10is], 0) [M10is], ISNULL([M10io], 0) [M10io],
		ISNULL([M11os], 0) [M11os], ISNULL([M11oo], 0) [M11oo],	ISNULL([M11is], 0) [M11is], ISNULL([M11io], 0) [M11io],
		ISNULL([M12os], 0) [M12os], ISNULL([M12oo], 0) [M12oo],	ISNULL([M12is], 0) [M12is], ISNULL([M12io], 0) [M12io],
		ISNULL([M1os], 0) + ISNULL([M2os], 0) + ISNULL([M3os], 0) + ISNULL([M4os], 0) + ISNULL([M5os], 0) + ISNULL([M6os], 0)
		+ ISNULL([M7os], 0) + ISNULL([M8os], 0) + ISNULL([M9os], 0) + ISNULL([M10os], 0) + ISNULL([M11os], 0) + ISNULL([M12os], 0) [osalesTT],
		ISNULL([M1is], 0) + ISNULL([M2is], 0) + ISNULL([M3is], 0) + ISNULL([M4is], 0) + ISNULL([M5is], 0) + ISNULL([M6is], 0) 
		+ ISNULL([M7is], 0) + ISNULL([M8is], 0) + ISNULL([M9is], 0) + ISNULL([M10is], 0) + ISNULL([M11is], 0) + ISNULL([M12is], 0) [isalesTT],
		ISNULL([M1oo], 0) + ISNULL([M2oo], 0) + ISNULL([M3oo], 0) + ISNULL([M4oo], 0) + ISNULL([M5oo], 0) + ISNULL([M6oo], 0) 
		+ ISNULL([M7oo], 0) + ISNULL([M8oo], 0) + ISNULL([M9oo], 0) + ISNULL([M10oo], 0) + ISNULL([M11oo], 0) + ISNULL([M12oo], 0) [oorderTT],
		ISNULL([M1io], 0) + ISNULL([M2io], 0) + ISNULL([M3io], 0) + ISNULL([M4io], 0) + ISNULL([M5io], 0) + ISNULL([M6io], 0) 
		+ ISNULL([M7io], 0) + ISNULL([M8io], 0) + ISNULL([M9io], 0) + ISNULL([M10io], 0) + ISNULL([M11io], 0) + ISNULL([M12io], 0) [iorderTT]
	FROM salesRptByMedium_MM
	
END
GO
