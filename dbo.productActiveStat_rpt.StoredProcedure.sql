USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[productActiveStat_rpt]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[productActiveStat_rpt]
AS
BEGIN
	EXEC productActiveStat;
	
	SELECT ISNULL(numProdInNAV, 0) numProdInNAV, 
		ISNULL(discontinuedNAV, 0) discontinuedNAV,  
		ISNULL(webItemNAV, 0) webItemNAV,  
		ISNULL(notCatNAV, 0) notCatNAV,  
		ISNULL(liveOnWeb, 0) liveOnWeb,  
		ISNULL(discontinuedButActive, 0) discontinuedButActive,  
		ISNULL(notCatOnWEB, 0) notCatOnWEB, 
		ISNULL(webItemNAV+discontinuedButActive, 0) [webanddisc], 
		ISNULL(numProdInNAV-discontinuedNAV, 0) [activeProdNAV]
	FROM productStat  
	ORDER BY mfgid
END
GO
