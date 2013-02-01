USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[MFG_SP]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[MFG_SP]
AS
BEGIN
	DECLARE @jobID INT
	INSERT INTO jobHistory(jobName, startdT) values('MFG_UPD', GETDATE())
	SET @jobID = @@identity

	--TODAY LOGIC IS LIMITTING THE MFGID TO 3 DIGITS AND IT IS RECYCLABLE. 
	INSERT INTO  mfg(mfgID, navName, mfgName, mfgShortName, shipDirect, ebay, isWeb, Active, addedDT)
	SELECT [No_], Name, [Name 2], [Name 2], [Drop Ship Only], 0, 0, 0, GETDATE()
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Vendor] 
	WHERE LEN([No_]) = 3 
		AND [No_] NOT IN (SELECT mfgid COLLATE SQL_Latin1_General_CP1_CI_AS FROM mfg)

	--COUNTING THE NUMBER OF ACTIVE PRODUCTS
	TRUNCATE TABLE prodTemp

	INSERT INTO prodTemp([Mfg ID], Blocked)
	SELECT mfgid, COUNT(*) FROM products WHERE active = 1 GROUP BY mfgid

	UPDATE m
	SET m.activeProdCount = p.Blocked
	FROM mfg m join prodTemp p on m.mfgID = p.[Mfg ID]

	UPDATE mfg
	SET isWeb = 1, Active = 1
	WHERE DATEDIFF(DAY, GETDATE(), addedDT) = 0 AND activeProdCount > 0

	--REMOVE MFG INFO WHERE IT DOES NOT EXIST IN NAV
	DELETE FROM mfg WHERE mfgID NOT IN (SELECT [No_] COLLATE Latin1_General_CS_AS FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Vendor] )

	--UPDATING MFG INFO
	UPDATE m
	SET m.NAVName = LTRIM(RTRIM(v.Name))
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Vendor] v JOIN mfg m on v.[No_] = m.mfgID COLLATE SQL_Latin1_General_CP1_CI_AS
	WHERE m.NAVName <> LTRIM(RTRIM(v.Name)) COLLATE SQL_Latin1_General_CP1_CI_AS

	UPDATE m
	SET m.mfgName = LTRIM(RTRIM(v.[Name 2]))
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Vendor] v JOIN mfg m on v.[No_] = m.mfgID COLLATE SQL_Latin1_General_CP1_CI_AS
	WHERE m.mfgName <> LTRIM(RTRIM(v.[Name 2])) COLLATE SQL_Latin1_General_CP1_CI_AS

	UPDATE m
	SET m.shipDirect = LTRIM(RTRIM(v.[Drop Ship Only])),
		m.dropShipCapable = LTRIM(RTRIM(v.[Drop Ship Capable])),
		m.dropShipMin = LTRIM(RTRIM([Min_ Amount to Drop Ship]))
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Vendor] v JOIN mfg m on v.[No_] = m.mfgID COLLATE SQL_Latin1_General_CP1_CI_AS

	TRUNCATE TABLE prodTemp
		
	UPDATE jobHistory SET endDT = GETDATE() WHERE jobID = @jobID

END
GO
