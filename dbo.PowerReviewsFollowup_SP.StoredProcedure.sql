USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[PowerReviewsFollowup_SP]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[PowerReviewsFollowup_SP] 

AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

DELETE FROM powerreviewsfollowup

INSERT INTO powerreviewsfollowup
SELECT [Document No_], i.[No_], replace(h.[Ship-to Name], ',', ''), replace(h.[Ship-to Name], ',', ''), h.[e-mail]
FROM fs1.[Katom2009].[dbo].[B&B Equipment & Supply Inc_$Sales Invoice Line] i
JOIN fs1.[Katom2009].[dbo].[B&B Equipment & Supply Inc_$Sales Invoice Header] h
ON h.[No_] = i.[Document No_]
join trackingInfo
on h.[Package Tracking No_] collate Latin1_General_CS_AS = [Tracking Number] 
WHERE i.No_ NOT LIKE '100%' AND i.No_ NOT LIKE '420' and i.No_ like '%-%'
AND h.[e-mail] LIKE '%@%'
AND h.[e-mail] <> 'CHARLES.GRIFFIN@PILOTTRAVELCENTERS.COM'
AND i.[Shortcut Dimension 2 Code] = 'KATOM'
AND (CONVERT(nvarchar(50), i.[Posting Date],101) = CONVERT(nvarchar(50), dateadd(week, -3, getdate()), 101)
)
and h.[Web Order No_] like 'KT%'
--and Delivered = 'True'
ORDER BY i.No_

END
GO
