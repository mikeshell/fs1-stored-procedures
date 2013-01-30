USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[PowerReviewsFollowup_SP_OldNav]    Script Date: 01/30/2013 16:26:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[PowerReviewsFollowup_SP_OldNav] 

AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

DELETE FROM powerreviewsfollowup

INSERT INTO powerreviewsfollowup
SELECT [Document No_], i.[No_], replace(h.name, ',', ''), replace(h.name, ',', ''), h.[e-mail]
FROM fs1.[KatomDev].[dbo].[B&B Equipment & Supply Inc_$Sales Invoice Line] i
JOIN fs1.[KatomDev].[dbo].[B&B Equipment & Supply Inc_$Customer] h
ON h.[No_] = i.[Sell-to Customer No_]
WHERE i.No_ NOT LIKE '100%' AND i.No_ NOT LIKE '420' and i.No_ like '%-%'
AND CONVERT(nvarchar(50), i.[shipment date],3) = CONVERT(nvarchar(50), dateadd(week, -3, getdate()), 3)
AND [e-mail] LIKE '%@%'
AND [e-mail] <> 'CHARLES.GRIFFIN@PILOTTRAVELCENTERS.COM'
AND i.[Project Code] = 'KATOM'
ORDER BY i.No_

Select * FROM powerreviewsfollowup

END
GO
