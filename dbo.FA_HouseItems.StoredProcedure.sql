USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[FA_HouseItems]    Script Date: 01/30/2013 16:26:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[FA_HouseItems]
AS
BEGIN
	SELECT T.CODE, 
		ISNULL([1], 0) [MO1],
		ISNULL([2], 0) [MO2],
		ISNULL([3], 0) [MO3],
		ISNULL([4], 0) [MO4],
		ISNULL([5], 0) [MO5],
		ISNULL([6], 0) [MO6],
		ISNULL([7], 0) [MO7],
		ISNULL([8], 0) [MO8],
		ISNULL([9], 0) [MO9],
		ISNULL([10], 0) [MO10],
		ISNULL([11], 0) [MO11],
		ISNULL([12], 0) [MO12],
		ISNULL([12], 0) [MO13],
		ISNULL([13], 0) cMonth,
		(ISNULL([10], 0) + ISNULL([11], 0) + ISNULL([12], 0))/3 [3MOAVG], 
		(ISNULL([7], 0) + ISNULL([8], 0) + ISNULL([9], 0) + ISNULL([10], 0) + ISNULL([11], 0) + ISNULL([12], 0))/6 [6MOAVG],
		p.qtyOnHand 
	FROM FA_HouseOrderItems t join products p on t.code = p.CODE
	ORDER BY t.code	
END
GO
