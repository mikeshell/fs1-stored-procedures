USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[clearanceUpdate]    Script Date: 01/30/2013 16:26:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Beau Dierman>
-- Create date: <1/13/2011>
-- Description:	<Update clearance items>
-- =============================================
CREATE PROCEDURE [dbo].[clearanceUpdate] 
	-- Add the parameters for the stored procedure here
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

INSERT OPENQUERY(MYSQL, 'select code,clearance,salesPrice  from katom_mm5.clearance_copy')
/****** Script for SelectTopNRows command from SSMS  ******/
SELECT [CODE]
      ,[clearance]
      ,[salesPrice]
  FROM [KatomDev].[dbo].[products] WHERE clearance = '1';
END
GO
