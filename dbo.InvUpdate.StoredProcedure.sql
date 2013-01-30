USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[InvUpdate]    Script Date: 01/30/2013 16:26:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Beau Dierman>
-- Create date: <12/06/10>
-- Description:	<Insert inventory information into ProductInventory table>
-- =============================================
CREATE PROCEDURE [dbo].[InvUpdate]
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    -- Insert statements for procedure here


declare @cmd varchar(4000)
select @cmd = 'osql -Q"select mivaProdID, qtyOnHand from products" -o"c:\SqlJobs\prodinv.txt" -w500 -Uwebsite -PW3b5iTe -SFS1\WEBSQL -dKatomDev'
exec master..xp_cmdshell @cmd
 
 END
GO
