USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[sp_Mail]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
Create Procedure [dbo].[sp_Mail]
	@SenderName varchar(100),
	@SenderAddress varchar(100),
	@RecipientName varchar(100),
	@RecipientAddress varchar(100),
	@Subject varchar(200),
	@Body varchar(8000),
	@MailServer varchar(100) = 'mail.bnbequip.com'

	AS	
	
	SET nocount on
GO
