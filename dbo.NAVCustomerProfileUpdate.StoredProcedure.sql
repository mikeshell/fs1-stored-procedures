USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[NAVCustomerProfileUpdate]    Script Date: 01/30/2013 16:26:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[NAVCustomerProfileUpdate]
AS
BEGIN
	-- INSERT CUSTOMER TABLE
	INSERT INTO NAVCustomer
	SELECT 
		[No_], [Name], [Search Name],  [Address], [Address 2], [City], [Contact], 
		[Phone No_], [Project Code], [Cust__Item Disc_ Gr_], [Blocked], [Last Date Modified], 
		[Fax No_], [ZIP Code], [State], [E-Mail], [Tax Area Code], [Ship-to Name], [Ship-to Address], 
		[Ship-to Address 2], [Ship-to City], [Ship-to Contact], [Ship-to ZIP Code], [Ship-to State], 
		[Search Phone], [Bill-to Name], [Bill-to Address], [Bill-to Address 2], [Bill-to City], 
		[Bill-to State], [Bill-to ZIP Code], getdate()
	FROM fs1.z_SANDBOX.dbo.[B&B Equipment & Supply Inc_$Customer] WITH(NOLOCK)
	WHERE DATEDIFF(DAY, [Last Date Modified], GETDATE()) < 31
		  AND [No_] not in (SELECT customerID FROM NVCustomer)

	UPDATE c
	SET c.name = n.[Name], 
		c.searchName = n.[Search Name],
		c.Address = n.[Address],
		c.address2 = n.[Address 2], 
		c.City = n.[City], 
		c.Contact = n.[Contact], 
		c.phoneNum = n.[Phone No_], 
		c.customerOf = n.[Project Code], 
		c.priceStructure = n.[Cust__Item Disc_ Gr_], 
		c.Blocked = n.[Blocked], 
		c.lastModifiedDT = n.[Last Date Modified], 
		c.faxNo = n.[Fax No_], 
		c.zipCode = n.[ZIP Code], 
		c.State = n.[State], 
		c.email = n.[E-Mail],
		c.taxAreaCode = n.[Tax Area Code],
		c.shiptoName = n.[Ship-to Name],
		c.shiptoAddress = n.[Ship-to Address], 
		c.shiptoAddress2 = n.[Ship-to Address 2],
		c.shiptoCity = n.[Ship-to City],
		c.shiptoContact = n.[Ship-to Contact],
		c.shiptoZipcode = n.[Ship-to ZIP Code],
		c.shiptoState = n.[Ship-to State], 
		c.searchPhone = n.[Search Phone],
		c.billtoName = n.[Bill-to Name],
		c.billtoAddress = n.[Bill-to Address],
		c.billtoAddress2 = n.[Bill-to Address 2],
		c.billtoCity = n.[Bill-to City], 
		c.billtoState = n.[Bill-to State],
		c.billtoZipcode  = n.[Bill-to ZIP Code]
	FROM fs1.z_SANDBOX.dbo.[B&B Equipment & Supply Inc_$Customer] n WITH(NOLOCK) JOIN NAVCustomer c on n.[No_] = c.customerID
	WHERE DATEDIFF(DAY, [Last Date Modified], GETDATE()) =0
END
GO
