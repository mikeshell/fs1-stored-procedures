USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[downloadMIVAtables]    Script Date: 01/30/2013 16:26:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[downloadMIVAtables]
AS
BEGIN

/** Update products - Temporarily here until the full synch is created */
drop table ProductsMIVA
select * into ProductsMIVA from openquery(MYSQL, 'select * from katom_mm5.s01_Products')


-- IMPORTING CATEGOGY'S INFO, RELATED PRODUCT & CUSTOM FIELD FROM MIVA - DAVID LU 02/16/2010
DROP TABLE mivaCategories
SELECT * INTO mivaCategories FROM OPENQUERY(MYSQL, 'select * from katom_mm5.s01_Categories')
DROP TABLE MIVACatXProd
SELECT * INTO MIVACatXProd FROM OPENQUERY(MYSQL, 'select * from katom_mm5.s01_CategoryXProduct')
DROP TABLE MIVARelProd
SELECT * INTO MIVARelProd FROM OPENQUERY(MYSQL, 'select * from katom_mm5.s01_RelatedProducts')
DROP TABLE MivaprodFields
SELECT * INTO MivaprodFields FROM OPENQUERY(MYSQL, 'select * from katom_mm5.s01_CFM_ProdFields')
DROP TABLE MivaprodFieldsValue
SELECT * INTO MivaprodFieldsValue FROM OPENQUERY(MYSQL, 'select * from katom_mm5.s01_CFM_ProdValues')
DROP TABLE MIVAVirtualCat
SELECT * INTO MIVAVirtualCat FROM OPENQUERY(MYSQL, 'select * from katom_mm5.s01_NBVIRTCAT_cats')
DROP TABLE mivaProductExtendedDesc
SELECT * INTO mivaProductExtendedDesc FROM OPENQUERY(MYSQL, 'select * from katom_mm5.ProductExtendedDescription')

DROP TABLE MivaAttributes
SELECT * INTO MivaAttributes FROM OPENQUERY(MYSQL, 'select * from katom_mm5.s01_Attributes')
DROP TABLE mivaAttributesTemplates
SELECT * INTO mivaAttributesTemplates FROM OPENQUERY(MYSQL, 'select * from katom_mm5.s01_AttributeTemplates')
DROP TABLE mivaAttributesTemplateOptions
SELECT * INTO mivaAttributesTemplateOptions FROM OPENQUERY(MYSQL, 'select * from katom_mm5.s01_AttributeTemplateOptions')
DROP TABLE mivaAttributesTemplateAttrs
SELECT * INTO mivaAttributesTemplateAttrs FROM OPENQUERY(MYSQL, 'select * from katom_mm5.s01_AttributeTemplateAttrs')


END
GO
