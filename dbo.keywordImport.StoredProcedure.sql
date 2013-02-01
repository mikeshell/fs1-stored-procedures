USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[keywordImport]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[keywordImport] 
AS
BEGIN
	
	INSERT INTO keywordSearched(searchDT, keyword, ipaddress)
	SELECT * FROM openquery(MYSQL, '
	SELECT *
	FROM katom_mm5.SLISearchTerm
	WHERE DATEDIFF(date, now()) = -1')

	DECLARE @newkeyword int, @sqlCommand varchar(max)

	set @sqlCommand = ''
	set @newkeyword = 0
	truncate table keywordWorktable
	
	insert into keywordWorktable(keyword, numSearched)
	select dbo.keywordCleaner(replace(replace(replace(replace(keyword, '/', '-'), '@', '-'), '(', ''), ')', '')), count(*)
	from keywordSearched
	where isnull(ipAddress, '') not in (select ipAddress from iplookup)
		and LEN(ltrim(rtrim(keyword))) > 2
		and DATEDIFF(day, searchDT, GETDATE()) = 1
		and (ltrim(rtrim(keyword)) not like '%katom%'
				and ltrim(rtrim(keyword)) not like '%return%')
		and keyword not in (select keyword from keywordSearched where LEN(ltrim(rtrim(keyword))) = 3 and ISNUMERIC(keyword) = 1)		
		and keyword not in (select keyword from keywordNegative)
	group by dbo.keywordCleaner(replace(replace(replace(replace(keyword, '/', '-'), '@', '-'), '(', ''), ')', ''))
	order by 2 desc

	update s
	set s.timeSearched = s.timeSearched + kw.numSearched,
		s.updatedt = GETDATE()
	from keywordWorktable kw join searchTerm s on kw.keyword = s.keyword

	insert into searchTerm
	select *, 0 
	from keywordWorktable 
	where keyword not in (select keyword from searchTerm)

	--select @newkeyword =
	--		CASE 
	--			WHEN COUNT(*) > 2499 THEN 0
	--			ELSE 2500-COUNT(*) 
	--		END 
	--from searchTerm where DATEDIFF(day, updatedt, GETDATE()) = 0 and addToMap = 0


	--IF @newkeyword > 0 
	--   BEGIN
	--	update searchTerm
	--	set addToMap = 1
	--	where addToMap = 0 and DATEDIFF(day, updatedt, GETDATE()) = 0 and timeSearched > 9

	--	SET @sqlCommand = 'update searchTerm
	--						set addToMap = 1
	--						where addToMap = 0
	--								AND keyword in (select top ' + cast(@newkeyword as varchar(10)) + ' keyword 
	--												from searchterm where  where timeSearched > 9 and addtoMap = 0 order by timesearched desc' 
	--	exec (@sqlCommand)
	--   END
	   
	--truncate table keywordWorktable
	--set @newkeyword = 0
	--set @sqlCommand = ''
	
END
GO
