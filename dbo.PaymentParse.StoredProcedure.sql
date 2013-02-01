USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[PaymentParse]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*

      You can use the information below to quickly parse out a delimited string through T-SQL into a temp table.

      This informaton was modeled after a solution you will find if you follow the link in the

      External Links section of this article.

*/
CREATE procedure [dbo].[PaymentParse]
	@Order varchar(10)
AS
 

DECLARE @Delimiter Char(1)

DECLARE @Delimiter2 Char(1)

DECLARE @StringToParse VarChar(MAX) 

DECLARE @ParseName VarChar(MAX) 

DECLARE @CharactersLeftToParse INT

DECLARE @LoopCounter INT

DECLARE @ParsedString VarChar(100)

DECLARE @ParsedIndex INT

--DECLARE @Order INT

--DECLARE @sql_str nvarchar (4000)
 

SET @Delimiter = '='

SET @Delimiter2 = ','

--SET @Order = '528090'

---------------------------------


CREATE TABLE #TEMPORD 
(
	value1 VARCHAR(200)
)

DECLARE @sql_str nvarchar (4000)

--SET @Order = '528308'

SET @sql_str = 'select pay_secdat from katom_mm5.s01_Orders where id = ''' + @Order + ''''

SET @sql_str = N'insert into #TEMPORD select pay_secdat from OPENQUERY(MYSQL, ''' + REPLACE(@sql_str, '''', '''''') + ''')'

PRINT @sql_str

EXEC (@sql_str)


---------------------------------

SET @StringToParse = (SELECT * FROM #TEMPORD)

SET @CharactersLeftToParse =Len(@StringToParse) 

SET @ParsedIndex = 0

SET @LoopCounter = 1


CREATE TABLE #ParsedValuesTable

(
      ParsedValue VarChar(100),
      ParsedIndex VarChar(100)
)
/*Handler for any fields without delimiter*/
IF (CHARINDEX(@Delimiter, @StringToParse,1) = 0)

      PRINT @StringToParse

/*Detects presence of delimiters*/
WHILE (CHARINDEX(@Delimiter, @StringToParse,1) <> 0)  

      BEGIN 

            IF @LoopCounter = 1 

			/*SET @ParsedString = SUBSTRING(@StringToParse, @LoopCounter, CHARINDEX(@Delimiter,@StringToParse,1) - 1)*/
                IF (CHARINDEX(@Delimiter2, @StringToParse,1) <> 0)
                
                SET @ParsedString = SUBSTRING(@StringToParse, CHARINDEX(@Delimiter,@StringToParse,1)+1, (CHARINDEX(@Delimiter2,@StringToParse,1)- 1)-(CHARINDEX(@Delimiter,@StringToParse,1)))
                
                ELSE 
                
                SET @ParsedString = SUBSTRING(@StringToParse, CHARINDEX(@Delimiter,@StringToParse,1)+1, Len(@StringToParse)-CHARINDEX(@Delimiter,@StringToParse,1))

			SET @ParsedIndex = @ParsedIndex+1
			
			IF @ParsedIndex = 1 
				SET @ParseName = 'EXPMO'
			ELSE IF @ParsedIndex = 2
				SET @ParseName = 'EXPYR'
			ELSE IF @ParsedIndex = 3
				SET @ParseName = 'NAME'
			ELSE IF @ParsedIndex = 4
				SET @ParseName = 'CCNUM'
			
			
			
            INSERT INTO #ParsedValuesTable (ParsedValue, ParsedIndex) VALUES (@ParsedString, @ParseName)

			IF (CHARINDEX(@Delimiter2, @StringToParse,1) <> 0)
            SET @StringToParse = SUBSTRING(@StringToParse, CHARINDEX(@Delimiter2,@StringToParse,1)+1, Len(@StringToParse))
			ELSE
			SET @StringToParse = ''
			--print @stringtoparse

            SET @CharactersLeftToParse = @CharactersLeftToParse - 1
            --print @CharactersLeftToParse

	
      END

update order_hdr 
set 
ccnum = (select parsedvalue from #ParsedValuesTable where ParsedIndex = 'CCNUM'),
ccname = (select replace(parsedvalue, '+', ' ') from #ParsedValuesTable where ParsedIndex = 'NAME'),
ccexpmo = (select parsedvalue from #ParsedValuesTable where ParsedIndex = 'EXPMO'),
ccexpyr = (select parsedvalue from #ParsedValuesTable where ParsedIndex = 'EXPYR')
where customerpo = @Order


DROP TABLE #ParsedValuesTable

DROP TABLE #TEMPORD




/*

DECLARE @Delimiter Char(1)

DECLARE @Delimiter2 Char(1)

DECLARE @StringToParse VarChar(MAX) 

 

SET @Delimiter = '='

SET @Delimiter2 = ','

SET @StringToParse = (select pay_secdat from openquery (MYSQL, 'select pay_secdat from katom_mm5.s01_Orders where id = 525524'))

PRINT SUBSTRING(@StringToParse, CHARINDEX(@Delimiter,@StringToParse,1)+1, (CHARINDEX(@Delimiter2,@StringToParse,1)- 1)-(CHARINDEX(@Delimiter,@StringToParse,1)))
 */
GO
