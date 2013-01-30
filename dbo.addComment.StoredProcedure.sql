USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[addComment]    Script Date: 01/30/2013 16:26:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[addComment]
	@code varchar(100),
	@poNum varchar(100),
	@notes varchar(max),
	@docType varchar(100)
AS
BEGIN	
	DECLARE @currentNotes varchar(max), @id int	

    IF EXISTS(SELECT * FROM comment WHERE prodNum=@code AND documentNum = @poNum AND documentType=@docType)
	  BEGIN	
		SELECT @currentNotes = UPPER(LTRIM(RTRIM(ISNULL(notes, '')))), @id = id
		FROM comment
		WHERE prodNum=@code AND documentNum = @poNum AND documentType=@docType
		
		IF UPPER(LTRIM(RTRIM(@notes))) <> @currentNotes
		  BEGIN
			UPDATE comment SET notes = @currentNotes + 
				CASE 
					WHEN LEN(@currentNotes) > 0 THEN '<BR />' 
				END 
				+ UPPER(@notes)
			WHERE id = @id
		  END		
	  END
	 ELSE
	  BEGIN	
	   INSERT INTO comment(prodNum, documentNum, notes, documentType)
					VALUES(@code, @poNum, @notes, @docType)
	  END
	 
	 SELECT notes FROM comment WHERE prodNum=@code AND documentNum = @poNum AND documentType=@docType
END
GO
