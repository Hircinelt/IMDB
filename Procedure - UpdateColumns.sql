USE [TEST]
GO
/****** Object:  StoredProcedure [dbo].[UpdateColumns]    Script Date: 8/29/2023 8:29:39 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


ALTER PROCEDURE [dbo].[UpdateColumns]
AS
BEGIN
    UPDATE IMDB
    SET Duration = LEFT(Duration, CHARINDEX(' ', Duration) - 1)
    WHERE CHARINDEX(' ', Duration) > 0;
	UPDATE IMDB
    SET Year = SUBSTRING(Year, PATINDEX('%[0-9]%', Year), 4)
    WHERE PATINDEX('%[0-9]%', Year) > 0;
	UPDATE IMDB
	SET Gross = SUBSTRING(Gross, 2, LEN(Gross) - 2)
    WHERE LEFT(Gross, 1) = '$';
END;


