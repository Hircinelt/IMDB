USE [IMDB]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





ALTER PROCEDURE [dbo].[UpdateColumns]
AS
BEGIN
    UPDATE Movies
    SET Duration = LEFT(Duration, CHARINDEX(' ', Duration) - 1)
    WHERE CHARINDEX(' ', Duration) > 0;
	UPDATE Movies
    SET Year = SUBSTRING(Year, PATINDEX('%[0-9]%', Year), 4)
    WHERE PATINDEX('%[0-9]%', Year) > 0;
	DECLARE @Counter INT = 0;
    UPDATE Movies
    SET ID = @Counter, @Counter = @Counter + 1;
END;


GO


