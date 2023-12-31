USE [IMDB]
GO

/****** Object:  StoredProcedure [dbo].[UpdateColumns]    Script Date: 9/5/2023 7:32:06 PM ******/
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
    UPDATE Movies
    SET ID = REPLACE(ID, ',', '')
	UPDATE Movies
    SET Decade = LEFT(Year, 3) + '0s'
    WHERE Year IS NOT NULL;
	UPDATE Movies
    SET Rated = CASE WHEN Metascore <> '' THEN 'Y' ELSE 'N' END;
	UPDATE Movies
    SET DurationRange = CASE
        WHEN Duration <= 60 THEN '0-60'
        WHEN Duration BETWEEN 61 AND 90 THEN '61-90'
        WHEN Duration BETWEEN 91 AND 120 THEN '91-120'
        WHEN Duration BETWEEN 121 AND 150 THEN '121-150'
        WHEN Duration BETWEEN 151 AND 180 THEN '151-180'
        ELSE '>180'
		END
	UPDATE Movies
    SET RatingRange = CASE
        WHEN Rating >= 0 AND Rating < 1 THEN '0+'
        WHEN Rating >= 1 AND Rating < 2 THEN '1+'
        WHEN Rating >= 2 AND Rating < 3 THEN '2+'
        WHEN Rating >= 3 AND Rating < 4 THEN '3+'
        WHEN Rating >= 4 AND Rating < 5 THEN '4+'
        WHEN Rating >= 5 AND Rating < 6 THEN '5+'
        WHEN Rating >= 6 AND Rating < 7 THEN '6+'
        WHEN Rating >= 7 AND Rating < 8 THEN '7+'
        WHEN Rating >= 8 AND Rating < 9 THEN '8+'
		WHEN Rating >= 9 THEN '9+'
        ELSE ''
        END;
	UPDATE Movies
    SET MetascoreRange = CASE
        WHEN Metascore >= 0 AND Metascore < 10 THEN '0+'
        WHEN Metascore >= 10 AND Metascore < 20 THEN '10+'
        WHEN Metascore >= 20 AND Metascore < 30 THEN '20+'
		WHEN Metascore >= 30 AND Metascore < 40 THEN '30+'
		WHEN Metascore >= 40 AND Metascore < 50 THEN '40+'
		WHEN Metascore >= 50 AND Metascore < 60 THEN '50+'
		WHEN Metascore >= 60 AND Metascore < 70 THEN '60+'
		WHEN Metascore >= 70 AND Metascore < 80 THEN '70+'
		WHEN Metascore >= 80 AND Metascore < 90 THEN '80+'
		WHEN Metascore >= 90 THEN '90+'
        ELSE ''
        END;
	ALTER TABLE Movies ALTER COLUMN Year int
	ALTER TABLE Movies ALTER COLUMN ID int
	ALTER TABLE Movies ALTER COLUMN Duration int
	ALTER TABLE Movies ALTER COLUMN Gross int
	ALTER TABLE Movies ALTER COLUMN Votes int
	ALTER TABLE Movies ALTER COLUMN Metascore int
END;


GO


