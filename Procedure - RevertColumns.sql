USE [IMDB]
GO
/****** Object:  StoredProcedure [dbo].[RevertColumns]    Script Date: 9/5/2023 8:10:35 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

















ALTER PROCEDURE [dbo].[RevertColumns]
AS
BEGIN
	ALTER TABLE Movies ALTER COLUMN Year nvarchar(50)
	ALTER TABLE Movies ALTER COLUMN ID nvarchar(50)
	ALTER TABLE Movies ALTER COLUMN Duration nvarchar(50)
	ALTER TABLE Movies ALTER COLUMN Gross nvarchar(50)
	ALTER TABLE Movies ALTER COLUMN Votes nvarchar(50)
	ALTER TABLE Movies ALTER COLUMN Metascore nvarchar(50)
END;


