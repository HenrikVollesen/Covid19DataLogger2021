USE [master]
GO
/****** Object:  Database [Covid-19_Stats]    Script Date: 10-01-2022 19:04:20 ******/
CREATE DATABASE [Covid-19_Stats]
GO
/****** Object:  Login [Covid19_Writer]    Script Date: 10-01-2022 19:04:20 ******/
CREATE LOGIN [Covid19_Writer] WITH PASSWORD=N'Corona_2020_W', DEFAULT_DATABASE=[Covid-19_Stats], DEFAULT_LANGUAGE=[us_english], CHECK_EXPIRATION=OFF, CHECK_POLICY=ON
GO
/****** Object:  Login [Covid19_Reader]    Script Date: 10-01-2022 19:04:20 ******/
CREATE LOGIN [Covid19_Reader] WITH PASSWORD=N'Corona_2020', DEFAULT_DATABASE=[Covid-19_Stats], DEFAULT_LANGUAGE=[us_english], CHECK_EXPIRATION=OFF, CHECK_POLICY=ON
GO
USE [Covid-19_Stats]
GO
/****** Object:  User [Covid19_Writer]    Script Date: 10-01-2022 19:04:20 ******/
CREATE USER [Covid19_Writer] FOR LOGIN [Covid19_Writer] WITH DEFAULT_SCHEMA=[dbo]
GO
/****** Object:  User [Covid19_Reader]    Script Date: 10-01-2022 19:04:20 ******/
CREATE USER [Covid19_Reader] FOR LOGIN [Covid19_Reader] WITH DEFAULT_SCHEMA=[dbo]
GO
ALTER ROLE [db_datareader] ADD MEMBER [Covid19_Writer]
GO
ALTER ROLE [db_datawriter] ADD MEMBER [Covid19_Writer]
GO
ALTER ROLE [db_datareader] ADD MEMBER [Covid19_Reader]
GO
--ALTER ROLE [db_datawriter] ADD MEMBER [Covid19_Reader]
--GO
GRANT CONNECT TO [Covid19_Writer] AS [dbo]
GO
GRANT CONNECT TO [Covid19_Reader] AS [dbo]
GO
GRANT VIEW ANY COLUMN ENCRYPTION KEY DEFINITION TO [public] AS [dbo]
GO
GRANT VIEW ANY COLUMN MASTER KEY DEFINITION TO [public] AS [dbo]
GO
/****** Object:  UserDefinedFunction [dbo].[FirstBadDate]    Script Date: 10-01-2022 19:04:20 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date, ,>
-- Description:	<Description, ,>
-- =============================================
CREATE FUNCTION [dbo].[FirstBadDate] 
(
	-- Add the parameters for the function here
	@Param1 NVarChar(2)
)
RETURNS DateTime
AS
BEGIN
	-- Declare the return variable here
	DECLARE @ResultVar DateTime

	-- Add the T-SQL statements to compute the return value here
	SELECT @ResultVar = MIN(date) FROM DimDate 
	WHERE(date NOT IN (SELECT date FROM FactCovid19Stat WHERE(Alpha_2_code = @Param1))) 
	GROUP BY date 
	-- Return the result of the function
	RETURN @ResultVar

END
GO
ALTER AUTHORIZATION ON [dbo].[FirstBadDate] TO  SCHEMA OWNER 
GO
GRANT EXECUTE ON [dbo].[FirstBadDate] TO [Covid19_Writer] AS [dbo]
GO
/****** Object:  Table [dbo].[DimLocation]    Script Date: 10-01-2022 19:04:20 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[DimLocation](
	[Alpha_2_code] [nvarchar](10) NOT NULL,
	[ParentCode] [nvarchar](10) NULL,
	[Location] [nvarchar](50) NOT NULL,
	[Alpha_3_code] [nvarchar](10) NULL,
	[Numeric_code] [smallint] NULL,
	[Latitude_average] [float] NOT NULL,
	[Longitude_average] [float] NOT NULL,
	[IsCovidCountry] [smallint] NOT NULL,
 CONSTRAINT [PK_DimLocation] PRIMARY KEY CLUSTERED 
(
	[Alpha_2_code] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
ALTER AUTHORIZATION ON [dbo].[DimLocation] TO  SCHEMA OWNER 
GO
/****** Object:  UserDefinedFunction [dbo].[GetAPIStates]    Script Date: 10-01-2022 19:04:20 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE FUNCTION [dbo].[GetAPIStates] 
(	
)
RETURNS TABLE 
AS
RETURN 
(
	-- Add the SELECT statement with parameter references here
SELECT DISTINCT Alpha_2_code, [Location]
FROM            DimLocation
WHERE        (ParentCode IN ('AU', 'CA', 'CN', 'US'))
)
GO
ALTER AUTHORIZATION ON [dbo].[GetAPIStates] TO  SCHEMA OWNER 
GO
/****** Object:  UserDefinedFunction [dbo].[GetAPICountries]    Script Date: 10-01-2022 19:04:20 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE FUNCTION [dbo].[GetAPICountries] 
(	
)
RETURNS TABLE 
AS
RETURN 
(
	-- Add the SELECT statement with parameter references here
SELECT DISTINCT Alpha_2_code, [Location] 
FROM            DimLocation
WHERE        (IsCovidCountry = 1)
)
GO
ALTER AUTHORIZATION ON [dbo].[GetAPICountries] TO  SCHEMA OWNER 
GO
/****** Object:  Table [dbo].[DimDate]    Script Date: 10-01-2022 19:04:20 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[DimDate](
	[date] [datetime] NOT NULL,
	[Year] [int] NOT NULL,
	[Month] [int] NOT NULL,
	[Day] [int] NOT NULL,
 CONSTRAINT [PK_DimDate] PRIMARY KEY CLUSTERED 
(
	[date] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
ALTER AUTHORIZATION ON [dbo].[DimDate] TO  SCHEMA OWNER 
GO
/****** Object:  Table [dbo].[FactCovid19Stat]    Script Date: 10-01-2022 19:04:20 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[FactCovid19Stat](
	[Alpha_2_code] [nvarchar](10) NOT NULL,
	[date] [datetime] NOT NULL,
	[confirmed] [int] NOT NULL,
	[deaths] [int] NOT NULL,
	[recovered] [int] NOT NULL,
	[confirmedDay] [int] NOT NULL,
	[deathsDay] [int] NOT NULL,
 CONSTRAINT [PK_FactCovid19Stat] PRIMARY KEY CLUSTERED 
(
	[Alpha_2_code] ASC,
	[date] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER AUTHORIZATION ON [dbo].[FactCovid19Stat] TO  SCHEMA OWNER 
GO
/****** Object:  Table [dbo].[FactPopulation]    Script Date: 10-01-2022 19:04:20 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[FactPopulation](
	[Alpha_2_code] [nvarchar](10) NOT NULL,
	[Population] [float] NOT NULL,
	[Population_density] [float] NOT NULL
) ON [PRIMARY]
GO
ALTER AUTHORIZATION ON [dbo].[FactPopulation] TO  SCHEMA OWNER 
GO
/****** Insert:  Locations (2-character Alpha code, (), Country name,  3-character Alpha code, Numeric code, latitude, longitude ******/
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'AD', NULL, N'Andorra', N'AND', 20, 42.5, 1.6, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'AE', NULL, N'UAE
', N'ARE', 784, 24, 54, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'AF', NULL, N'Afghanistan', N'AFG', 4, 33, 65, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'AG', NULL, N'Antigua-and-Barbuda', N'ATG', 28, 17.05, -61.8, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'AI', NULL, N'Anguilla', N'AIA', 660, 18.25, -63.167, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'AL', NULL, N'Albania', N'ALB', 8, 41, 20, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'AM', NULL, N'Armenia', N'ARM', 51, 40, 45, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'AO', NULL, N'Angola', N'AGO', 24, -12.5, 18.5, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'AR', NULL, N'Argentina', N'ARG', 32, -34, -64, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'AT', NULL, N'Austria', N'AUT', 40, 47.333, 13.333, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'AU', NULL, N'Australia', N'AUS', 36, -27, 133, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'AU-NSW', N'AU', N'New South Wales', NULL, NULL, -33.8688, 151.2093, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'AU-NT', N'AU', N'Northern Territory', NULL, NULL, -12.4634, 130.8456, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'AU-QLD', N'AU', N'Queensland', NULL, NULL, -28.0167, 153.4, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'AU-SA', N'AU', N'South Australia', NULL, NULL, -34.9285, 138.6007, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'AU-TAS', N'AU', N'Tasmania', NULL, NULL, -41.4545, 145.9707, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'AU-VIC', N'AU', N'Victoria', NULL, NULL, -37.8136, 144.9631, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'AU-WA', N'AU', N'Western Australia', NULL, NULL, -31.9505, 115.8605, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'AW', NULL, N'Aruba', N'ABW', 533, 12.5, -69.967, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'AZ', NULL, N'Azerbaijan', N'AZE', 31, 40.5, 47.5, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'BA', NULL, N'Bosnia-and-Herzegovina', N'BIH', 70, 44, 18, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'BB', NULL, N'Barbados', N'BRB', 52, 13.167, -59.533, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'BD', NULL, N'Bangladesh', N'BGD', 50, 24, 90, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'BE', NULL, N'Belgium', N'BEL', 56, 50.833, 4, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'BF', NULL, N'Burkina-Faso', N'BFA', 854, 13, -2, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'BG', NULL, N'Bulgaria', N'BGR', 100, 43, 25, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'BH', NULL, N'Bahrain', N'BHR', 48, 26, 50.55, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'BI', NULL, N'Burundi', N'BDI', 108, -3.5, 30, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'BJ', NULL, N'Benin', N'BEN', 204, 9.5, 2.25, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'BM', NULL, N'Bermuda', N'BMU', 60, 32.333, -64.75, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'BN', NULL, N'Brunei', N'BRN', 96, 4.5, 114.667, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'BO', NULL, N'Bolivia', N'BOL', 68, -17, -65, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'BR', NULL, N'Brazil', N'BRA', 76, -10, -55, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'BS', NULL, N'Bahamas', N'BHS', 44, 24.25, -76, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'BT', NULL, N'Bhutan', N'BTN', 64, 27.5, 90.5, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'BW', NULL, N'Botswana', N'BWA', 72, -22, 24, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'BY', NULL, N'Belarus', N'BLR', 112, 53, 28, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'BZ', NULL, N'Belize', N'BLZ', 84, 17.25, -88.75, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CA', NULL, N'Canada', N'CAN', 124, 60, -95, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CA-AB', N'CA', N'Alberta', NULL, NULL, 53.9333, -116.5765, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CA-BC', N'CA', N'British Columbia', NULL, NULL, 49.2827, -123.1207, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CA-MB', N'CA', N'Manitoba', NULL, NULL, 53.7609, -98.8139, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CA-NB', N'CA', N'New Brunswick', NULL, NULL, 46.5653, -66.4619, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CA-NL', N'CA', N'Newfoundland and Labrador', NULL, NULL, 53.1355, -57.6604, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CA-NS', N'CA', N'Nova Scotia', NULL, NULL, 44.682, -63.7443, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CA-ON', N'CA', N'Ontario', NULL, NULL, 51.2538, -85.3232, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CA-PE', N'CA', N'Prince Edward Island', NULL, NULL, 46.5107, -63.4168, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CA-QC', N'CA', N'Quebec', NULL, NULL, 52.9399, -73.5491, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CA-SK', N'CN', N'Saskatchewan', NULL, NULL, 52.9399, -106.4509, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CD', NULL, N'Democratic-Republic-of-the-Congo', N'COD', 180, 0, 0, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CF', NULL, N'Central African Republic', N'CAF', 140, 0, 0, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CG', NULL, N'Congo', N'COG', 178, -1, 15, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CH', NULL, N'Switzerland', N'CHE', 756, 47, 8, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CI', NULL, N'Ivory-Coast', N'CIV', 384, 8, -5, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CL', NULL, N'Chile', N'CHL', 152, -30, -71, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CM', NULL, N'Cameroon', N'CMR', 120, 6, 12, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CN', NULL, N'China', N'CHN', 156, 35, 105, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CN-AH', N'CN', N'Anhui Sheng', NULL, NULL, 31.8257, 117.2264, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CN-BJ', N'CN', N'Beijing Shi', NULL, NULL, 40.1824, 116.4142, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CN-CQ', N'CN', N'Chongqing Shi', NULL, NULL, 30.0572, 107.874, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CN-FJ', N'CN', N'Fujian Sheng', NULL, NULL, 26.0789, 117.9874, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CN-GD', N'CN', N'Guangdong Sheng', NULL, NULL, 23.3417, 113.4244, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CN-GS', N'CN', N'Gansu Sheng', NULL, NULL, 36.0611, 103.8343, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CN-GX', N'CN', N'Guangxi', NULL, NULL, 23.8298, 108.7881, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CN-GZ', N'CN', N'Guizhou Sheng', NULL, NULL, 26.8154, 106.8748, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CN-HA', N'CN', N'Henan Sheng', NULL, NULL, 33.88202, 113.614, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CN-HB', N'CN', N'Hubei Sheng', NULL, NULL, 30.9756, 112.2707, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CN-HE', N'CN', N'Hebei Sheng', NULL, NULL, 38.0428, 114.5149, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CN-HI', N'CN', N'Hainan Sheng', NULL, NULL, 19.1959, 109.7453, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CN-HK', N'CN', N'Hong Kong SAR', NULL, NULL, 22.3, 114.2, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CN-HL', N'CN', N'Heilongjiang Sheng', NULL, NULL, 47.862, 127.7615, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CN-HN', N'CN', N'Hunan Sheng', NULL, NULL, 27.6104, 111.7088, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CN-JL', N'CN', N'Jilin Sheng', NULL, NULL, 43.6661, 126.1923, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CN-JS', N'CN', N'Jiangsu Sheng', NULL, NULL, 32.9711, 119.455, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CN-JX', N'CN', N'Jiangxi Sheng', NULL, NULL, 27.614, 115.7221, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CN-LN', N'CN', N'Liaoning Sheng', NULL, NULL, 41.2956, 122.6085, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CN-MO', N'CN', N'Macao SAR', NULL, NULL, 22.1667, 113.55, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CN-NM', N'CN', N'Nei Mongol', NULL, NULL, 44.0935, 113.9448, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CN-NX', N'CN', N'Ningxia', NULL, NULL, 37.2692, 106.1655, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CN-QH', N'CN', N'Qinghai Sheng', NULL, NULL, 35.7452, 95.9956, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CN-SC', N'CN', N'Sichuan Sheng', NULL, NULL, 30.6171, 102.7103, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CN-SD', N'CN', N'Shandong Sheng', NULL, NULL, 36.3427, 118.1498, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CN-SH', N'CN', N'Shanghai Shi', NULL, NULL, 31.202, 121.4491, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CN-SN', N'CN', N'Shaanxi Sheng', NULL, NULL, 35.1917, 108.8701, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CN-SX', N'CN', N'Shanxi Sheng', NULL, NULL, 37.5777, 112.2922, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CN-TJ', N'CN', N'Tianjin Shi', NULL, NULL, 39.3054, 117.323, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CN-XJ', N'CN', N'Xinjiang', NULL, NULL, 41.1129, 85.2401, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CN-XZ', N'CN', N'Xizang', NULL, NULL, 31.6927, 88.0924, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CN-YN', N'CN', N'Yunnan Sheng', NULL, NULL, 24.974, 101.487, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CN-ZJ', N'CN', N'Zhejiang Sheng', NULL, NULL, 29.1832, 120.0934, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CO', NULL, N'Colombia', N'COL', 170, 4, -72, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CR', NULL, N'Costa-Rica
', N'CRI', 188, 10, -84, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CU', NULL, N'Cuba', N'CUB', 192, 21.5, -80, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CV', NULL, N'Cabo-Verde', N'CPV', 132, 16, -24, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CY', NULL, N'Cyprus', N'CYP', 196, 35, 33, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'CZ', NULL, N'Czechia', N'CZE', 203, 49.75, 15.5, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'DE', NULL, N'Germany', N'DEU', 276, 51, 9, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'DJ', NULL, N'Djibouti', N'DJI', 262, 11.5, 43, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'DK', NULL, N'Denmark', N'DNK', 208, 56, 10, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'DM', NULL, N'Dominica', N'DMA', 212, 15.417, -61.333, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'DO', NULL, N'Dominican-Republic', N'DOM', 214, 19, -70.667, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'DZ', NULL, N'Algeria', N'DZA', 12, 28, 3, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'EC', NULL, N'Ecuador', N'ECU', 218, -2, -77.5, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'EE', NULL, N'Estonia', N'EST', 233, 59, 26, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'EG', NULL, N'Egypt', N'EGY', 818, 27, 30, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'EH', NULL, N'Western-Sahara
', N'ESH', 732, 24.5, -13, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'ER', NULL, N'Eritrea', N'ERI', 232, 15, 39, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'ES', NULL, N'Spain', N'ESP', 724, 40, -4, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'ET', NULL, N'Ethiopia', N'ETH', 231, 8, 38, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'FI', NULL, N'Finland', N'FIN', 246, 64, 26, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'FJ', NULL, N'Fiji', N'FJI', 242, -18, 175, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'FK', NULL, N'Falkland-Islands
', N'FLK', 238, -51.75, -59, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'FM', NULL, N'Micronesia
', N'FSM', 583, 6.917, 158.25, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'FO', NULL, N'Faeroe-Islands', N'FRO', 234, 62, -7, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'FR', NULL, N'France', N'FRA', 250, 46, 2, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'GA', NULL, N'Gabon', N'GAB', 266, -1, 11.75, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'GB', NULL, N'UK
', N'GBR', 826, 54, -2, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'GD', NULL, N'Grenada', N'GRD', 308, 12.117, -61.667, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'GE', NULL, N'Georgia', N'GEO', 268, 42, 43.5, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'GF', NULL, N'French-Guiana
', N'GUF', 254, 4, -53, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'GH', NULL, N'Ghana', N'GHA', 288, 8, -2, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'GI', NULL, N'Gibraltar', N'GIB', 292, 36.183, -5.367, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'GL', NULL, N'Greenland', N'GRL', 304, 72, -40, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'GM', NULL, N'Gambia', N'GMB', 270, 13.467, -16.567, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'GN', NULL, N'Guinea', N'GIN', 324, 11, -10, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'GP', NULL, N'Guadeloupe', N'GLP', 312, 16.25, -61.583, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'GQ', NULL, N'Equatorial-Guinea', N'GNQ', 226, 2, 10, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'GR', NULL, N'Greece', N'GRC', 300, 39, 22, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'GT', NULL, N'Guatemala', N'GTM', 320, 15.5, -90.25, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'GU', NULL, N'Guam', N'GUM', 316, 13.467, 144.783, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'GW', NULL, N'Guinea-Bissau', N'GNB', 624, 12, -15, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'GY', NULL, N'Guyana', N'GUY', 328, 0, 0, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'HK', NULL, N'Hong-Kong
', N'HKG', 344, 22.25, 114.167, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'HN', NULL, N'Honduras', N'HND', 340, 15, -86.5, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'HR', NULL, N'Croatia', N'HRV', 191, 45.167, 15.5, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'HT', NULL, N'Haiti', N'HTI', 332, 19, -72.417, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'HU', NULL, N'Hungary', N'HUN', 348, 47, 20, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'ID', NULL, N'Indonesia', N'IDN', 360, -5, 120, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'IE', NULL, N'Ireland', N'IRL', 372, 53, -8, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'IL', NULL, N'Israel', N'ISR', 376, 31.5, 34.75, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'IM', NULL, N'Isle-of-Man
', N'IMN', 833, 54.23, -4.55, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'IN', NULL, N'India', N'IND', 356, 20, 77, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'IQ', NULL, N'Iraq', N'IRQ', 368, 33, 44, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'IR', NULL, N'Iran
', N'IRN', 364, 32, 53, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'IS', NULL, N'Iceland', N'ISL', 352, 65, -18, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'IT', NULL, N'Italy', N'ITA', 380, 42.833, 12.833, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'JM', NULL, N'Jamaica', N'JAM', 388, 18.25, -77.5, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'JO', NULL, N'Jordan', N'JOR', 400, 31, 36, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'JP', NULL, N'Japan', N'JPN', 392, 36, 138, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'KE', NULL, N'Kenya', N'KEN', 404, 1, 38, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'KG', NULL, N'Kyrgyzstan', N'KGZ', 417, 41, 75, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'KH', NULL, N'Cambodia', N'KHM', 116, 13, 105, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'KM', NULL, N'Comoros', N'COM', 174, -12.167, 44.25, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'KN', NULL, N'Saint-Kitts-and-Nevis
', N'KNA', 659, 17.333, -62.75, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'KR', NULL, N'Korea', N'KOR', 410, 0, 0, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'KW', NULL, N'Kuwait', N'KWT', 414, 29.338, 47.658, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'KY', NULL, N'Cayman-Islands', N'CYM', 136, 19.5, -80.5, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'KZ', NULL, N'Kazakhstan', N'KAZ', 398, 48, 68, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'LA', NULL, N'Laos
', N'LAO', 418, 18, 105, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'LB', NULL, N'Lebanon', N'LBN', 422, 33.833, 35.833, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'LC', NULL, N'Saint-Lucia
', N'LCA', 662, 13.883, -61.133, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'LI', NULL, N'Liechtenstein', N'LIE', 438, 47.167, 9.533, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'LK', NULL, N'Sri-Lanka
', N'LKA', 144, 7, 81, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'LR', NULL, N'Liberia', N'LBR', 430, 6.5, -9.5, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'LS', NULL, N'Lesotho', N'LSO', 426, -29.5, 28.5, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'LT', NULL, N'Lithuania', N'LTU', 440, 56, 24, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'LU', NULL, N'Luxembourg', N'LUX', 442, 49.75, 6.167, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'LV', NULL, N'Latvia', N'LVA', 428, 57, 25, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'LY', NULL, N'Libya
', N'LBY', 434, 25, 17, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'MA', NULL, N'Morocco', N'MAR', 504, 32, -5, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'MC', NULL, N'Monaco', N'MCO', 492, 43.733, 7.4, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'MD', NULL, N'Moldova
', N'MDA', 498, 47, 29, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'ME', NULL, N'Montenegro', N'MNE', 499, 42, 19, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'MG', NULL, N'Madagascar', N'MDG', 450, -20, 47, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'MH', NULL, N'Marshall-Islands
', N'MHL', 584, 9, 168, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'MK', NULL, N'Republic of North Macedonia', N'MKD', 807, 0, 0, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'ML', NULL, N'Mali', N'MLI', 466, 17, -4, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'MM', NULL, N'Myanmar', N'MMR', 104, 22, 98, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'MN', NULL, N'Mongolia', N'MNG', 496, 46, 105, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'MO', NULL, N'Macao', N'MAC', 446, 22.167, 113.55, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'MQ', NULL, N'Martinique', N'MTQ', 474, 14.667, -61, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'MR', NULL, N'Mauritania', N'MRT', 478, 20, -12, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'MS', NULL, N'Montserrat', N'MSR', 500, 16.75, -62.2, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'MT', NULL, N'Malta', N'MLT', 470, 35.833, 14.583, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'MU', NULL, N'Mauritius', N'MUS', 480, -20.283, 57.55, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'MV', NULL, N'Maldives', N'MDV', 462, 3.25, 73, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'MW', NULL, N'Malawi', N'MWI', 454, -13.5, 34, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'MX', NULL, N'Mexico', N'MEX', 484, 23, -102, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'MY', NULL, N'Malaysia', N'MYS', 458, 2.5, 112.5, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'MZ', NULL, N'Mozambique', N'MOZ', 508, -18.25, 35, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'NA', NULL, N'Namibia', N'NAM', 516, -22, 17, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'NC', NULL, N'New-Caledonia
', N'NCL', 540, -21.5, 165.5, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'NE', NULL, N'Niger', N'NER', 562, 16, 8, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'NG', NULL, N'Nigeria', N'NGA', 566, 10, 8, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'NI', NULL, N'Nicaragua', N'NIC', 558, 13, -85, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'NL', NULL, N'Netherlands', N'NLD', 528, 52.5, 5.75, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'NO', NULL, N'Norway', N'NOR', 578, 62, 10, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'NP', NULL, N'Nepal', N'NPL', 524, 28, 84, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'NZ', NULL, N'New-Zealand
', N'NZL', 554, -41, 174, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'OM', NULL, N'Oman', N'OMN', 512, 21, 57, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'PA', NULL, N'Panama', N'PAN', 591, 9, -80, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'PE', NULL, N'Peru', N'PER', 604, -10, -76, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'PF', NULL, N'French-Polynesia
', N'PYF', 258, -15, -140, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'PG', NULL, N'Papua-New-Guinea
', N'PNG', 598, -6, 147, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'PH', NULL, N'Philippines', N'PHL', 608, 13, 122, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'PK', NULL, N'Pakistan', N'PAK', 586, 30, 70, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'PL', NULL, N'Poland', N'POL', 616, 52, 20, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'PM', NULL, N'Saint-Pierre-Miquelon
', N'SPM', 666, 46.833, -56.333, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'PR', NULL, N'Puerto-Rico
', N'PRI', 630, 18.25, -66.5, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'PS', NULL, N'Palestine
', N'PSE', 275, 32, 35.25, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'PT', NULL, N'Portugal', N'PRT', 620, 39.5, -8, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'PY', NULL, N'Paraguay', N'PRY', 600, -23, -58, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'QA', NULL, N'Qatar', N'QAT', 634, 25.5, 51.25, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'RO', NULL, N'Romania', N'ROU', 642, 46, 25, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'RS', NULL, N'Serbia', N'SRB', 688, 44, 21, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'RU', NULL, N'Russia
', N'RUS', 643, 60, 100, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'RW', NULL, N'Rwanda', N'RWA', 646, -2, 30, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'SA', NULL, N'Saudi-Arabia
', N'SAU', 682, 25, 45, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'SB', NULL, N'Solomon-Islands
', N'SLB', 90, -8, 159, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'SC', NULL, N'Seychelles', N'SYC', 690, -4.583, 55.667, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'SD', NULL, N'Sudan', N'SDN', 736, 15, 30, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'SE', NULL, N'Sweden', N'SWE', 752, 62, 15, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'SG', NULL, N'Singapore', N'SGP', 702, 1.367, 103.8, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'SI', NULL, N'Slovenia', N'SVN', 705, 46, 15, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'SK', NULL, N'Slovakia', N'SVK', 703, 48.667, 19.5, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'SL', NULL, N'Sierra-Leone
', N'SLE', 694, 8.5, -11.5, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'SM', NULL, N'San-Marino
', N'SMR', 674, 43.767, 12.417, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'SN', NULL, N'Senegal', N'SEN', 686, 14, -14, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'SO', NULL, N'Somalia', N'SOM', 706, 10, 49, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'SR', NULL, N'Suriname', N'SUR', 740, 4, -56, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'SS', NULL, N'South Sudan', N'SSD', 728, 0, 0, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'ST', NULL, N'Sao-Tome-and-Principe
', N'STP', 678, 1, 7, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'SV', NULL, N'El-Salvador', N'SLV', 222, 13.833, -88.917, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'SY', NULL, N'Syria
', N'SYR', 760, 35, 38, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'SZ', NULL, N'Eswatini', N'SWZ', 748, 0, 0, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'TC', NULL, N'Turks-and-Caicos
', N'TCA', 796, 21.75, -71.583, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'TD', NULL, N'Chad', N'TCD', 148, 15, 19, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'TG', NULL, N'Togo', N'TGO', 768, 8, 1.167, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'TH', NULL, N'Thailand', N'THA', 764, 15, 100, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'TJ', NULL, N'Tajikistan', N'TJK', 762, 39, 71, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'TL', NULL, N'Timor-Leste', N'TLS', 626, -8.55, 125.517, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'TN', NULL, N'Tunisia', N'TUN', 788, 34, 9, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'TR', NULL, N'Turkey', N'TUR', 792, 39, 35, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'TT', NULL, N'Trinidad-and-Tobago
', N'TTO', 780, 11, -61, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'TW', NULL, N'Taiwan
', N'TWN', 158, 23.5, 121, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'TZ', NULL, N'Tanzania
', N'TZA', 834, -6, 35, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'UA', NULL, N'Ukraine', N'UKR', 804, 49, 32, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'UG', NULL, N'Uganda', N'UGA', 800, 1, 32, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US', NULL, N'USA
', N'USA', 840, 38, -97, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-AK', N'US', N'Alaska', NULL, NULL, 63.588753, -154.493062, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-AL', N'US', N'Alabama', NULL, NULL, 32.318231, -86.902298, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-AR', N'US', N'Arkansas', NULL, NULL, 35.20105, -91.831833, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-AZ', N'US', N'Arizona', NULL, NULL, 34.048928, -111.093731, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-CA', N'US', N'California', NULL, NULL, 36.778261, -119.417932, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-CO', N'US', N'Colorado', NULL, NULL, 39.550051, -105.782067, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-CT', N'US', N'Connecticut', NULL, NULL, 41.603221, -73.087749, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-DC', N'US', N'District of Columbia', NULL, NULL, 38.9072, -77.0369, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-DE', N'US', N'Delaware', NULL, NULL, 38.910832, -75.52767, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-FL', N'US', N'Florida', NULL, NULL, 27.664827, -81.515754, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-GA', N'US', N'Georgia', NULL, NULL, 32.157435, -82.907123, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-HI', N'US', N'Hawaii', NULL, NULL, 19.898682, -155.665857, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-IA', N'US', N'Iowa', NULL, NULL, 41.878003, -93.097702, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-ID', N'US', N'Idaho', NULL, NULL, 44.068202, -114.742041, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-IL', N'US', N'Illinois', NULL, NULL, 40.633125, -89.398528, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-IN', N'US', N'Indiana', NULL, NULL, 40.551217, -85.602364, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-KS', N'US', N'Kansas', NULL, NULL, 39.011902, -98.484246, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-KY', N'US', N'Kentucky', NULL, NULL, 37.839333, -84.270018, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-LA', N'US', N'Louisiana', NULL, NULL, 31.244823, -92.145024, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-MA', N'US', N'Massachusetts', NULL, NULL, 42.407211, -71.382437, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-MD', N'US', N'Maryland', NULL, NULL, 39.045755, -76.641271, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-ME', N'US', N'Maine', NULL, NULL, 45.253783, -69.445469, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-MI', N'US', N'Michigan', NULL, NULL, 44.314844, -85.602364, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-MN', N'US', N'Minnesota', NULL, NULL, 46.729553, -94.6859, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-MO', N'US', N'Missouri', NULL, NULL, 37.964253, -91.831833, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-MS', N'US', N'Mississippi', NULL, NULL, 32.354668, -89.398528, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-MT', N'US', N'Montana', NULL, NULL, 46.879682, -110.362566, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-NC', N'US', N'North Carolina', NULL, NULL, 35.759573, -79.0193, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-ND', N'US', N'North Dakota', NULL, NULL, 47.551493, -101.002012, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-NE', N'US', N'Nebraska', NULL, NULL, 41.492537, -99.901813, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-NH', N'US', N'New Hampshire', NULL, NULL, 43.193852, -71.572395, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-NJ', N'US', N'New Jersey', NULL, NULL, 40.058324, -74.405661, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-NM', N'US', N'New Mexico', NULL, NULL, 34.97273, -105.032363, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-NV', N'US', N'Nevada', NULL, NULL, 38.80261, -116.419389, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-NY', N'US', N'New York', NULL, NULL, 43.299428, -74.217933, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-OH', N'US', N'Ohio', NULL, NULL, 40.417287, -82.907123, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-OK', N'US', N'Oklahoma', NULL, NULL, 35.007752, -97.092877, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-OR', N'US', N'Oregon', NULL, NULL, 43.804133, -120.554201, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-PA', N'US', N'Pennsylvania', NULL, NULL, 41.203322, -77.194525, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-RI', N'US', N'Rhode Island', NULL, NULL, 41.580095, -71.477429, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-SC', N'US', N'South Carolina', NULL, NULL, 33.836081, -81.163725, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-SD', N'US', N'South Dakota', NULL, NULL, 43.969515, -99.901813, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-TN', N'US', N'Tennessee', NULL, NULL, 35.517491, -86.580447, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-TX', N'US', N'Texas', NULL, NULL, 31.968599, -99.901813, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-UT', N'US', N'Utah', NULL, NULL, 39.32098, -111.093731, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-VA', N'US', N'Virginia', NULL, NULL, 37.431573, -78.656894, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-VT', N'US', N'Vermont', NULL, NULL, 44.558803, -72.577841, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-WA', N'US', N'Washington', NULL, NULL, 47.751074, -120.740139, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-WI', N'US', N'Wisconsin', NULL, NULL, 43.78444, -88.787868, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'US-WY', N'US', N'Wyoming', NULL, NULL, 43.075968, -107.290284, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'UY', NULL, N'Uruguay', N'URY', 858, -33, -56, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'UZ', NULL, N'Uzbekistan', N'UZB', 860, 41, 64, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'VC', NULL, N'Saint Vincent and the Grenadines', N'VCT', 670, 0, 0, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'VE', NULL, N'Venezuela
', N'VEN', 862, 8, -66, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'VN', NULL, N'Vietnam
', N'VNM', 704, 16, 106, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'VU', NULL, N'Vanuatu', N'VUT', 548, -16, 167, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'WF', NULL, N'Wallis-and-Futuna
', N'WLF', 876, -13.3, -176.2, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'WS', NULL, N'Samoa', N'WSM', 882, -13.583, -172.333, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'YE', NULL, N'Yemen', N'YEM', 887, 15, 48, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'YT', NULL, N'Mayotte', N'MYT', 175, -12.833, 45.167, 0)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'ZA', NULL, N'South-Africa
', N'ZAF', 710, -29, 24, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'ZM', NULL, N'Zambia', N'ZMB', 894, -15, 30, 1)
GO
INSERT [dbo].[DimLocation] ([Alpha_2_code], [ParentCode], [Location], [Alpha_3_code], [Numeric_code], [Latitude_average], [Longitude_average], [IsCovidCountry]) VALUES (N'ZW', NULL, N'Zimbabwe', N'ZWE', 716, -20, 30, 1)
GO
/****** Insert:  Population (2-character Alpha code, Population, Population_density ******/
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'AD', 77.27, 163.7553)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'AE', 9890.4, 112.4419)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'AF', 38928.34, 54.4222)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'AG', 97.93, 231.8455)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'AL', 2877.8, 104.8707)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'AM', 2963.23, 102.9312)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'AO', 32866.27, 23.8904)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'AR', 45195.78, 16.1769)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'AT', 9006.4, 106.7486)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'AU', 25499.88, 3.202)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'AW', 106.77, 584.8)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'AZ', 10139.18, 119.3089)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'BA', 3280.82, 68.4964)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'BB', 287.37, 664.4628)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'BD', 164689.38, 1265.0361)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'BE', 11589.62, 375.5637)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'BF', 20903.28, 70.1513)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'BG', 6948.45, 65.1805)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'BH', 1701.58, 1935.9066)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'BI', 11890.78, 423.0625)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'BJ', 12123.2, 99.1104)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'BM', 62.27, 1308.82)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'BN', 437.48, 81.3467)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'BO', 11673.03, 10.2018)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'BR', 212559.41, 25.0401)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'BS', 393.25, 39.4966)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'BT', 771.61, 21.1877)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'BW', 2351.63, 4.0437)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'BY', 9449.32, 46.8576)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'BZ', 397.62, 16.4262)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'CA', 37742.16, 4.0367)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'CG', 5518.09, 15.4048)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'CH', 8654.62, 214.2428)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'CI', 26378.28, 76.3986)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'CL', 19116.21, 24.2824)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'CM', 26545.86, 50.8847)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'CN', 1439323.77, 147.674)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'CO', 50882.88, 44.2232)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'CR', 5094.11, 96.0785)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'CU', 11326.62, 110.408)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'CV', 555.99, 135.5801)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'CY', 1207.36, 127.657)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'CZ', 10708.98, 137.1755)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'DE', 83783.95, 237.0163)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'DJ', 988, 41.2849)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'DK', 5792.2, 136.5199)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'DM', 71.99, 98.5667)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'DO', 10847.9, 222.8731)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'DZ', 43851.04, 17.3479)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'EC', 17643.06, 66.9385)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'EE', 1326.54, 31.0328)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'EG', 102334.4, 97.999)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'ES', 46754.78, 93.105)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'ET', 114963.58, 104.9574)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'FI', 5540.72, 18.1358)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'FJ', 896.44, 49.5622)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'FM', 115.02, 150.7771)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'FO', 48.87, 35.308)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'FR', 65273.51, 122.5784)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'GA', 2225.73, 7.8594)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'GB', 67886, 272.8982)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'GD', 112.52, 317.1324)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'GE', 3989.18, 65.032)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'GH', 31072.95, 126.7189)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'GI', 33.69, 3457.1)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'GL', 56.77, 0.1369)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'GM', 2416.66, 207.566)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'GN', 13132.79, 51.7547)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'GQ', 1402.99, 45.1939)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'GR', 10423.06, 83.4788)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'GT', 17915.57, 157.8341)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'GU', 168.78, 304.1278)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'GW', 1968, 66.1907)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'HK', 7496.99, 7039.7143)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'HN', 9904.61, 82.8051)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'HR', 4105.27, 73.7259)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'HT', 11402.53, 398.4481)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'HU', 9660.35, 108.0429)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'ID', 273523.62, 145.7252)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'IE', 4937.8, 69.8738)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'IL', 8655.54, 402.6063)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'IM', 85.03, 147.8719)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'IN', 1380004.39, 450.4186)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'IQ', 40222.5, 88.1254)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'IR', 83992.95, 49.831)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'IS', 341.25, 3.4043)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'IT', 60461.83, 205.8592)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'JM', 2961.16, 266.8789)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'JO', 10203.14, 109.2853)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'JP', 126476.46, 347.7776)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'KE', 53771.3, 87.3245)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'KG', 6524.19, 32.3332)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'KH', 16718.97, 90.6717)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'KM', 869.6, 437.352)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'KN', 53.19, 212.8654)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'KW', 4270.56, 232.1284)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'KY', 65.72, 256.4958)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'KZ', 18776.71, 6.6814)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'LA', 7275.56, 29.7147)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'LB', 6825.44, 594.5608)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'LC', 183.63, 293.1869)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'LI', 38.14, 237.0125)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'LK', 21413.25, 341.955)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'LR', 5057.68, 49.1269)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'LS', 2142.25, 73.5619)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'LT', 2722.29, 45.1352)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'LU', 625.98, 231.4475)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'LV', 1886.2, 31.2116)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'LY', 6871.29, 3.6229)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'MA', 36910.56, 80.0797)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'MC', 39.24, 19347.5)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'MD', 4033.96, 123.6545)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'ME', 628.06, 46.2804)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'MG', 27691.02, 43.9513)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'MH', 59.19, 295.15)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'ML', 20250.83, 15.196)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'MM', 54409.79, 81.7214)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'MN', 3278.29, 1.9797)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'MO', 649.34, 20546.7657)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'MR', 4649.66, 4.2885)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'MT', 441.54, 1454.0375)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'MU', 1271.77, 622.9621)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'MV', 540.54, 1454.4333)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'MW', 19129.96, 197.5191)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'MX', 128932.75, 66.4437)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'MY', 32366, 96.254)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'MZ', 31255.44, 37.7284)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'NA', 2540.92, 3.0776)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'NC', 285.49, 15.3425)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'NE', 24206.64, 16.9554)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'NG', 206139.59, 209.5878)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'NI', 6624.55, 51.6668)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'NL', 17134.87, 508.5442)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'NO', 5421.24, 14.4621)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'NP', 29136.81, 204.4297)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'NZ', 4822.23, 18.2063)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'OM', 5106.62, 14.9798)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'PA', 4314.77, 55.133)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'PE', 32971.85, 25.1293)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'PF', 280.9, 77.3243)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'PG', 8947.03, 18.2201)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'PH', 109581.09, 351.8734)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'PK', 220892.33, 255.5728)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'PL', 37846.61, 124.027)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'PR', 2860.84, 376.2319)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'PS', 5101.42, 778.2022)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'PT', 10196.71, 112.3707)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'PY', 7132.53, 17.144)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'QA', 2881.06, 227.3222)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'RO', 19237.68, 85.1293)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'RS', 8737.37, 80.2912)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'RU', 145934.46, 8.8231)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'RW', 12952.21, 494.8685)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'SA', 34813.87, 15.3223)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'SB', 686.88, 21.8415)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'SC', 98.34, 208.3543)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'SE', 10099.27, 24.7176)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'SG', 5850.34, 7915.7306)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'SI', 2078.93, 102.6191)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'SK', 5459.64, 113.1284)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'SL', 7976.99, 104.6995)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'SM', 33.94, 556.6667)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'SN', 16743.93, 82.3278)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'SO', 15893.22, 23.5001)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'SR', 586.63, 3.6116)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'ST', 219.16, 212.8406)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'SV', 6486.2, 307.8114)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'TC', 38.72, 37.3116)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'TD', 16425.86, 11.8329)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'TG', 8278.74, 143.3663)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'TH', 69799.98, 135.1319)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'TJ', 9537.64, 64.2813)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'TL', 1318.44, 87.1763)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'TN', 11818.62, 74.2284)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'TR', 84339.07, 104.9141)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'TT', 1399.49, 266.886)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'TZ', 59734.21, 64.6986)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'UA', 43733.76, 77.3898)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'UG', 45741, 213.759)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'US', 331002.65, 35.6078)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'UY', 3473.73, 19.7506)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'UZ', 33469.2, 76.1335)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'VE', 28435.94, 36.2531)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'VN', 97338.58, 308.1266)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'VU', 307.15, 22.6615)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'WS', 198.41, 69.4134)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'YE', 29825.97, 53.5076)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'ZA', 59308.69, 46.7543)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'ZM', 18383.96, 22.9948)
GO
INSERT [dbo].[FactPopulation] ([Alpha_2_code], [Population], [Population_density]) VALUES (N'ZW', 14862.93, 42.7295)
GO
ALTER TABLE [dbo].[DimDate] ADD  CONSTRAINT [DF_DimDate_Year]  DEFAULT ((2020)) FOR [Year]
GO
ALTER TABLE [dbo].[DimDate] ADD  CONSTRAINT [DF_DimDate_Month]  DEFAULT ((0)) FOR [Month]
GO
ALTER TABLE [dbo].[DimDate] ADD  CONSTRAINT [DF_DimDate_Day]  DEFAULT ((0)) FOR [Day]
GO
ALTER TABLE [dbo].[DimLocation] ADD  CONSTRAINT [DF_DimLocation_Latitude_average]  DEFAULT ((0.0)) FOR [Latitude_average]
GO
ALTER TABLE [dbo].[DimLocation] ADD  CONSTRAINT [DF_DimLocation_Longitude_average]  DEFAULT ((0.0)) FOR [Longitude_average]
GO
ALTER TABLE [dbo].[DimLocation] ADD  CONSTRAINT [DF_DimLocation_IsCovidCountry]  DEFAULT ((0)) FOR [IsCovidCountry]
GO
ALTER TABLE [dbo].[FactCovid19Stat] ADD  CONSTRAINT [DF_Table_1_Confirmed]  DEFAULT ((0)) FOR [confirmed]
GO

ALTER TABLE [dbo].[FactCovid19Stat] ADD  CONSTRAINT [DF_Table_1_Deaths]  DEFAULT ((0)) FOR [deaths]
GO

ALTER TABLE [dbo].[FactCovid19Stat] ADD  CONSTRAINT [DF_Covid19_DayStat_recovered]  DEFAULT ((0)) FOR [recovered]
GO

ALTER TABLE [dbo].[FactCovid19Stat] ADD  CONSTRAINT [DF_FactCovid19Stat_confirmedDay]  DEFAULT ((0)) FOR [confirmedDay]
GO

ALTER TABLE [dbo].[FactCovid19Stat] ADD  CONSTRAINT [DF_FactCovid19Stat_deathsDay]  DEFAULT ((0)) FOR [deathsDay]
GO

ALTER TABLE [dbo].[FactCovid19Stat]  WITH CHECK ADD  CONSTRAINT [FK_FactCovid19Stat_DimDate] FOREIGN KEY([date])
REFERENCES [dbo].[DimDate] ([date])
ON UPDATE CASCADE
ON DELETE CASCADE
GO

ALTER TABLE [dbo].[FactCovid19Stat] CHECK CONSTRAINT [FK_FactCovid19Stat_DimDate]
GO

ALTER TABLE [dbo].[FactCovid19Stat]  WITH CHECK ADD  CONSTRAINT [FK_FactCovid19Stat_DimLocation] FOREIGN KEY([Alpha_2_code])
REFERENCES [dbo].[DimLocation] ([Alpha_2_code])
ON UPDATE CASCADE
ON DELETE CASCADE
GO

ALTER TABLE [dbo].[FactCovid19Stat] CHECK CONSTRAINT [FK_FactCovid19Stat_DimLocation]
GO
ALTER TABLE [dbo].[FactPopulation]  WITH CHECK ADD  CONSTRAINT [FK_FactPopulation_DimLocation] FOREIGN KEY([Alpha_2_code])
REFERENCES [dbo].[DimLocation] ([Alpha_2_code])
ON UPDATE CASCADE
ON DELETE CASCADE
GO
ALTER TABLE [dbo].[FactPopulation] CHECK CONSTRAINT [FK_FactPopulation_DimLocation]
GO
/****** Object:  StoredProcedure [dbo].[Save_Date]    Script Date: 10-01-2022 19:04:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[Save_Date] 
	@date datetime
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;
	DECLARE @Count int

    -- Insert statements for procedure here
	SELECT @Count = COUNT(date) FROM DimDate
	WHERE date = @date
	IF (@Count = 0) 
	BEGIN
		INSERT INTO DimDate ([date], Year, Month, Day) VALUES (@date, Year(@date), Month(@date), Day(@date))
	END
END
GO
ALTER AUTHORIZATION ON [dbo].[Save_Date] TO  SCHEMA OWNER 
GO
GRANT EXECUTE ON [dbo].[Save_Date] TO [Covid19_Writer] AS [dbo]
GO
/****** Object:  StoredProcedure [dbo].[Save_DayStat]    Script Date: 10-01-2022 19:04:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[Save_DayStat]
	-- Add the parameters for the stored procedure here
	@Alpha_2_code nvarchar(10),
	@date datetime,
	@confirmed int,
	@deaths int,
	@recovered int,
	@confirmedDay int,
	@deathsDay int

AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;
	DECLARE @Count int

    -- Insert statements for procedure here
	SELECT @Count = COUNT(confirmed) FROM FactCovid19Stat
	WHERE date = @date AND Alpha_2_code = @Alpha_2_code
	IF (@Count = 0) 
	BEGIN
		INSERT INTO FactCovid19Stat (Alpha_2_code, [date],
			confirmed, deaths, recovered, confirmedDay, deathsDay) VALUES (@Alpha_2_code, 
			@date, @confirmed, @deaths, @recovered, @confirmedDay, @deathsDay)
	END
END
GO
ALTER AUTHORIZATION ON [dbo].[Save_DayStat] TO  SCHEMA OWNER 
GO
GRANT EXECUTE ON [dbo].[Save_DayStat] TO [Covid19_Writer] AS [dbo]
GO
USE [master]
GO
ALTER DATABASE [Covid-19_Stats] SET  READ_WRITE 
GO
