-- PROJECT: World Layoffs Data Set - Exploratory Data Analysis
# In this part 2 of the project I wanted to explore the data and find trends or patterns and anything interesting like outliers

SELECT *
FROM layoffs_staging2
;

# The columns of the dataset that I want to focus most on are these, because they provide a great insight on how COVID affected the Job Market during those years
SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2
;

# This statement showcases all categories for which companys had a 100% layoff (Essentially shut down completely) || Note this is for PERCENTAGE
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC
;

# This statement showcases which company had the most total layoffs || Amazon being the top contributor to this
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC
;

# This statement showcases the minimum and maximum date range within this dataset || Start of 2020 to the first 3 months of 2023
# Implies that all these metrics occurred within the last three years
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2
;
 
# This statement showcases which Industry had the most layoffs (sum) || Consumer and Retail being the top contributors
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC
;

# This statement showcases which country had the most layoffs (sum) || United States with a considerably high number of layoffs compared to the rest of the other countries
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC
;

# This statement, I wanted to explore around to see which years had the most layoffs and if the made sense with the COVID Lockdown timeline
SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC
;

# This statement was to showcase at which stages of a companies had the most layoffs (sum)
SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC
;

# In these following statements I wanted to explore around with the ROLLING SUM & PROGRESSION OF LAYOFF by Year || First 3 months of 2023 had an astronomical amount of layoffs compared to previous years
SELECT SUBSTRING(`date`,1,7) AS `Month`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1 ASC
;

WITH Rolling_Total AS
(
	SELECT SUBSTRING(`date`,1,7) AS `Month`, SUM(total_laid_off) AS Total_off
	FROM layoffs_staging2
	WHERE SUBSTRING(`date`,1,7) IS NOT NULL
	GROUP BY `Month`
	ORDER BY 1 ASC
)
SELECT `Month`, total_off
,SUM(Total_off) OVER(ORDER BY `Month`) AS rolling_total
FROM Rolling_Total
;

# Using the previous statements, I want to target and see how individual companies compared in the Rolling Totals by Year
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC
;

# This statement showcases the larger companys and the total layoffs for individual years
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

# To be more specific, these following statements I want to focus on the top 5 companies with layoffs per year
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

# Created two CTE's in order to determine top 5 ranked companies with the most laidoffs for the three years and queried the results
# From these statements, what I found interesting aside from just Covid being a factor for layoffs - in the 2023 year the top ranked companies with layoffs were all TECH companies
# My insight and thought process here as to why that is interesting is because those potential layoffs could also be a result of the SILICON VALLEY collpase that occurred as well which resulted
# in major TECH employee layoffs
WITH Company_year (company, years, total_laid_off) AS
(
	SELECT company, YEAR(`date`), SUM(total_laid_off)
	FROM layoffs_staging2
	GROUP BY company, YEAR(`date`)
	ORDER BY 3 DESC
), Company_Year_Rank AS
(
	SELECT *, 
	DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Rankings
	FROM Company_year
	WHERE years IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
WHERE Rankings <= 5
;
