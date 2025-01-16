-- PROJECT: World Layoffs - DATA CLEANING

# For this project, I drew upon a dataset that I found interesting on Kaggle: World Layoffs
	# https://www.kaggle.com/datasets/swaptr/layoffs-2022

# Imported the dataset into the SCHEMAS and confirm that it was successfull
SELECT *
FROM layoffs
;

-- STEPS THAT I WANT TO CONSIDER FOR THIS DATA CLEANING
# 1. Remove Duplicates
# 2. Standardize the Data (Spelling etc.) and fix errors
# 3. Look at Null Values or Blank Values
# 4. Remove any columns or rows that are unnecessary

-- CREATED A STAGING TABLE SEPARATE FROM THE RAW TABLE || BEST PRACTICE
CREATE TABLE layoffs_staging 
LIKE layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs
;

SELECT *
FROM layoffs_staging
;

-- STEP 1: Removing Duplicates

SELECT *,
ROW_NUMBER() OVER(
	PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
	SELECT *,
	ROW_NUMBER() OVER(
	PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
	FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

# checked to see if the returned values actually are duplicates:
SELECT *
FROM layoffs_staging
WHERE company = 'Oda';
# In this case there was an error where the CTE I created data returned "2" for ODA as a duplicate however, turns out it wasn't
# Realized the problem was that in the PARTITION BY section of the previous code block, I should've accounted for all columns, instead of just specific ones
# ERROR FIX:
WITH duplicate_cte AS
(
	SELECT *,
	ROW_NUMBER() OVER(
	PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
	FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

# Now checked again to see if the returned values are accurate and actually duplicates
SELECT *
FROM layoffs_staging
WHERE company = 'Casper';
# Confirmed now that previous CTE worked as it should

# Now to actually remove the duplicates, I created a new column and added those row numbers in. Then deleted where row numbers are over 2, followed by deleteing the column as a whole
# Start by creating a statement table of the layoffs_staging table

# Here I added "row_num" as a column and gave it the "INT" value
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2
;

INSERT INTO layoffs_staging2
SELECT *,
	ROW_NUMBER() OVER(
	PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
	FROM layoffs_staging
;

# Created another SELECT statement to confirm that data has been inserted properly || Best practice to always confirm with a select statement that changes have been made
SELECT *
FROM layoffs_staging2
;

# This statement is to delete the duplicates
DELETE
FROM layoffs_staging2
WHERE row_num > 1
;

# Once again confirm that the delete statement worked and removed the duplicates
SELECT *
FROM layoffs_staging2
WHERE row_num > 1
;

-- Step 2. Standardizing the Data
# Finding issues in the data and fixing it

# Started by triming the data set to make it more neat
SELECT company, TRIM(company)
FROM layoffs_staging2
;

UPDATE layoffs_staging2
SET company = trim(company)
;

# Cleared the discrepencies of multiple industry names like for example Crypto currency had three names || Purpose of this was to keep them all uniform
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1
;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%'
;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%'
;

# Now the industry tab is adjusted to only just having one category for Crypto
SELECT DISTINCT industry
FROM layoffs_staging2
;

# Fixed the problem where someone wrote United States twice (added a "." which created two columns) for the category of Country
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1
;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%'
;

# Noticed that the data format was currently set as "text" so converted data into "DATE"
SELECT `date`,
str_to_date(`date`, '%m/%d/%Y')
FROM layoffs_staging2
;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2
;

# Now that the data is in DATE format I altered the definition of the column from "text" to "date" || Was only able to do once the date format (M/D/Y) was established, otherwise Err appeared
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


-- Step 3. Removed NULLS and Blank Values
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

# UPDATE BLANK SPACES INTO NULLS
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON  t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL
;

UPDATE layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON  t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

# There is outliar that was for some reason not affected or changed "Bally's" so resolved that || In hindsight realized that this was because Bally's had only one row where others had multiple
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';


-- Final Step: Removed redundant columns and rows

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

# COMPLETED THE DATA CLEANING PROGESS 

# RECAP:
-- 1. REMOVED DUPLICATES
-- 2. STANDARDIZE THE DATA
-- 3. LOOK FOR AND REMOVED NULL OR BLANK VALUES
-- 4. REMOVED ANY UNNCESSARY COLUMNS OR ROWS