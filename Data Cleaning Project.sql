-- Data Cleaning
 
SELECT * 
FROM layoffs;

-- 1. Remove Duplicates 
-- 2. Standardize the Data
-- 3. Null values or Blank values
-- 4. Remove any columns

-- 1. Remove Duplicates

CREATE TABLE layoffs_staging
like layoffs;

SELECT * 
FROM layoffs_staging;

INSERT layoffs_staging
SELECT * FROM layoffs;

SELECT *,
        ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`,stage,country,funds_raised_millions) AS row_num 
    FROM layoffs_staging;
    
WITH duplicate_cte AS (
   SELECT *,
        ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`,stage,country,funds_raised_millions) AS row_num 
    FROM layoffs_staging
) 

select * 
from duplicate_cte
where row_num>1;

delete 
from duplicate_cte
where row_num>1;

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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * 
FROM layoffs_staging2;

insert into layoffs_staging2
SELECT *,
        ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`,stage,country,funds_raised_millions) AS row_num 
    FROM layoffs_staging;
    
SELECT * 
FROM layoffs_staging2
where row_num>1;

DELETE FROM layoffs_staging2 WHERE row_num > 1;

-- 2. Standardizing Data

select company,trim(company) from layoffs_staging2;

update layoffs_staging2
set company =trim(company);

select * from layoffs_staging2 where industry like 'crypto%';

update layoffs_staging2 
set industry = 'Crypto'
where industry like 'crypto%';

select distinct(location)
from layoffs_staging2
order by 1;

select * from layoffs_staging2 where location like 'da%ss%';

update layoffs_staging2 
set location = 'Dusseldorf'
where location like 'da%ss%';

select * from layoffs_staging2 where location like 'flo%polis';

update layoffs_staging2 
set location = 'Florianpolis'
where location like 'flo%polis';

select * from layoffs_staging2 where location like 'Malm%';

update layoffs_staging2 
set location = 'Malmo'
where location like 'MalmÃ%';

select distinct(location)
from layoffs_staging2
order by 1;

select distinct(country) from layoffs_staging2 order by 1;

update layoffs_staging2 
set country = 'United States'
where country like 'United%.';

select `date`
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`,'%m/%d/%Y');

alter table layoffs_staging2
modify column `date` date;

-- 3. Clean Null values and empty values

select *
from layoffs_staging2
where industry is null or industry = '';

select *
from layoffs_staging2
where company = 'Airbnb';

select *
from layoffs_staging2 t1
join layoffs_staging2 t2 on t1.company = t2.company
where (t1.industry is null or t1.industry='') and t2.industry is not null ;

update layoffs_staging2
set industry = null 
where industry ='';

update layoffs_staging2 t1
join layoffs_staging2 t2 on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null and t2.industry is not null ;

select * 
from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;

delete
from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;

alter table layoffs_staging2
drop column row_num;

-- 4. Remove any columns

select *
from layoffs_staging2;