## DATA CLEANING PROJECT

select * from layoffs;

## what needs to be done for cleaned data 
## 1. remove duplicates
## 2. 	standardize the data 
## 3. deal with the null and blank values 
## 4. remove unncessary columns or rows (removing columns from the raw data sets in  big data can be a problem we have to be sure that we know what were doing)


## lets create a copy table so we wont directly affect the raw table

create table layoffs_staging
like layoffs; 

select * from layoffs_staging; 

## now lets populate the copy table

insert layoffs_staging
select * from layoffs;


## lets work on removing duplicates
## window function to get an idea

select *,
 row_number() over
 (partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_cnt
from layoffs_staging;

## lets use cte to filter row counts over 1

with CTE_duplicates as
( select *,
 row_number() over
 (partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_cnt
from layoffs_staging)
SELECT * from CTE_duplicates
where row_cnt > 1;


select * from layoffs_staging 
where company = 'Hibob';


## now that the duplicates are known we wanna get rid of them\
## in other paltforms like postgres or microsoft sql server we could have passed a clause in the main cte itself that wouldve potentially gotten rid of 
##the duplicates but mysql is a bit comlicated than that



CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL
  , `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


select * from layoffs_staging2;


insert into layoffs_staging2
select *,
 row_number() over
 (partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_cnt
from layoffs_staging;



select * from layoffs_staging2
where row_num > 1;


delete  from layoffs_staging2
where row_num > 1;

select * from layoffs_staging2
where row_num > 1;
## now the duplicates are gone 


##lets focus on standardizeing data


update layoffs_staging2 set 
company = trim(company);



select * from layoffs_staging2;

select distinct industry 
from layoffs_staging2
order by industry;


select * from layoffs_staging2
where industry like 'crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'crypto%';

select distinct industry 
from layoffs_staging2
order by industry;

select distinct country
from layoffs_staging2
where country like 'United States%';

update layoffs_staging2
set country = trim(trailing '.' from country )
where country like 'United States%';


## fixing date from text to datetime

select `date`, str_to_date(`date`, '%m/%d/%Y') from layoffs_staging2;


update layoffs_staging2 
set `date` = str_to_date(`date`, '%m/%d/%Y');

select `date` from layoffs_staging2;

alter table layoffs_staging2
modify column `date`  date; 






## dealing with nulls 



select * from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;


select distinct industry from layoffs_staging2;

select * from layoffs_staging2
where industry is null or industry = '';

select * from layoffs_staging2 
where company like 'Bally%';

select t1.industry, t2.industry from layoffs_staging2 t1
join layoffs_staging2 t2 
on t1.company=t2.company and t1.location=t2.location
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

## update the blnks to nulls first 
update layoffs_staging2
set industry = null
where industry = '';


## populate the nulls 

update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company=t2.company and t1.location=t2.location
set t1.industry= t2.industry
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;


select t1.industry, t2.industry from layoffs_staging2 t1
join layoffs_staging2 t2 
on t1.company=t2.company and t1.location=t2.location
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

select * from layoffs_staging2 
where company = 'Juul';

select * from layoffs_staging2 ;

## checking where both total and % laid offf is null cuz we cant really work with those 
select * from layoffs_staging2
where total_laid_off is null and
percentage_laid_off is null ;


# i believe as these nulls wont help us later in explotory data deleting them might be helpful

delete from layoffs_staging2
where total_laid_off is null and
percentage_laid_off is null ;

select * from layoffs_staging2;


# delete the column row_num cuz we really dont need it anymore


alter table layoffs_staging2
drop column row_num;

select * from layoffs_staging2;


## so now from the layoffs data we created a new copy table
##removed duplicates from it
## standardize the sytax and date
## dealt with nulls populated some deleted some 
##we deleted the row_num column that we created to deal with duplicates 



