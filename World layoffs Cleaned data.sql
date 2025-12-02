SELECT * 
FROM world_layoffs.layoffs;

create table layoffs_stagging 
like layoffs;

select * 
from layoffs_stagging;

insert layoffs_stagging 
select * from layoffs;

select * from layoffs_stagging;
WITH duplicate_cte AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY company,location, industry, total_laid_off,
                            percentage_laid_off, `date`,stage,country,funds_raised_millions
           ) AS row_num
    FROM layoffs_stagging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

select * from layoffs_stagging where company='casper'; 	

CREATE TABLE `layoffs_stagging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_million` int DEFAULT NULL,
  `row_num` int 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoffs_stagging2;
insert into layoffs_stagging2 
SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY company,location, industry, total_laid_off,
                            percentage_laid_off, `date`,stage,country,funds_raised_millions
           ) AS row_num
    FROM layoffs_stagging;

select * 
from layoffs_stagging2 
where row_num>1;
SET SQL_SAFE_UPDATES = 0;

delete from layoffs_stagging2 
where row_num>1;

select company,trim(company) 
from layoffs_stagging2;
 
update layoffs_stagging2 
set company =trim(company);

select distinct industry 
from layoffs_stagging2 
order by 1;

select * 
from layoffs_stagging2 
where industry like 'crypto%';

update layoffs_stagging2 
set industry='crypto' 
where industry like 'crypto%';

select distinct industry 
from layoffs_stagging2;

select distinct country 
from layoffs_stagging2 
order by 1;

select distinct country,trim(trailing  '.' from country) 
from layoffs_stagging2 order by 1;

update layoffs_stagging2 
set country=trim(trailing  '.' from country) 
where country like 'United States%';

select `date`  from layoffs_stagging2;

update layoffs_stagging2 
set `date`=str_to_date(`date`,'%m/%d/%Y');

alter table layoffs_stagging2 
modify column `date` date;

select * from  layoffs_stagging2 
where total_laid_off is null 
and percentage_laid_off is null;

select * 
from  layoffs_stagging2 
where industry is null 
or industry ='';

select * 
from  layoffs_stagging2 
where company='Airbnb'; 

update layoffs_stagging2 
set industry=Null where industry='';

select t1.industry,t2.industry 
from layoffs_stagging2 t1 
join layoffs_stagging2 t2 on t1.company=t2.company 
where (t1.industry is null or t1.industry='')
and t2.industry is not null;	

UPDATE layoffs_stagging2 t1
JOIN layoffs_stagging2 t2
    ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
  AND t2.industry IS NOT NULL;
 
 
 select * from  layoffs_stagging2 
 where company like 'Bally%'; 
 
 select * from  layoffs_stagging2 ;


select * 
from layoffs_stagging2 
where  total_laid_off is null
 and percentage_laid_off is null;
 
 delete 
 from layoffs_stagging2 
 where total_laid_off is null
 and percentage_laid_off is null;
 
 select* 
 from layoffs_stagging2; 
 
 alter table layoffs_stagging2 drop column row_num;