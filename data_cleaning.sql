 -- data cleaning


select * from layoffs_data;
-- Remove dulpicate data
-- Standardize
-- Null values or blank
-- Remove any columns

create table layoffs_staging
like layoffs_data;

select * from layoffs_staging;

insert layoffs_staging
select * from layoffs_data;



select * ,
row_number() over( 
partition by Company,Industry,Laid_Off_Count,Percentage, 'Date') 
as row_num from layoffs_staging;

with duplicate_cte as(
select *,
row_number() over( 
partition by Company,Location_HQ,Industry,Laid_Off_Count,Funds_Raised,Stage,Percentage, 'Date') as row_num 
from layoffs_staging
)
select * from duplicate_cte 
where row_num >1;

select * from layoffs_staging where Company='Amazon' ;

drop table layoffs_staging2;
create table layoffs_staging2(
Company text, 
Location_HQ text, 
Industry text ,
Laid_Off_Count int default null, 
Date text ,
Source text ,
Funds_Raised double,
Stage text,
Date_Added text,
Country text,
Percentage text, 
List_of_Employees_Laid_Off text,
row_num int)
Engine=InnoDB default charset =utf8mb4 collate=utf8mb4_0900_ai_ci;

select * from layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT 
    Company, Location_HQ, Industry, 
    NULLIF(Laid_Off_Count, '') AS Laid_Off_Count,
    Date, Source, Funds_Raised, Stage, Date_Added, Country, 
    Percentage, List_of_Employees_Laid_Off,
    ROW_NUMBER() OVER (
        PARTITION BY Company, Location_HQ, Industry, 
                     NULLIF(Laid_Off_Count, ''), Date, Source, 
                     Funds_Raised, Stage, Date_Added, Country, Percentage, 
                     List_of_Employees_Laid_Off
    ) AS row_num
FROM layoffs_staging;

select * from layoffs_staging2 layoffs_staging2 where row_num >1;

SET SQL_SAFE_UPDATES = 0;
DELETE FROM layoffs_staging2 WHERE row_num > 1;
SET SQL_SAFE_UPDATES = 1;

select * from layoffs_staging2;

-- Standardize
select distinct(Company) from layoffs_staging2;
update layoffs_staging2 set Company = 'Open' where Company like '%Open';
update layoffs_staging2 set Company = 'Paid' where Company like '%Paid';
select trim(Company) from layoffs_staging2;
select distinct(Location_HQ) from layoffs_staging2 order by 1;
update layoffs_staging2 set Location_HQ = 'Malmo' where Location_HQ like 'Malm%';
update layoffs_staging2 set Location_HQ = 'Ferdericton' where Location_HQ like 'FÃ¸rde%';


SET SQL_SAFE_UPDATES = 0;

update layoffs_staging2
set Company = trim(Company);

select distinct(Industry) from layoffs_staging2 order by 1;

select distinct(Country) from layoffs_staging2 order by 1;
select distinct(Date) from layoffs_staging2 order by 1;

select 'Date',
str_to_date('Date','%m/%d/%y') from layoffs_staging2;

select * from  layoffs_staging2 where Laid_Off_Count is null and Percentage = '' ;
delete  from  layoffs_staging2 where Laid_Off_Count is null and Percentage = '' ;

select * from  layoffs_staging2 where Industry is null or Industry = '' ;

select * from  layoffs_staging2;
alter table layoffs_staging2 drop column row_num;


