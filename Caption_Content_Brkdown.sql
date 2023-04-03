# Content Breakdown

with cte as (SELECT 
DATE(timestamp(sdpBusinessDate), "America/New_York") AS Adobe_Date,
adobe_id,
case when REGEXP_CONTAINS(LOWER(subtitle),r'(^en|eng|english)') then "English"
when REGEXP_CONTAINS(LOWER(subtitle), r'(zh|zh-hant|zh-hans|mandarin)') then "Chinese"
when REGEXP_CONTAINS(LOWER(subtitle), r'(spa|Spanish|es)') then "Spanish"
when REGEXP_CONTAINS(LOWER(subtitle), r'(cc|cc1|cc3|closed captions)') then 'Closed_Caption'
end as Caption,
display_name,
num_seconds_played_no_ads
FROM 
(select *
from
`nbcu-ds-sandbox-a-001.jf_sandbox.Subtitle_Analysis_Final_Jan`
union all 
select *
from
`nbcu-ds-sandbox-a-001.jf_sandbox.Subtitle_Analysis_Final_Dec`
union all
select *
from
`nbcu-ds-sandbox-a-001.jf_sandbox.Subtitle_Analysis_Final_Nov`
))


select 
extract(YEAR from Adobe_Date) as Year,
extract(MONTH from Adobe_Date) as Month,
Caption,
display_name,
count(distinct adobe_id) as Accts,
round(sum(num_seconds_played_no_ads)/3600,2) as Watch_Hours
from cte
group by 1,2,3,4
order by 1,2,6 desc
