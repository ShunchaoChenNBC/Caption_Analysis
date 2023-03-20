

with cte as (select 
 extract(YEAR from sdpBusinessDate) as Year,
 extract(MONTH from sdpBusinessDate) as Month,
 adobe_id,
 subtitle,
 SPLIT(subtitle, '|')[SAFE_OFFSET(0)] as Captions -- Caption_Extraction
FROM `nbcu-ds-sandbox-a-001.jf_sandbox.Subtitle_Analysis_Clickstream_Base` 
where extract(YEAR from sdpBusinessDate) between 2022 and 2023),

Captions as (select 
cte.*,
IF(REGEXP_CONTAINS(LOWER(Captions), r'(en|eng|english)'), "English", NULL) as English_Caption,
IF(REGEXP_CONTAINS(LOWER(Captions), r'(zh|zh-hant|zh-hans|mandarin)'), "Chinese", NULL) as Chinese_Caption,
IF(REGEXP_CONTAINS(LOWER(Captions), r'(spa|Spanish|es)'), "Spanish", NULL) as Spanish_Caption,
IF(REGEXP_CONTAINS(LOWER(Captions), r'(cc|cc1|cc3)'), "CLosed", NULL) as Closed_Caption -- Incl. Backgound sounds and speaker changes (for deaf and hard-of-hearing people)
from cte)

select Year,
Month,
count(distinct case when English_Caption = "English" and lower(subtitle) like "%enabled" then adobe_id end) as English_Accts,
count(distinct case when Chinese_Caption = "Chinese" and lower(subtitle) like "%enabled" then adobe_id end) as Chinese_Accts,
count(distinct case when Spanish_Caption = "Spanish" and lower(subtitle) like "%enabled" then adobe_id end) as Spanish_Accts,
count(distinct case when Closed_Caption = "Closed" and lower(subtitle) like "%enabled" then adobe_id end) as Closed_Accts,
count (distinct adobe_id) as All_Accts
from Captions 
group by 1, 2
order by 1, 2
