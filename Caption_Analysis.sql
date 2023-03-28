
create or replace table `nbcu-ds-sandbox-a-001.Shunchao_Sandbox_Final.English_Caption_Table_Feb` as  

with cte AS
(select
DATETIME(timestamp(sdpBusinessDate), "America/New_York") as sdpBusinessDate,
 adobe_id,
 subtitle,
 SPLIT(subtitle, '|')[SAFE_OFFSET(0)] as Subtitles -- Caption_Extraction
FROM `nbcu-ds-sandbox-a-001.jf_sandbox.Subtitle_Analysis_Clickstream_Base` 
where date(sdpBusinessDate) between '2023-02-01' and "2023-02-28"),

Captions as (select 
cte.*,
extract(YEAR from sdpBusinessDate) as Year,
extract(MONTH from sdpBusinessDate) as Month,
case when REGEXP_CONTAINS(LOWER(Subtitles),r'(^en|eng|english)') then "English"
when REGEXP_CONTAINS(LOWER(Subtitles), r'(zh|zh-hant|zh-hans|mandarin)') then "Chinese"
when REGEXP_CONTAINS(LOWER(Subtitles), r'(spa|Spanish|es)') then "Spanish"
when REGEXP_CONTAINS(LOWER(Subtitles), r'(cc|cc1|cc3|closed captions)') then 'Closed_Caption'
end as Caption
from cte
where lower(subtitle) like "%enabled" ),

sv as (
select adobe_date,
adobe_timestamp,
adobe_tracking_id,
display_name,
num_seconds_played_no_ads
from `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_VIDEO`
where adobe_date between '2023-02-01' and "2023-02-28"
)


select C.*,
sv.display_name,
sv.num_seconds_played_no_ads
from Captions C
left join sv on timestamp(C.sdpBusinessDate) = timestamp(sv.adobe_timestamp) and C.adobe_id = sv.adobe_tracking_id




