with Base AS
(
select
post_evar56 AS aid,
post_evar83 AS subtitle,
TIMESTAMP(datetime(sdpBusinessDate), "America/New_York") AS sdpBusinessDate,
from
`nbcu-sdp-prod-003.sdp_persistent_views.AdobeAnalyticsClickstreamView` click
where
post_evar83 is not null
and
REGEXP_CONTAINS(post_evar83, 'enabled|disabled')
and
date(sdpBusinessDate) = '2023-01-01'),

Intermed as (
select
base.*,
lead(subtitle) over (partition by aid order by
sdpBusinessDate) as next_event,
lead(sdpBusinessDate) over (partition by aid order by
sdpBusinessDate) as next_event_time
from
Base
),

Start_Point as (
select *
from Intermed
where REGEXP_CONTAINS(LOWER(subtitle), r'(^en|eng|english)') and lower(subtitle) like "%enabled%"
and (not REGEXP_CONTAINS(LOWER(subtitle), r'(^en|eng|english)') or next_event LIKE '%disabled%')
or next_event is null
),  -- Allow the next event to be null, which a user prepetually has subtitles enabled for content

Between_Usage as (
SELECT events.aid,
events.subtitle,
events.next_event,
events.sdpBusinessDate AS subtitle_event,
events.next_event_time,
CONCAT(video.post_visid_high, video.post_visid_low, visit_num,
visit_start_time_gmt) AS session_id,
video.post_evar122 AS video_id,
video.post_evar72 AS display_name,
TIMESTAMP(datetime(video.post_cust_hit_time_gmt),"America/New_York") AS
adobe_timestamp,
DATE(post_cust_hit_time_gmt, "America/New_York") AS
adobe_date,
CASE WHEN(post_event_list LIKE "%,20319=%") then
SAFE_CAST(REGEXP_EXTRACT(SUBSTR(post_event_list,
STRPOS(post_event_list, ',20319=')+7), r"^[0-9_.+-]+") AS
float64) ELSE 0 END AS num_seconds_played_no_ads,
CASE WHEN (post_event_list LIKE "%,20311%") THEN 1 ELSE 0 END
AS num_views_started
FROM Start_Point as events
JOIN `nbcu-sdp-prod-003.sdp_persistent_views.AdobeVideoAnalyticsView` video
ON events.aid = video.post_evar56 and TIMESTAMP(datetime(video.post_cust_hit_time_gmt),"America/New_York") BETWEEN events.sdpBusinessDate AND ifnull(timestamp(events.next_event_time), timestamp('2025-11-22',"UTC"))
)

select usage.*,
meta.TitleName AS display_name_correct
from Between_Usage as usage
LEFT JOIN `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_COMPASS_METADATA_ALL` meta ON usage.video_id = meta.ContentID





