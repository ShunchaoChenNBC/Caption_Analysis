/* Base table that pull all start and end points for users turning on subtitles*/

CREATE OR REPLACE TABLE `nbcu-ds-sandbox-a-001.jf_sandbox.Subtitle_Analysis_Clickstream_Start_Points` AS


/*First: pull adobe_id and timestamp of every subtitle event */
WITH events AS (
SELECT 
  post_evar56 AS adobe_id,
  post_evar83 AS subtitle,
  sdpBusinessDate
FROM 
  `nbcu-sdp-prod-003.sdp_persistent_views.AdobeAnalyticsClickstreamView` click
WHERE
  post_evar83 is not null
AND 
  REGEXP_CONTAINS(post_evar83, 'enabled|disabled')
AND
  --update date
  date(sdpBusinessDate) BETWEEN '2022-10-01' AND '2022-12-31'
),

/*This pulls the next subtitle event for each event */
next_event AS (
SELECT
  base.*,
  lead(subtitle) over (partition by adobe_id order by sdpBusinessDate) as next_event,
  lead(sdpBusinessDate) over (partition by adobe_id order by sdpBusinessDate) as next_event_time
FROM
  `nbcu-ds-sandbox-a-001.jf_sandbox.Subtitle_Analysis_Clickstream_Base` base
)

/*This narrows down tHe event pairs that start with spanish subtitles on and ends with disabled, another language, or null */
SELECT
  *
FROM
  `nbcu-ds-sandbox-a-001.jf_sandbox.Subtitle_Analysis_Clickstream_Intermed`
WHERE
  --update language, if needed
  regexp_contains(subtitle, '^spanish|^es|^spa|^Spanish|^SPA')
AND 
  subtitle LIKE '%enabled%'
AND 
  ((NOT regexp_contains(next_event, '^spanish|^es|^spa|^Spanish|^SPA') 
    OR next_event LIKE '%disabled%')
    OR next_event IS NULL)


-----Queries take a long time to run, so if needed create a new table and run them sepaerately



/* This gets video usage and title info for all periods where subtitles are enabled */

WITH usage AS (
SELECT events.*,
  adobe_date,
  adobe_timestamp,
  session_id,
  display_name,
  video_id,
  meta.TitleName AS display_name_correct,
  meta.ProductType AS content_type,
  meta.TypeOfContent AS content_type_details,
  meta.Secondary_Genre AS genre,
  num_seconds_played_no_ads
FROM `nbcu-ds-sandbox-a-001.jf_sandbox.Subtitle_Analysis_Clickstream_Start_Points` events
LEFT OUTER JOIN 
  `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_VIDEO` video
  ON events.adobe_id = video.adobe_tracking_id 
  AND video.adobe_timestamp BETWEEN DATETIME(events.sdpBusinessDate,"America/New_York") AND IFNULL    (DATETIME(events.next_event_time,"America/New_York"), DATETIME('2022-12-31', "America/New_York"))
LEFT JOIN `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_COMPASS_METADATA_ALL` meta
ON video.video_id = meta.ContentID
WHERE adobe_date BETWEEN "2022-10-01" AND '2022-12-31' --update date
AND num_seconds_played_no_ads > 0
AND display_name NOT LIKE '%trailer%'
)


/* Joins to silver_user to get user data */

SELECT *
FROM `nbcu-ds-sandbox-a-001.jf_sandbox.Subtitle_Analysis_Clickstream_In_Between_Usage` usage
LEFT JOIN `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER` users
  ON usage.adobe_id = users.adobe_tracking_id
  AND usage.adobe_date = users.report_date
WHERE users.report_date BETWEEN '2022-10-01' AND '2022-12-31' --update date

