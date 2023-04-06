CREATE OR REPLACE TABLE `nbcu-ds-sandbox-a-001.Shunchao_Sandbox.Subtitle_Spanish_Clickstream_Start_Points` AS


/*First: pull adobe_id and timestamp of every subtitle event */
WITH events AS (
SELECT 
  post_evar56 AS aid,
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
  DATETIME(sdpBusinessDate,"America/New_York") BETWEEN '2022-11-01' AND '2023-03-31'
),

/*This pulls the next subtitle event for each event */
next_events AS (
SELECT
  events.*,
  lead(subtitle) over (partition by aid order by sdpBusinessDate) as next_event,
  lead(sdpBusinessDate) over (partition by aid order by sdpBusinessDate) as next_event_time
FROM events
)

/*This narrows down tHe event pairs that start with English subtitles on and ends with disabled, another language, or null */
SELECT
  *
FROM
  next_events
WHERE
  --update language, if needed
  regexp_contains(subtitle, '^spanish|^es|^spa|^Spanish|^SPA')
AND 
  subtitle LIKE '%enabled%'
AND 
  ((NOT regexp_contains(next_event, '^spanish|^es|^spa|^Spanish|^SPA') 
    OR next_event LIKE '%disabled%')
    OR next_event IS NULL)

