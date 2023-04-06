
CREATE OR REPLACE TABLE `nbcu-ds-sandbox-a-001.Shunchao_Sandbox.Subtitle_Analysis_Clickstream_Intermed` AS (
select
base.*,
lead(subtitle) over (partition by aid order by
sdpBusinessDate) as next_event,
lead(sdpBusinessDate) over (partition by aid order by
sdpBusinessDate) as next_event_time
from
`nbcu-ds-sandbox-a-001.Shunchao_Sandbox.Subtitle_Analysis_Clickstream_Base_alt` base
)

 CREATE OR REPLACE TABLE `nbcu-ds-sandbox-a-001.Shunchao_Sandbox.Subtitle_Analysis_Clickstream_Base_alt` AS
 
select
post_evar56 AS aid,
post_evar83 AS subtitle,
sdpBusinessDate
from
`nbcu-sdp-prod-003.sdp_persistent_views.AdobeAnalyticsClickstreamView` click
where post_evar83 is not null and REGEXP_CONTAINS(post_evar83, 'enabled|disabled') and date(sdpBusinessDate) > '2022-10-31'
