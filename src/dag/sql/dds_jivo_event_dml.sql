INSERT INTO dds.jivo_event
  SELECT
    webhook_id
    , widget_id
    , event_name
    , `datetime` AS event_dt
    , JSONExtractString(event, 'chat_id') AS chat_id
    , JSONExtractString(event, 'topic_id') AS topic_id
    , JSONExtractString(event, 'client_id') AS visitor_id
    , JSONExtractString(JSONExtractString(event, 'visitor'), 'name') AS visitor_name
    , JSONExtractUInt(JSONExtractString(event, 'visitor'), 'number') AS visitor_number
    , JSONExtractString(JSONExtractString(event, 'visitor'), 'description') AS visitor_description
    , JSONExtractString(JSONExtractString(event, 'visitor'), 'email') AS visitor_email
    , JSONExtractString(JSONExtractString(event, 'visitor'), 'phone') AS visitor_phone
    , JSONExtractString(JSONExtractString(event, 'visitor'), 'plain_messages') AS visitor_plain_message
    , JSONExtractString(JSONExtractString(event, 'visitor'), 'html_messages') AS visitor_html_message
    , JSONExtractUInt(JSONExtractString(event, 'visitor'), 'chats_count') AS visitor_chat_total
    , JSONExtract(COALESCE(JSONExtractString(JSONExtractString(JSONExtractString(event, 'visitor'), 'social'), 'social_profiles'), ''), 'Array(Tuple(String, String))').1 AS visitor_social_profile_name
    , JSONExtract(COALESCE(JSONExtractString(JSONExtractString(JSONExtractString(event, 'visitor'), 'social'), 'social_profiles'), ''), 'Array(Tuple(String, String))').2 AS visitor_social_profile_link
    , JSONExtract(COALESCE(JSONExtractString(JSONExtractString(JSONExtractString(event, 'visitor'), 'social'), 'photos'), ''), 'Array(Tuple(String, String))').1 AS visitor_social_profile_icon_name
    , JSONExtract(COALESCE(JSONExtractString(JSONExtractString(JSONExtractString(event, 'visitor'), 'social'), 'photos'), ''), 'Array(Tuple(String, String))').2 AS visitor_social_profile_icon_link
    , JSONExtractString(JSONExtractString(event, 'assigned_agent'), 'id') AS assigned_agent_id
    , JSONExtractString(JSONExtractString(event, 'visitor'), 'email') AS assigned_agent_email
    , JSONExtractString(JSONExtractString(event, 'visitor'), 'name') AS assigned_agent_name
    , JSONExtractString(JSONExtractString(event, 'agent'), 'id') AS agent_id
    , JSONExtractString(JSONExtractString(event, 'agent'), 'email') AS agent_email
    , JSONExtractString(JSONExtractString(event, 'agent'), 'name') AS agent_name
    , JSONExtract(COALESCE(JSONExtractString(event, 'agents'), ''), 'Array(Tuple(String, String, String))').1 AS agents_id
    , JSONExtract(COALESCE(JSONExtractString(event, 'agents'), ''), 'Array(Tuple(String, String, String))').2 AS agents_email
    , JSONExtract(COALESCE(JSONExtractString(event, 'agents'), ''), 'Array(Tuple(String, String, String))').3 AS agents_name
    , JSONExtractString(JSONExtractString(JSONExtractString(event, 'session'), 'geoip'), 'country_code') AS geoip_country_code
    , JSONExtractString(JSONExtractString(JSONExtractString(event, 'session'), 'geoip'), 'region_code') AS geoip_region_code
    , JSONExtractString(JSONExtractString(JSONExtractString(event, 'session'), 'geoip'), 'country') AS geoip_country
    , JSONExtractString(JSONExtractString(JSONExtractString(event, 'session'), 'geoip'), 'region') AS geoip_region
    , JSONExtractString(JSONExtractString(JSONExtractString(event, 'session'), 'geoip'), 'city') AS geoip_city
    , JSONExtractString(JSONExtractString(JSONExtractString(event, 'session'), 'geoip'), 'isp') AS geoip_isp
    , JSONExtractString(JSONExtractString(JSONExtractString(event, 'session'), 'geoip'), 'latitude') AS geoip_latitude
    , JSONExtractString(JSONExtractString(JSONExtractString(event, 'session'), 'geoip'), 'longitude') AS geoip_longitude
    , JSONExtractString(JSONExtractString(JSONExtractString(event, 'session'), 'geoip'), 'organization') AS geoip_organization
    , JSONExtractString(JSONExtractString(event, 'session'), 'utm') AS session_utm
    , JSONExtractString(JSONExtractString(JSONExtractString(event, 'session'), 'utm_json'), 'campaign') AS session_utm_campaign
    , JSONExtractString(JSONExtractString(JSONExtractString(event, 'session'), 'utm_json'), 'source') AS session_utm_source
    , JSONExtractString(JSONExtractString(JSONExtractString(event, 'session'), 'utm_json'), 'medium') AS session_utm_medium
    , JSONExtractString(JSONExtractString(JSONExtractString(event, 'session'), 'utm_json'), 'term') AS session_utm_term
    , JSONExtractString(JSONExtractString(JSONExtractString(event, 'session'), 'utm_json'), 'keyword') AS session_utm_keyword
    , JSONExtractString(JSONExtractString(JSONExtractString(event, 'session'), 'utm_json'), 'content') AS session_utm_content
    , JSONExtractString(JSONExtractString(event, 'session'), 'ip_addr') AS ip_addr
    , JSONExtractString(JSONExtractString(event, 'session'), 'user_agent') AS user_agent
    , JSONExtractString(JSONExtractString(event, 'page'), 'url') AS page_url
    , JSONExtractString(JSONExtractString(event, 'page'), 'title') AS page_title
    , JSONExtractString(JSONExtractString(event, 'analytics'), 'ga') AS analytics_ga
    , JSONExtractString(JSONExtractString(event, 'analytics'), 'ym') AS analytics_ym
    , JSONExtractBool(JSONExtractString(event, 'chat'), 'blacklisted') AS chat_blacklisted
    , JSONExtractString(JSONExtractString(event, 'chat'), 'invitation') AS chat_invitation_text
    , JSONExtractString(JSONExtractString(event, 'chat'), 'rate') AS chat_rate
    , JSONExtractString(event, 'message') AS offline_message_text
    , JSONExtractString(event, 'offline_message_id') AS offline_message_id
    , JSONExtract(COALESCE(JSONExtractString(JSONExtractString(event, 'chat'), 'messages'), ''), 'Array(Tuple(String, String, String, String))').1 AS chat_message_text
    , JSONExtract(COALESCE(JSONExtractString(JSONExtractString(event, 'chat'), 'messages'), ''), 'Array(Tuple(String, Int64, String, String))').2 AS chat_message_ts
    , JSONExtract(COALESCE(JSONExtractString(JSONExtractString(event, 'chat'), 'messages'), ''), 'Array(Tuple(String, String, String, String))').3 AS chat_message_role
    , JSONExtract(COALESCE(JSONExtractString(JSONExtractString(event, 'chat'), 'messages'), ''), 'Array(Tuple(String, String, String, String))').4 AS chat_message_agent_id
  FROM
    s3('https://storage.yandexcloud.net/analitics-airflow/services_data/jivo/*', '<ID>', '<Ключ>', 'JSONEachRow')
  WHERE
    toDate(event_dt) = toDate('{{ti.xcom_pull(key="yesterday_date", task_ids="get_jivo_data")}}')