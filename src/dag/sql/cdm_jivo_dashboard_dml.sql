INSERT INTO cdm.jivo_dashboard
  WITH jivo_event_prep AS
    (SELECT
      chat_id
      , visitor_number
      , topic_id
      , arrayElement(agents_id, 1) AS agent_id
      , widget_id
      , chat_rate
      , MIN(toDate(chat_message_ts)) AS chat_date
      , MIN(toDateTime(chat_message_ts, 'Europe/Moscow')) AS chat_start_dt
      , MAX(toDateTime(chat_message_ts, 'Europe/Moscow')) AS chat_finish_dt
      , date_diff('minute', chat_start_dt, chat_finish_dt) AS chat_duration_minute
      , MIN(CASE
          WHEN chat_message_role = 'visitor' THEN toDateTime(chat_message_ts, 'Europe/Moscow')
        END) AS first_visitor_message_dt
      , MIN(CASE
          WHEN chat_message_role = 'agent' THEN toDateTime(chat_message_ts, 'Europe/Moscow')
        END) AS first_agent_message_dt
      , (CASE
          WHEN topic_id = '' AND agent_id != '' THEN 'Заявка не решена'
          WHEN agent_id = '' THEN 'Заявка на отвечена'
          ELSE 'Заявка решена'
        END) AS chat_status
      , (CASE
          WHEN first_visitor_message_dt < first_agent_message_dt THEN date_diff('second', first_visitor_message_dt, first_agent_message_dt)
          ELSE 0
        END) as time_to_response_second
    FROM
      dds.jivo_event
    ARRAY JOIN
      chat_message_ts
      , chat_message_role
    GROUP BY
      chat_id
      , visitor_number
      , topic_id
      , agent_id
      , widget_id
      , chat_rate
      , chat_status),

  offline_messages AS
    (SELECT
      chat_id
      , true AS offline_status
    FROM
      dds.jivo_event FINAL
    WHERE
      event_name = 'offline_message')

 SELECT
  t1.chat_id AS chat_id
  , t1.visitor_number AS visitor_number
  , t2.topic_name AS topic_name
  , t3.agent_name AS agent_name
  , t4.widget_name AS widget_name
  , t1.chat_rate AS chat_rate
  , t1.chat_date AS chat_date
  , t1.chat_duration_minute AS chat_duration_minute
  , t1.time_to_response_second AS time_to_response_second
  , t1.chat_status AS chat_status
  , t5.offline_status AS offline_status
  , (CASE
      WHEN toHour(t1.chat_start_dt) < 9 THEN false
      WHEN toHour(t1.chat_start_dt) >= 21 THEN false
      ELSE true
    END) AS working_time_status
FROM
  jivo_event_prep AS t1
LEFT JOIN
  dicts.jivo_topic_dict AS t2 ON t1.topic_id = t2.topic_id
LEFT JOIN
  dicts.jivo_agent_dict AS t3 ON t1.agent_id = t3.agent_id
LEFT JOIN
  dicts.jivo_widget_dict AS t4 ON t1.widget_id = t4.widget_id
LEFT JOIN
  offline_messages AS t5 ON t1.chat_id = t5.chat_id