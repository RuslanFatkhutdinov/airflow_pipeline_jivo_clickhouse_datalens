/* Схема для хранения витрины для дашборда */
-- CREATE DATABASE cdm;

/* Витрина для дашборда */
-- DROP TABLE cdm.jivo_dashboard;
CREATE TABLE cdm.jivo_dashboard (
  chat_id String NOT NULL                                             -- ID чата
  , visitor_number String                                             -- ID посетителя
  , topic_name String                                                 -- Тема оращения
  , agent_name String                                                 -- Имя агента
  , widget_name String NOT NULL                                       -- Название виджета
  , chat_rate String                                                  -- Оценка чата
  , chat_date Date NOT NULL                                           -- Дата чата
  , chat_duration_minute UInt32 NOT NULL                              -- Длительность чата в минутах
  , time_to_response_second UInt32                                    -- Время до первого ответа в секундах
  , chat_status String NOT NULL                                       -- Статус чата
  , offline_status Bool NOT NULL                                      -- Было ли оффлайн сообщение
  , working_time_status Bool NOT NULL                                 -- Было ли сообщение в рабочее время
)
  ENGINE = MergeTree()
  PARTITION BY toYYYYMM(chat_date)
  ORDER BY chat_id;