/* Схема для таблицы c данныvb Jivo */
-- CREATE DATABASE dds;

/* Таблица с данными Jivo */
--DROP TABLE dds.jivo_event;
CREATE TABLE dds.jivo_event (
  webhook_id String NOT NULL                                          --  ID вебхука
  , widget_id String NOT NULL                                         --  ID виджета
  , event_name String NOT NULL                                        --  Название события
  , event_dt DateTime() NOT NULL                                      --  Дата и время события
  , chat_id String                                                    --  ID чата
  , topic_id String                                                   --  ID темы
  , visitor_id String                                                 --  ID посетителя
  , visitor_name String                                               --  ФИО посетителя
  , visitor_number UInt32                                             --  ID посетителя
  , visitor_description String                                        --  Описание посетителя
  , visitor_email String                                              --  Email посетителя
  , visitor_phone String                                              --  Телефон посетителя
  , visitor_plain_message String                                      --  Текст ответа в письме
  , visitor_html_message String                                       --  Текст с HTML ответа в письме
  , visitor_chat_total UInt16                                         --  Количество чатов с посетителем
  , visitor_social_profile_name Array(String)                         --  Название социальной сети
  , visitor_social_profile_link Array(String)                         --  Ссылка на профиль в социальной сети
  , visitor_social_profile_icon_name Array(String)                    --  Название социальной сети
  , visitor_social_profile_icon_link Array(String)                    --  Ссылка на иконку социальной сети
  , assigned_agent_id String                                          --  ID ответственного
  , assigned_agent_email String                                       --  Email ответственного
  , assigned_agent_name String                                        --  Имя ответственного
  , agent_id String                                                   --  ID первого принявшего опрератора
  , agent_email String                                                --  Email первого принявшего опрератора
  , agent_name String                                                 --  Имя первого принявшего опрератора
  , agents_id Array(String)                                           --  ID операторов, учавствоваших разговоре
  , agents_email Array(String)                                        --  Email операторов, учавствоваших разговоре
  , agents_name Array(String)                                         --  Имена операторов, учавствоваших разговор
  , geoip_country_code String                                         --  Код страны
  , geoip_region_code String                                          --  Код региона
  , geoip_country String                                              --  Название страны
  , geoip_region String                                               --  Название региона
  , geoip_city String                                                 --  Название города
  , geoip_isp String                                                  --  Провайдер
  , geoip_latitude String                                             --  Широта
  , geoip_longitude String                                            --  Долгота
  , geoip_organization String                                         --  Юридическое лицо провайдера
  , session_utm String                                                --  Строка utm
  , session_utm_campaign String                                       --  utm_campaign
  , session_utm_source String                                         --  utm_source
  , session_utm_medium String                                         --  utm_medium
  , session_utm_term String                                           --  utm_term
  , session_utm_keyword String                                        --  utm_keyword
  , session_utm_content String                                        --  utm_content
  , ip_addr String                                                    --  IP адрес
  , user_agent String                                                 --  User-agent
  , page_url String                                                   --  URL страницы
  , page_title String                                                 --  Title страницы
  , analytics_ga String                                               --  Google Analytics Client ID
  , analytics_ym String                                               --  Яндекс Метрика Client ID
  , chat_blacklisted Bool                                             --  Помечен ли чат черным списком
  , chat_invitation_text String                                       --  Текст сообщения оператора
  , chat_rate String                                                  --  Оценка диалога
  , offline_message_text String                                       --  Текст оффлайн сообщения
  , offline_message_id String                                         --  ID оффлайн сообщения
  , chat_message_text Array(String)                                   --  Сообщения в переписке
  , chat_message_ts Array(UInt32)                                     --  Timestamp сообщений
  , chat_message_role Array(String)                                   --  Кто отправляет сообщение
  , chat_message_agent_id Array(String)                               --  ID агента в переписке
)
  ENGINE = ReplacingMergeTree
  PARTITION BY toYYYYMM(event_dt)
  ORDER BY webhook_id;
