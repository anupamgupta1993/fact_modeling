/*
Original Data : 
game_id   | team_id   | team_abbreviation | team_city    | player_id | player_name   | nickname | start_position | comment                   | min    | fgm  | fga  | fg_pct | fg3m | fg3a | fg3_pct | ftm  | fta  | ft_pct | oreb | dreb | reb | ast | stl | blk | to  | pf  | pts | plus_minus
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
21600824  | 1610612749 | MIL               | Milwaukee    | 201150    | Spencer Hawes | null     | null           | DNP - Coach's Decision    | null   | null | null | null   | null | null | null    | null | null | null   | null | null | null| null| null| null| null| null| null| null
20900108  | 1610612758 | SAC               | Sacramento   | 201150    | Spencer Hawes | null     | C              | null                     | 31:59  | 5    | 10   | 0.5    | 0    | 3    | 0       | 2    | 2    | 1      | 2    | 6    | 8   | 2   | 0   | 4   | 1   | 4   | 12  | -5
21600529  | 1610612766 | CHA               | Charlotte    | 201150    | Spencer Hawes | null     | null           | null                     | 23:47  | 2    | 6    | 0.333  | 2    | 3    | 0.667   | 2    | 2    | 1      | 0    | 4    | 4   | 2   | 2   | 2   | 1   | 4   | 8   | 10

*/

-- De-duplication of data 

WITH indexed
     AS (SELECT *,
                Row_number()
                  OVER (
                    partition BY game_id, team_id, player_id) AS rn
         FROM   bootcamp.nba_game_details),
     de_duped
     AS (SELECT game_id,
                team_id,
                team_abbreviation,
                team_city,
                player_id,
                player_name,
                nickname,
                start_position,
                comment,
                min,
                fgm,
                fga,
                fg_pct,
                fg3m,
                fg3a,
                fg3_pct,
                ftm,
                fta,
                ft_pct,
                oreb,
                dreb,
                reb,
                ast,
                stl,
                blk,
                to,
                pf,
                pts,
                plus_minus
         FROM   indexed
         WHERE  rn = 1)
SELECT Count(*)
FROM   de_duped 

-- User Devices Activity Datelist DDL 

CREATE TABLE agupta93.user_devices_cumulated
             (
                          user_id      BIGINT,
                          browser_type VARCHAR,
                          dates_active ARRAY(date),
                          date date
             )
             WITH
             (
                          format = 'PARQUET',
                          partitioning = array['date']
             )

-- Populating the above table

WITH
yesterday
AS
  (
         SELECT *
         FROM   agupta93.user_devices_cumulated
         WHERE  date = date('2023-01-06') ),
  today
AS
  (
           SELECT   we.user_id,
                    cast(date_trunc('day', we.event_time) AS date) AS event_date,
                    d.browser_type,
                    count(*)
           FROM     bootcamp.web_events we
           JOIN     bootcamp.devices d
           ON       we.device_id = d.device_id
           WHERE    date_trunc('day', event_time) = date('2023-01-07')
           GROUP BY we.user_id,
                    cast(date_trunc('day', we.event_time) AS date),
                    d.browser_type )
  SELECT          coalesce(y.user_id, t.user_id)           AS user_id,
                  coalesce(y.browser_type, t.browser_type) AS browser_type,
                  CASE
                                  WHEN y.dates_active IS NOT NULL THEN array[t.event_date]
                                                                  || y.dates_active
                                  ELSE array[t.event_date]
                  end                AS dates_active,
                  date('2023-01-07') AS date
  FROM            yesterday y
  full OUTER JOIN today t
  ON              y.user_id = t.user_id
  AND             y.browser_type = t.browser_type

/*
Output of the above query :

user_id       | browser_type | dates_active                                                                 | date
-----------------------------------------------------------------------------------------------------------------------------------
-1116803864   | Googlebot     | ["2023-01-07", null, "2023-01-05", null, null, "2023-01-02", "2023-01-01"]    | 2023-01-07
-1619560135   | Googlebot     | ["2023-01-07", "2023-01-06"]                                                 | 2023-01-07
-1818138431   | FacebookBot   | ["2023-01-07"]                                                               | 2023-01-07
-932504726    | Safari        | ["2023-01-07"]                                                               | 2023-01-07
495022226     | Googlebot     | ["2023-01-07", null, "2023-01-05", null, "2023-01-03", "2023-01-02"]          | 2023-01-07
-1230449857   | Linespider    | ["2023-01-07"]                                                               | 2023-01-07
2105693509    | AhrefsBot     | ["2023-01-07"]                                                               | 2023-01-07
1968028459    | Googlebot     | ["2023-01-07", null, "2023-01-05", "2023-01-04", "2023-01-03"]                | 2023-01-07

*/
-- 

WITH
today
AS
  (
         SELECT *
         FROM   agupta93.user_devices_cumulated
         WHERE  date = date('2023-01-07') ),
  date_list_int
AS
  (
             SELECT     user_id,
                        browser_type,
                        cast( sum(
                        CASE
                                   WHEN CONTAINS(dates_active, sequence_date) THEN pow(2, 30 - date_diff('day', sequence_date, date))
                                   ELSE 0
                        end ) AS BIGINT ) AS history_int
             FROM       today
             CROSS JOIN unnest (sequence(date('2023-01-01'), date('2023-01-07'))) AS t (sequence_date)
             GROUP BY   user_id,
                        browser_type )
  SELECT *,
         to_base(history_int, 2)    AS history_in_binary,
         bit_count(history_int, 32) AS num_days_active
  FROM   date_list_int query 5 :
  

/*
Output of the above table :

user_id       | browser_type   | history_int | history_in_binary                  | num_days_active
------------------------------------------------------------------------------------------------------
517501749     | Googlebot      | 536870912   | 100000000000000000000000000000      | 1
443044601     | Googlebot      | 805306368   | 110000000000000000000000000000      | 2
1536730180    | Chrome Mobile  | 536870912   | 100000000000000000000000000000      | 1
-1930074104   | Chrome         | 536870912   | 100000000000000000000000000000      | 1
2103117747    | Other          | 536870912   | 100000000000000000000000000000      | 1
1260731553    | Chrome         | 603979776   | 100100000000000000000000000000      | 2
142043613     | IE             | 268435456   | 10000000000000000000000000000       | 1
1556683089    | Firefox        | 268435456   | 10000000000000000000000000000       | 1
573743021     | bingbot        | 268435456   | 10000000000000000000000000000       | 1
-579685573    | PetalBot       | 67108864    | 100000000000000000000000000         | 1


From here we could use the bitmast generated to get things like active last 3 days 

*/

-- get last 3 days active

SELECT *,
       To_base(history_int, 2)                                      AS
       history_in_binary,
       To_base(From_base('11111110000000000000000000000000', 2), 2) AS
       weekly_base,
       Bit_count(history_int, 32)                                   AS
       num_days_active,
       Bit_count(Bitwise_and(history_int,
                 From_base('11100000000000000000000000000000',
                 2)), 32) > 0              AS is_active_last_three_days
FROM   date_list_int 

/*
Output : 

user_id       | browser_type   | history_int | history_in_binary                  | weekly_base                  | num_days_active | is_active_last_three_days
-----------------------------------------------------------------------------------------------------------------------------------------------------------
-704806762    | Chrome         | 1073741824  | 1000000000000000000000000000000     | 11111110000000000000000000000000 | 1              | true
-352340254    | Googlebot      | 536870912   | 100000000000000000000000000000      | 11111110000000000000000000000000 | 1              | true
-297633340    | Chrome         | 402653184   | 11000000000000000000000000000       | 11111110000000000000000000000000 | 2              | false
1339032184    | Chrome         | 268435456   | 10000000000000000000000000000       | 11111110000000000000000000000000 | 1              | false
-1952919474   | Firefox        | 268435456   | 10000000000000000000000000000       | 11111110000000000000000000000000 | 1              | false
-640851146    | Chrome Mobile  | 268435456   | 10000000000000000000000000000       | 11111110000000000000000000000000 | 1              | false
1293676563    | Googlebot      | 167772160   | 1010000000000000000000000000        | 11111110000000000000000000000000 | 2              | false
-1936905104   | MJ12bot        | 201326592   | 1100000000000000000000000000        | 11111110000000000000000000000000 | 2              | false


*/
