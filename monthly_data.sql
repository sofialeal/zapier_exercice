create view sleal.monthly_data as

with user_active_periods as (
     select user_id,
            case when sum(sum_tasks_used)>=1 then date end as first_active_day, --day with tasks
            case when sum(sum_tasks_used)>=1 then dateadd(day,27,date) end as last_active_day --after 27 days
    from source_data.tasks_used_da
    group by user_id, date
),

--condense time periods for active periods, per user
active_period as(
select user_id, first_active_day as ts, +1 as type, 1 as sub
  from user_active_periods
union all
select user_id, dateadd(day, +1, last_active_day) as ts, -1 as type, 0 as sub
  from user_active_periods),

partition_active as(
select active_period.*
     , sum(type) over(partition by user_id order by ts, type desc
                      rows between unbounded preceding and current row) - sub as cnt
  from active_period),

groupnum as(
select user_id, ts, floor((row_number() over(partition by user_id order by ts) - 1) / 2 + 1) as grpnum
  from partition_active
  where cnt = 0),

--condensed user active periods
user_active_periods_c as (
select user_id, min(ts) as first_active_day, dateadd(day, -1, max(ts)) as last_active_day
  from groupnum
  group by user_id, grpnum
  having first_active_day is not null and last_active_day is not null
),

--find churn periods
user_churn_periods as (
    select
        user_id,
        dateadd(day, +1, last_active_day) as first_churn_day,
        dateadd(day, +28, last_active_day) as last_churn_day
    from user_active_periods_c
),

--data ranges where it's possible to be churn (no activity)
possible_to_be_churn as(
  select user_id,
    max(last_active_day) over(partition by user_id order by first_active_day rows between unbounded preceding and current row ) + 1 start_gap,
    lead(first_active_day) over(partition by user_id order by first_active_day ) - 1 end_gap
  from user_active_periods_c
    )
,

--find overlap
user_churn_periods_2 as (
select c.user_id,
      case when (first_churn_day<= start_gap and last_churn_day>= end_gap) then start_gap
        when (first_churn_day>= start_gap and last_churn_day<= end_gap) then first_churn_day
        when (first_churn_day<= start_gap and last_churn_day<= end_gap) then start_gap
        when (first_churn_day>= start_gap and last_churn_day>= end_gap) then first_churn_day end as first_churn_day,
      case when (first_churn_day<= start_gap and last_churn_day>= end_gap) then end_gap
        when (first_churn_day>= start_gap and last_churn_day<= end_gap) then last_churn_day
        when (first_churn_day<= start_gap and last_churn_day<= end_gap) then last_churn_day
        when (first_churn_day>= start_gap and last_churn_day>= end_gap) then end_gap end as last_churn_day

from user_churn_periods c
    join possible_to_be_churn g on g.user_id=c.user_id and start_gap <= end_gap
),

--add final churn period (after last activity)
user_churn_periods_c as (
    select
        user_id,
        first_churn_day,
        last_churn_day
        from user_churn_periods_2
        where first_churn_day<=last_churn_day
    union all
    select
        user_id,
        dateadd(day,+1,max(last_active_day)) as first_churn_day,
        dateadd(day,+28,max(last_active_day)) as last_churn_day
           from user_active_periods_c
       group by user_id

),

--generate time series, to get full dates between 2017-01-01 (min date available) and 2017-06-01 (max date with activity)
timeseries as (
    SELECT ('2016-12-31'::date + row_number() OVER (ORDER BY true))::date AS date
    from  source_data.tasks_used_da
    limit 152
),

--get full timeseries with user status per day
user_status_full_ts as (
    select distinct ts.date,
                    t.user_id,
                    case
                        when sum(case
                                     when ts.date between churn.first_churn_day and churn.last_churn_day then 1
                                     else 0 end) >= 1 then 1
                        else 0 end as ischurn,
                    case
                        when sum(case
                                     when ts.date between active.first_active_day and active.last_active_day then 1
                                     else 0 end) >= 1 then 1
                        else 0 end as isactive
    from timeseries ts
             cross join source_data.tasks_used_da t
             join user_churn_periods_c churn on churn.user_id = t.user_id
             join user_active_periods_c active on active.user_id = t.user_id
    group by t.user_id, ts.date
)

select
    date_part(month,ts.date) as month,
    count(distinct case when ischurn=1 then user_id end ) as churn_users,
    count(distinct case when isactive=1 then user_id end ) as MAU, --monthly active users
    count( distinct case when (date_part(day,ts.date)=01 and isactive=1) then user_id end) as active_users_begining_month --active users on the first day of the month

from user_status_full_ts ts
group by date_part(month,ts.date)