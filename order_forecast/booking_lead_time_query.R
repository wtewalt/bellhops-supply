
lead_time_query <- "
select 
 market,
 reservation_start::date as \"date\",
 trim( to_char(reservation_start::date, 'Day') ) as weekday,
 trim( to_char(reservation_start::date, 'Month') ) as month, 
 -- 0 as total_executed_moves,
 CASE WHEN reservation_start::date <= CURRENT_DATE then
    sum( (days_booked_before_move >= 1)::integer ) - sum( (1 <= days_cancelled_before_move)::integer )
    ELSE 0 END as \"total_executed_moves\",
 CASE WHEN reservation_start::date <= CURRENT_DATE + INTERVAL '1 day' then
    sum( (days_booked_before_move >= 1)::integer ) - sum( (1 <= days_cancelled_before_move)::integer )
    ELSE 0 END as \"1_day_out_booked\",
 CASE WHEN reservation_start::date <= CURRENT_DATE + INTERVAL '2 days' then
    sum( (days_booked_before_move >= 2)::integer ) - sum( (2 <= days_cancelled_before_move)::integer ) 
    ELSE 0 END as \"2_day_out_booked\",
 CASE WHEN reservation_start::date <= CURRENT_DATE + INTERVAL '3 days' then
    sum( (days_booked_before_move >= 3)::integer ) - sum( (3 <= days_cancelled_before_move)::integer ) 
    ELSE 0 END as \"3_day_out_booked\",
 CASE WHEN reservation_start::date <= CURRENT_DATE + INTERVAL '4 days' then
    sum( (days_booked_before_move >= 4)::integer ) - sum( (4 <= days_cancelled_before_move)::integer ) 
    ELSE 0 END as \"4_day_out_booked\",
 CASE WHEN reservation_start::date <= CURRENT_DATE + INTERVAL '5 days' then
    sum( (days_booked_before_move >= 5)::integer ) - sum( (5 <= days_cancelled_before_move)::integer ) 
    ELSE 0 END as \"5_day_out_booked\",
 CASE WHEN reservation_start::date <= CURRENT_DATE + INTERVAL '6 days' then
    sum( (days_booked_before_move >= 6)::integer ) - sum( (6 <= days_cancelled_before_move)::integer ) 
    ELSE 0 END as \"6_day_out_booked\",
 CASE WHEN reservation_start::date <= CURRENT_DATE + INTERVAL '7 days' then
    sum( (days_booked_before_move >= 7)::integer ) - sum( (7 <= days_cancelled_before_move)::integer ) 
    ELSE 0 END as \"7_day_out_booked\",
 CASE WHEN reservation_start::date <= CURRENT_DATE + INTERVAL '8 days' then
    sum( (days_booked_before_move >= 8)::integer ) - sum( (8 <= days_cancelled_before_move)::integer ) 
    ELSE 0 END as \"8_day_out_booked\",
 CASE WHEN reservation_start::date <= CURRENT_DATE + INTERVAL '9 days' then
    sum( (days_booked_before_move >= 9)::integer ) - sum( (9 <= days_cancelled_before_move)::integer )
    ELSE 0 END as \"9_day_out_booked\",
 CASE WHEN reservation_start::date <= CURRENT_DATE + INTERVAL '10 days' then
    sum( (days_booked_before_move >= 10)::integer ) - sum( (10 <= days_cancelled_before_move)::integer ) 
    ELSE 0 END as \"10_day_out_booked\",
 CASE WHEN reservation_start::date <= CURRENT_DATE + INTERVAL '11 days' then
    sum( (days_booked_before_move >= 11)::integer ) - sum( (11 <= days_cancelled_before_move)::integer ) 
    ELSE 0 END as \"11_day_out_booked\",
 CASE WHEN reservation_start::date <= CURRENT_DATE + INTERVAL '12 days' then
    sum( (days_booked_before_move >= 12)::integer ) - sum( (12 <= days_cancelled_before_move)::integer ) 
    ELSE 0 END as \"12_day_out_booked\",
 CASE WHEN reservation_start::date <= CURRENT_DATE + INTERVAL '13 days' then
    sum( (days_booked_before_move >= 13)::integer ) - sum( (13 <= days_cancelled_before_move)::integer ) 
    ELSE 0 END as \"13_day_out_booked\",
 CASE WHEN reservation_start::date <= CURRENT_DATE + INTERVAL '14 days' then
    sum( (days_booked_before_move >= 14)::integer ) - sum( (14 <= days_cancelled_before_move)::integer ) 
    ELSE 0 END as \"14_day_out_booked\",
 CASE WHEN reservation_start::date <= CURRENT_DATE + INTERVAL '15 days' then
    sum( (days_booked_before_move >= 15)::integer ) - sum( (15 <= days_cancelled_before_move)::integer ) 
    ELSE 0 END as \"15_day_out_booked\",
 CASE WHEN reservation_start::date <= CURRENT_DATE + INTERVAL '16 days' then
    sum( (days_booked_before_move >= 16)::integer ) - sum( (16 <= days_cancelled_before_move)::integer ) 
    ELSE 0 END as \"16_day_out_booked\",
 CASE WHEN reservation_start::date <= CURRENT_DATE + INTERVAL '17 days' then
    sum( (days_booked_before_move >= 17)::integer ) - sum( (17 <= days_cancelled_before_move)::integer ) 
    ELSE 0 END as \"17_day_out_booked\",
 CASE WHEN reservation_start::date <= CURRENT_DATE + INTERVAL '18 days' then
    sum( (days_booked_before_move >= 18)::integer ) - sum( (18 <= days_cancelled_before_move)::integer ) 
    ELSE 0 END as \"18_day_out_booked\",
 CASE WHEN reservation_start::date <= CURRENT_DATE + INTERVAL '19 days' then
    sum( (days_booked_before_move >= 19)::integer ) - sum( (19 <= days_cancelled_before_move)::integer ) 
    ELSE 0 END as \"19_day_out_booked\",
 CASE WHEN reservation_start::date <= CURRENT_DATE + INTERVAL '20 days' then
    sum( (days_booked_before_move >= 20)::integer ) - sum( (20 <= days_cancelled_before_move)::integer ) 
    ELSE 0 END as \"20_day_out_booked\",
 CASE WHEN reservation_start::date <= CURRENT_DATE + INTERVAL '21 days' then
    sum( (days_booked_before_move >= 21)::integer ) - sum( (21 <= days_cancelled_before_move)::integer ) 
    ELSE 0 END as \"21_day_out_booked\",
 CASE WHEN reservation_start::date <= CURRENT_DATE + INTERVAL '22 days' then
    sum( (days_booked_before_move >= 22)::integer ) - sum( (22 <= days_cancelled_before_move)::integer ) 
    ELSE 0 END as \"22_day_out_booked\",
 CASE WHEN reservation_start::date <= CURRENT_DATE + INTERVAL '23 days' then
    sum( (days_booked_before_move >= 23)::integer ) - sum( (23 <= days_cancelled_before_move)::integer ) 
    ELSE 0 END as \"23_day_out_booked\",
 CASE WHEN reservation_start::date <= CURRENT_DATE + INTERVAL '24 days' then
    sum( (days_booked_before_move >= 24)::integer ) - sum( (24 <= days_cancelled_before_move)::integer ) 
    ELSE 0 END as \"24_day_out_booked\",
 CASE WHEN reservation_start::date <= CURRENT_DATE + INTERVAL '25 days' then
    sum( (days_booked_before_move >= 25)::integer ) - sum( (25 <= days_cancelled_before_move)::integer ) 
    ELSE 0 END as \"25_day_out_booked\",
 CASE WHEN reservation_start::date <= CURRENT_DATE + INTERVAL '26 days' then
    sum( (days_booked_before_move >= 26)::integer ) - sum( (26 <= days_cancelled_before_move)::integer ) 
    ELSE 0 END as \"26_day_out_booked\",
 CASE WHEN reservation_start::date <= CURRENT_DATE + INTERVAL '27 days' then
    sum( (days_booked_before_move >= 27)::integer ) - sum( (27 <= days_cancelled_before_move)::integer ) 
    END as \"27_day_out_booked\",
 CASE WHEN reservation_start::date <= CURRENT_DATE + INTERVAL '28 days' then
    sum( (days_booked_before_move >= 28)::integer ) - sum( (28 <= days_cancelled_before_move)::integer ) 
    ELSE 0 END as \"28_day_out_booked\"
 from
 (
    select 
    a.id as order_id,
    a.order_status,
    (a.order_status = 'cancelled')::integer as was_cancelled,
     a.modified_at,
    c.name || ', ' || c.state  as market,
    a.booked_at,
    a.reservation_start,
    a.reservation_end,
    EXTRACT(EPOCH FROM (a.reservation_end - a.booked_at)) / 3600 / 24 as days_booked_before_move,
    CASE WHEN (a.order_status = 'cancelled') = TRUE then EXTRACT(EPOCH FROM (a.reservation_end - a.modified_at)) / 3600 / 24
        else 0 end as days_cancelled_before_move,
    1 as n

    from reservations_order a
    INNER JOIN markets_market c ON c.id = a.market_id
    where 
    a.order_status IN('complete', 'booked', 'incomplete', 'cancelled')
    AND reservation_start::date <= ( CURRENT_DATE + interval '28 days' )::date
    order by order_id 
) tmp


group by 
market,
date

order by
market, 
date desc;
"