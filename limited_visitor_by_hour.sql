/* 
The aim of this query to is provide limited visitors information in a better format.
The format provided allows for easier aggregation and model building.
This query feeds the 'limited_visitor_by_hour' limited_visitor_by_hour
*/ 





select 
all_availability.*,
mm.name || ', ' || mm.state as market

from
(
    select 
    availability.order_number,
    availability.product_type,
    sent_at,
    unnest(availability.available_hours) as "hour",
    unnest(availability.hours_available) as hour_available,
    requested_move_date

    from
    (
        select
        order_number,
        product_type,
        sent_at,
        requested_move_date,
        times_available_array,
        array[8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21] as available_hours,
        array[
        CASE WHEN '{800}'::text[] <@ times_available_array THEN 1 else 0 END,
        CASE WHEN '{900}'::text[] <@ times_available_array THEN 1 else 0 END,
        CASE WHEN '{1000}'::text[] <@ times_available_array THEN 1 else 0 END,    
        CASE WHEN '{1100}'::text[] <@ times_available_array THEN 1 else 0 END,
        CASE WHEN '{1200}'::text[] <@ times_available_array THEN 1 else 0 END,
        CASE WHEN '{1300}'::text[] <@ times_available_array THEN 1 else 0 END,
        CASE WHEN '{1400}'::text[] <@ times_available_array THEN 1 else 0 END,
        CASE WHEN '{1500}'::text[] <@ times_available_array THEN 1 else 0 END,
        CASE WHEN '{1600}'::text[] <@ times_available_array THEN 1 else 0 END,
        CASE WHEN '{1700}'::text[] <@ times_available_array THEN 1 else 0 END,
        CASE WHEN '{1800}'::text[] <@ times_available_array THEN 1 else 0 END,
        CASE WHEN '{1900}'::text[] <@ times_available_array THEN 1 else 0 END,
        CASE WHEN '{2000}'::text[] <@ times_available_array THEN 1 else 0 END,
        CASE WHEN '{2100}'::text[] <@ times_available_array THEN 1 else 0 END
            ] as hours_available

        from
        (
            SELECT
            order_number,
            CASE WHEN service_name ILIKE('%truck%') THEN 'full_service' ELSE 'labor_only' END as product_type,
            sent_at,
            -- date is captured when we show what days we have available. It is not necessarily the 'preferred_date'
            date::date as requested_move_date,
            string_to_array(REGEXP_replace(replace(replace(times_available, '[', ''), ']', ''), '\\+', '', 'g'), ',') as times_available_array
            FROM web_prod.booking_rendered_limited_availability
            WHERE 
            ( (ccc_id is not null) OR (context_ip NOT IN ('104.171.248.240','172.56.4.170') AND ccc_id is null) )
            AND "date"::timestamp >= '2017-01-01'
            GROUP BY 1, 2, 3, 4, 5
        ) available_times

    ) availability

    UNION
    (
        SELECT
        order_number,
        CASE WHEN service_name ILIKE('%truck%') THEN 'full_service' ELSE 'labor_only' END as product_type,
        sent_at,
        "hour",
        -- the whole day is marked as unavailable
        0 as hour_available,
        /* 
        'preferred_date' is the optional field in the order flow before we show what is available. This is what they wanted.
        If preferred_date_available is FALSE, that means that the date they preferred had 0-availability at all hours
        */
        preferred_date::date as requested_move_date
        FROM web_prod.booking_appointment_rendered
        CROSS JOIN (
            select generate_series(8, 21) as "hour"
                    ) hours
        WHERE 
        -- Exclude contact center IP address
        ( (ccc_id is not null) OR (context_ip NOT IN ('104.171.248.240','172.56.4.170') AND ccc_id is null) )
        AND preferred_date is not null 
        AND preferred_date_available IS FALSE
        AND preferred_date >= '2017-01-01'       
    )   
) all_availability

INNER JOIN reservations_order ro on ro.number = all_availability.order_number
INNER JOIN markets_market mm on mm.id = ro.market_id

WHERE
mm.accepting_orders = TRUE

