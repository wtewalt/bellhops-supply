SELECT 
CASE WHEN a.market IS NOT NULL THEN a.market ELSE b.market END as market,
CASE WHEN a.date IS NOT NULL THEN a.date else b.date end as date,
CASE WHEN a.assignment_type IS NULL AND RIGHT(b.shift_id_description, 5) IN ('08-14', '14-21') then 'axle'
     WHEN a.assignment_type IS NULL AND RIGHT(b.shift_id_description, 5) IN ('08-12', '12-15', '15-18', '18-21') then 'wingmen'
     ELSE a.assignment_type END AS assignment_type,
CASE WHEN a.shift_id_description IS NOT NULL THEN a.shift_id_description
     ELSE b.shift_id_description END AS shift_id_description,
a.total_availability,
a.total_assigned,
b.forecasted_needs,
b.needs
FROM
(

    SELECT 
        a.market,
        a.date,
        a.assignment_type,
        a.shift_id_description,
        a.total_availability,
        b.total_assigned

    FROM
    (

    SELECT 
        date,
        market,
        CASE WHEN is_gopher THEN 'admiral'
             WHEN is_axle_driver THEN 'axle'
             ELSE 'wingmen'
             END as assignment_type,
        -- day_numeral,
        -- dow,
        shift_id_description,
        -- shift_hours_local,
        sum(num) as total_availability
    FROM
    (
        SELECT 
            a.bellhop_id,
            e.name || ', ' || e.state as market,
            d.is_gopher,
            d.is_axle_driver,
            a.start_time::date + day_numeral as date,
            b.shift_id,
            TRIM( TO_CHAR(a.start_time::date + day_numeral, 'Day') ) as dow,
          day_numeral,
            CASE WHEN c.begin_hour_numeral < 10 then 0::text || c.begin_hour_numeral::text ELSE c.begin_hour_numeral::text end || 
                '-' || c.end_hour_numeral as shift_hours_local,
            day_numeral || ' ' || TRIM( TO_CHAR(a.start_time::date + day_numeral, 'Day') ) || ': ' || 
            CASE WHEN c.begin_hour_numeral < 10 then 0::text || c.begin_hour_numeral::text ELSE c.begin_hour_numeral::text end || 
                '-' || c.end_hour_numeral as shift_id_description,
            1 as num

        FROM "availability_availabilityset" AS a
        INNER JOIN "availability_availabilityset_available_shifts" AS b ON b."availabilityset_id" = a."id"
        INNER JOIN availability_shift c ON b."shift_id" = c.id
        INNER JOIN hophr_bellhopprofile d ON a.bellhop_id = d.id
        INNER JOIN markets_market e ON e.id = d.current_market_id

        WHERE 
            b."shift_id" IS NOT NULL
            AND (a.start_time::timestamp) IS NOT NULL
            AND a.start_time::date + day_numeral >= CURRENT_DATE - INTERVAL '21 days' 
            AND a.start_time::date + day_numeral <= CURRENT_DATE + INTERVAL '21 days'
            AND d.employment_status = 'approved'
            AND e.accepting_orders = TRUE

        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    ) availability

    GROUP BY 1, 2, 3, 4
    order by date, market
   
) a
    INNER JOIN (
    SELECT 
        reservation_start::date as date,
        market,
        assignment_type,
        day_num || ' ' || dow || ': ' || shifts as shift_id_description,
        sum(1) as total_assigned
    FROM
    (
      SELECT 
          *,
          EXTRACT(hour from localized_start_time) as local_hour,
          CASE WHEN EXTRACT(dow from localized_start_time::date) = 0 THEN 6 
                    else EXTRACT(dow from localized_start_time::date) - 1 END as day_num,
          CASE WHEN EXTRACT(hour from localized_start_time) IN(7, 8, 9, 10, 11) AND assignment_type = 'wingmen' THEN '08-12'
               WHEN EXTRACT(hour from localized_start_time) IN(12, 13, 14) AND assignment_type = 'wingmen' THEN '12-15'
               WHEN EXTRACT(hour from localized_start_time) IN(15, 16, 17) AND assignment_type = 'wingmen' THEN '15-18'
               WHEN EXTRACT(hour from localized_start_time) IN(18, 19, 20) AND assignment_type = 'wingmen' THEN '18-21'
               WHEN EXTRACT(hour from localized_start_time) IN(7, 8, 9, 10, 11, 12, 13) 
                  AND assignment_type IN( 'admiral', 'axle') THEN '08-14'
               WHEN EXTRACT(hour from localized_start_time) IN(14, 15, 16, 17, 18, 19, 20) 
                  AND assignment_type IN( 'admiral', 'axle') THEN '14-21'
          END as shifts,
          trim( to_char(localized_start_time, 'Day') ) as dow
      FROM
    (
        SELECT 
            a.order_id,
            c.name || ', ' || c.state as market,
            b.reservation_start,
            -- TODO fix this to pull in a localized_start_time instead of calculating it from hard-coded values
            CASE WHEN c.name || ', ' || c.state IN( SELECT market from timezones where tz = 'eastern' )
                      THEN reservation_start at TIME ZONE 'EDT'  
                   WHEN c.name || ', ' || c.state IN( SELECT market from timezones where tz = 'central' )
                      THEN reservation_start at TIME ZONE 'CDT' 
                   WHEN c.name || ', ' || c.state IN( SELECT market from timezones where tz = 'mountain' )
                      THEN reservation_start at TIME ZONE 'MDT' 
                   ELSE NULL 
                   END as localized_start_time,
            CASE WHEN a.product_name IN ('Moving Help Captain', 'Moving Help Wingman', 'Gopher Moving Help Wingman') THEN 'wingmen'
                 WHEN a.product_name = 'Gopher Moving Help Captain' then 'admiral'
                 WHEN a.product_name = 'Axle Driver' then 'axle'
                 END as assignment_type

        FROM pricing_productselection a
        INNER JOIN reservations_order b ON b.id = a.order_id
        INNER JOIN markets_market c ON c.id = b.market_id

        WHERE 
            a.product_name IN ('Gopher Moving Help Captain', -- Admirals
                               'Axle Driver', -- Axle Drivers
                               'Moving Help Captain', -- Wingmen
                               'Moving Help Wingman', -- Wingmen
                               'Gopher Moving Help Wingman' -- Wingmen
                              )
            AND c.accepting_orders = TRUE
            AND b.reservation_start::date >= CURRENT_DATE - INTERVAL '21 days' 
            AND b.reservation_start::date <= CURRENT_DATE + INTERVAL '21 days'
            AND b.order_status IN ('booked','complete','incomplete')

        ) assignments

    ) grouped_assignments

    GROUP BY 1, 2, 3, 4
    
    
) b 
      ON 
        (
            b.date = a.date AND
            b.market = a.market AND
            b.assignment_type = a.assignment_type AND
            b.shift_id_description = a.shift_id_description
      )
    
) a
FULL OUTER JOIN (


    WITH booked_and_forecasted AS
    (

    SELECT 
    case when a.market is NOT NULL then a.market else b.market end as market,
    a.date, 
    case when a.shift_id_description is not null then a.shift_id_description
        else b.shift_id_description end as shift_id_description,
    CASE WHEN '08-12' = RIGHT(a.shift_id_description, 5) 
            THEN LEFT(a.shift_id_description,  LENGTH(a.shift_id_description)- 5) || '08-14'
         WHEN '12-15' = RIGHT(a.shift_id_description, 5) 
            THEN LEFT(a.shift_id_description,  LENGTH(a.shift_id_description)- 5) || '08-14'
         WHEN '15-18' = RIGHT(a.shift_id_description, 5) 
            THEN LEFT(a.shift_id_description,  LENGTH(a.shift_id_description)- 5) || '14-21'
         WHEN '18-21' = RIGHT(a.shift_id_description, 5) 
            THEN LEFT(a.shift_id_description,  LENGTH(a.shift_id_description)- 5) || '14-21'
         END as shift_id_description_fs,
    -- a.total_orders,
    -- a.total_lo_orders,
    -- a.total_fs_orders,
    b.forecasted_orders,
    -- b.forecasted_lo_orders,
    b.forecasted_fs_orders,
    CASE WHEN a.date > CURRENT_DATE THEN GREATEST( a.total_orders, b.forecasted_orders )
       ELSE a.total_orders END as expected_orders,
    CASE WHEN a.date > CURRENT_DATE THEN GREATEST( a.total_lo_orders, b.forecasted_lo_orders )
       ELSE a.total_lo_orders END as expected_lo_orders,
    CASE WHEN a.date > CURRENT_DATE THEN GREATEST( a.total_fs_orders, b.forecasted_fs_orders )
       ELSE a.total_fs_orders END as expected_fs_orders

    FROM
    (

    SELECT 
        date,
        market,
        day_num || ' ' || dow || ': ' || shift as shift_id_description,
        COUNT(DISTINCT id) as total_orders,
        COUNT(DISTINCT id) - SUM(is_gopher) as total_lo_orders,
        SUM(is_gopher) as total_fs_orders
        
    FROM
    (
      SELECT  
          *,
          EXTRACT(hour FROM localized_start_time) as local_hour,
          CASE WHEN EXTRACT(dow FROM localized_start_time::date) = 0 THEN 6 
               ELSE EXTRACT(dow FROM localized_start_time::date) - 1 END as day_num,
          CASE WHEN EXTRACT(hour FROM localized_start_time) IN(7, 8, 9, 10, 11) THEN '08-12'
               WHEN EXTRACT(hour FROM localized_start_time) IN(12, 13, 14) THEN '12-15'
               WHEN EXTRACT(hour FROM localized_start_time) IN(15, 16, 17) THEN '15-18'
               WHEN EXTRACT(hour FROM localized_start_time) IN(18, 19, 20) THEN '18-21'
               ELSE NULL 
               END as shift,
        trim( to_char(localized_start_time, 'Day') ) as dow

        FROM
        (
          SELECT
              b.name || ', ' || b.state as market,
              a.id,
              a.reservation_start::date as date,
              (d.name ILIKE '%gopher%')::integer as is_gopher, 
              CASE WHEN b.name || ', ' || b.state IN( SELECT market FROM timezones where tz = 'eastern' )
                      THEN reservation_start at TIME ZONE 'EDT'  
                   WHEN b.name || ', ' || b.state IN( SELECT market FROM timezones where tz = 'central' )
                      THEN reservation_start at TIME ZONE 'CDT' 
                   WHEN b.name || ', ' || b.state IN( SELECT market FROM timezones where tz = 'mountain' )
                      THEN reservation_start at TIME ZONE 'MDT' 
                   ELSE NULL 
                   END as localized_start_time


          FROM reservations_order a
          INNER JOIN markets_market b ON b.id = a.market_id
          LEFT JOIN reservations_taggedorder c ON c.content_object_id = a.id
          LEFT JOIN taggit_tag d ON c.tag_id = d.id

          WHERE 
              b.accepting_orders = TRUE 
              AND a.reservation_start::date >= CURRENT_DATE - INTERVAL '21 days'
              AND a.reservation_start::date <= CURRENT_DATE + INTERVAL '21 days'
              AND a.order_status IN ('booked','complete','incomplete')
        ) booked_orders
    ) booked_in_shifts

    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3
) a
    FULL OUTER JOIN (
  SELECT DISTINCT ON(1, 2, 3)
      date,
      market,
      shift_id_description,
      created_on as forecast_created_on,
      RIGHT(  shift_id_description, 5 ) as hours,
      forecasted_orders,
      forecasted_lo_orders,
      forecasted_fs_orders
  FROM
  (
    SELECT
        date::date as date,
        market,
        created_on,
        extract(dow FROM date::date - 1) || ' ' || dow || ': ' || '08-12' as shift_id_description,
        block_8to12 as forecasted_orders,
        "block_8to12_LO" as forecasted_lo_orders,
        "block_8to12_FS" as forecasted_fs_orders
    FROM order_forecast
    WHERE 
        date::date >= created_on::date
        AND "block_8to12_LO" is not null
        AND date::date >= CURRENT_DATE - interval '21 days'
        AND date::date <= CURRENT_DATE + interval '21 days'
    UNION

    SELECT  
        date::date as date,
        market,
        created_on,
        extract(dow FROM date::date - 1) || ' ' || dow || ': ' || '12-15' as shift_id_description,
        block_12to15 as forecasted_orders,
        "block_12to15_LO" as forecasted_lo_orders,
        "block_12to15_FS" as forecasted_fs_orders
    FROM order_forecast
    WHERE 
        date::date >= created_on::date
        AND "block_12to15_LO" is not null
        AND date::date >= CURRENT_DATE - interval '21 days'
        AND date::date <= CURRENT_DATE + interval '21 days'
        
    UNION

    SELECT  
        date::date as date,
        market,
        created_on,
        extract(dow FROM date::date - 1) || ' ' || dow || ': ' || '15-18' as shift_id_description,
        block_15to18 as forecasted_orders,
        "block_15to18_LO" as forecasted_lo_orders,
        "block_15to18_FS" as forecasted_fs_orders
    FROM order_forecast
    WHERE 
        date::date >= created_on::date
        AND "block_15to18_LO" is not null
        AND date::date >= CURRENT_DATE - interval '21 days'
        AND date::date <= CURRENT_DATE + interval '21 days'
        
    UNION

    SELECT  
        date::date as date,
        market,
        created_on,
        extract(dow FROM date::date - 1) || ' ' || dow || ': ' || '18-21' as shift_id_description,
        block_18to21 as forecasted_orders,
        "block_18to21_LO" as forecasted_lo_orders,
        "block_18to21_FS" as forecasted_fs_orders
    FROM order_forecast
    WHERE 
        date::date >= created_on::date
        AND "block_18to21_LO" is not null
        AND date::date >= CURRENT_DATE - interval '21 days'
        AND date::date <= CURRENT_DATE + interval '21 days'
  ) forecasted_orders
  WHERE
      date::date >= CURRENT_DATE - interval '21 days'
      AND date::date <= CURRENT_DATE + interval '21 days'
  ORDER BY 1, 2, 3, 4 desc

) b 
      ON 
        (
            b.date = a.date AND
            b.market = a.market AND
            b.shift_id_description = a.shift_id_description
        )
     WHERE
      a.date::date >= CURRENT_DATE - interval '21 days'
      AND a.date::date <= CURRENT_DATE + interval '21 days'
      AND b.date::date >= CURRENT_DATE - interval '21 days'
      AND b.date::date <= CURRENT_DATE + interval '21 days'
    ) 

    SELECT 
        market,
        date,
        shift_id_description_fs as shift_id_description,
        sum(forecasted_fs_orders) as forecasted_needs,
        sum(expected_fs_orders) as needs,
        'axle' as job_assignment 
    FROM booked_and_forecasted
    GROUP BY 1, 2, 3, 6

    UNION

    SELECT 
        market,
        date,
        shift_id_description_fs as shift_id_description,
        sum(forecasted_fs_orders) as forecasted_needs,
        sum(expected_fs_orders) as needs,
        'admiral' as job_assignment 
    FROM booked_and_forecasted
    GROUP BY 1, 2, 3, 6

    UNION

    SELECT 
        market,
        date,
        shift_id_description as shift_id_description,
        forecasted_orders * 2.5 as forecasted_needs,
        expected_orders * 2.5 as needs,
        'wingmen' as job_assignment 
    FROM booked_and_forecasted
    
) b 
  ON (
      a.market = b.market AND
      a.date = b.date AND
      a.shift_id_description = b.shift_id_description AND
      a.assignment_type = b.job_assignment
      )
 WHERE 
 b.date IS NOT NULL
;