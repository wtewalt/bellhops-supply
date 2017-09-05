
/* This query aims to identify the frequency of contiguous time-blocks */

select 
hop_id,
shift_id_taken,
all_possible_shifts,
shift_not_taken,
day_numeral,
week,
lagged_shift,
CASE WHEN shift_not_taken then FALSE else is_contiguous_tmp END as is_contiguous
from
(

select 
wingmen_shift_map.id as hop_id,
lags.shift_id as shift_id_taken,
lags.shift_id is NULL as shift_not_taken,
wingmen_shift_map.shift_id as all_possible_shifts,
day_numeral,
week,
lagged_shift,
(lags.shift_id - lagged_shift) = 1 as is_contiguous_tmp
from
(
    select 
    * ,
    lag(shift_id) over (partition BY bellhop_id, week, day_numeral ORDER BY shift_id, day_numeral, week) as lagged_shift
    from
    (

        SELECT a.bellhop_id,
              b.shift_id,
              c.day_numeral,
              extract(week from a.start_time) as week,
              1 as num
              
        FROM availability_availabilityset a
        INNER JOIN availability_availabilityset_available_shifts b ON b.availabilityset_id = a.id
        INNER JOIN availability_shift c ON b.shift_id = c.id
          
        WHERE b.shift_id IS NOT NULL
              AND (a.start_time::timestamp) IS NOT NULL
              AND shift_id not in(5, 11, 17, 23, 29, 35, 41, 43, 44, 45, 46, 47, 48, 49) -- These are 6-hour blocks
              AND start_time >= '2017-01-01'
              AND bellhop_id IN(select id from hophr_bellhopprofile where employment_status = 'approved')
          
        GROUP BY bellhop_id,
                 shift_id,
                 day_numeral,
                 week,
                 num
    ) shifts

    ORDER BY 
    bellhop_id,
    week,
    day_numeral
limit 28
) lags
FULL OUTER JOIN
(
    select
    id,
    shift_id
    from hophr_bellhopprofile
    CROSS JOIN (
                select 
                unnest(wingmen_shifts) as shift_id
                from (
                     select array[1, 2, 3, 4, -- Monday
                                 7, 8, 9, 10, -- Tuesday
                                 13, 14, 15, 16, -- Wednesday
                                 19, 20, 21, 22, -- Thursday
                                 25, 26, 27, 28, -- Friday
                                 31, 32, 33, 34, -- Saturday
                                 37, 38, 39, 40] -- Sunday
                                 as wingmen_shifts
                     ) shift_ids
                ) unnested
    where employment_status = 'approved'
    and id = 624
) wingmen_shift_map ON (wingmen_shift_map.id = lags.bellhop_id
                        AND wingmen_shift_map.shift_id = lags.shift_id)
    
) all_joined

