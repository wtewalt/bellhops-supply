/* This table was created as a central point to join to for formatted shift lengths based on hour */



CREATE TABLE shift_hours AS
(
    select 
    hours,
    CASE WHEN LENGTH(TRIM(wingmen_shifts)) < 5 then '0' || wingmen_shifts ELSE wingmen_shifts END as wingmen_shifts,
    CASE WHEN LENGTH(TRIM(axle_shifts)) < 5 then '0' || axle_shifts ELSE axle_shifts END as axle_shifts

    FROM
    (
        select 
        unnest(hours) as hours,
        unnest(wingmen_shifts) as wingmen_shifts,
        unnest(axle_shifts) as axle_shifts

        from 
        (
            select

            array[8, 9, 10, 11, 
                  12, 13, 14,
                  15, 16, 17,
                  18, 19, 20, 21] as hours, 

            array['9-12', '9-12', '9-12', '9-12',
                  '12-15', '12-15', '12-15',
                  '15-18', '15-18', '15-18',
                  '18-21', '18-21', '18-21', '18-21'] as wingmen_shifts,

            array['8-14', '8-14', '8-14', '8-14',
                  '8-14', '8-14', '14-21',
                  '14-21', '14-21', '14-21',
                  '14-21', '14-21', '14-21', '14-21'] as axle_shifts
        ) tmp

    ) tmp  
)
;