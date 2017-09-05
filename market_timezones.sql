CREATE TABLE timezones AS

select 
unnest(markets) as market,
tz
from
 (
    select 
        ARRAY['Baltimore, MD', 'Chattanooga, TN', 'Charlotte, NC', 'Charlotte, NC', 
              'Jacksonville, FL', 'Knoxville, TN', 'Pittsburgh, PA', 
              'Raleigh-Durham-Chapel-Hill, NC', 'Richmond, VA', 'Columbus, OH', 'Atlanta, GA', 
              'Indianapolis, IN', 'Louisville, KY'] as markets,
        'eastern' as tz
     
     UNION
    
     select 
        ARRAY['Austin, TX', 'Birmingham, AL', 'Charleston, SC', 'Dallas, TX', 'Fort Worth, TX',
              'Houston, TX', 'Kansas City, MO', 'Nashville, TN', 'Saint Louis, MO', 'San Antonio, TX'] as markets,
        'central' as tz
     
     UNION
    
     select 
        ARRAY['Denver, CO'] as markets,
        'mountain' as tz
     ) timezones