


# This script serves as the central point where are forecast data is joined.
#
# Included are the lead time forecast and the order forecast.
#
# Once all sources are ran, data from each is compared and then undergoes
#   additional analysis
#



# Capture the start time #

start_time <- Sys.time()



#####################
#
#
#
# FORECAST MODULES
#
#
#
#####################



### Lead Time Forecast
source('/Users/williamtewalt/Lead Time Forecast.R')


### Order forecast
source('/Users/williamtewalt/Orders Forecast.R')



###########
#
#
#
# CLEANUP
#
#
#
###########

to_keep <- c('lead_time_predictions', 'future_df', 'start_time')

removal_list <- ls()[!(ls() %in% to_keep)]
rm(list = removal_list)




##########################
#
#
# REQUIREMENTS SCRIPT    #
#
#
#
##########################



source('/Users/williamtewalt/forecast_requirements.R')


######################
#
#
#
# Compare Predictions
#
#
#
######################

# Merge and compare
future_df <- merge(future_df, lead_time_predictions, by = c('market', 'date'), all = TRUE)

# Replace NA with 0s ( for max function below to work properly)
future_df$lead_predictions[(is.na(future_df$lead_predictions))] <- 0

# assign the max prediction as the forecasted value. Must not have NAs(See above)
future_df$predicted_orders_adjusted <- 
  unlist( map2(future_df$predicted_orders_adjusted, future_df$lead_predictions, max) )

#remove the lead predictions.
future_df[['lead_predictions']] <- NULL


#####################
#
#
#
# Simulation Section
#
#
#
#####################




#################
#
#
# PRODUCT MIX.  #
#
#
#
#################

cons <- dbListConnections(PostgreSQL())

for (con in cons) {
  dbDisconnect(con)
}
# Connect to Sandbox
sandbox <- dbConnect(PostgreSQL(), user= cube_user, 
                     password= cube_pass,
                     host=cube_host, 
                     port=5432, dbname=cube_dbname)

### Query

# [TODO] convert to gospel orders table query
product_mix_query <- paste0("with CTE as
                            (
                            select 
                            market,
                            1 as num,
                            case when condensed_move_type = 'Full Service' then TRUE else FALSE end as truck_move
                            from performance_revenue
                            where 
                            order_state = 'executed'
                            AND market_id IN (Select id from markets_market where accepting_orders = TRUE)
                            AND reservation_start::date > CURRENT_DATE - INTERVAL '14 days'
                            )
                            select 
                            market,
                            truck_move,
                            sum(1) as count
                            FROM CTE
                            group by 1, 2")


product_mix <- dbGetQuery(conn = sandbox, statement = product_mix_query)
setDT(product_mix)

product_mix[, total := sum(count), by = .(market)]

product_mix <- product_mix[truck_move == TRUE][, percent := count/total]

#########################################################
# Splitting moves into FS and LO based on product mix.  #

#   - Also splits out adjusted FS and LO values         #
#########################################################


future_df$total_FS_moves <- 0
future_df$total_FS_moves_adjusted <- 0
future_df$total_LO_moves_adjusted <- 0
condition <- future_df$predicted_orders_adjusted > 0
adjust_date <- future_df[condition][,min(date)]

for(m in unique(product_mix$market)){
  FS_mix <- product_mix[market == m][['percent']]
  
  future_df[market == m][['total_FS_moves']] <- future_df[market == m][['predicted_orders']] * FS_mix
  
  future_df[market == m & date >= adjust_date][['total_FS_moves_adjusted']] <- 
    future_df[market == m & date >= adjust_date][['predicted_orders_adjusted']] * FS_mix
}

# Calculate LO moves(Initial and Adjusted)
future_df$total_LO_moves <-   future_df$predicted_orders - future_df$total_FS_moves

future_df[date >= adjust_date, total_LO_moves_adjusted := predicted_orders_adjusted - total_FS_moves_adjusted]


#Split into 2 df. FS predictions and LO predictions

fs_cols_to_keep <- c('market', 'date', 'total_FS_moves_adjusted')
fs_moves_daily_df <- future_df[date >= Sys.Date() - 7, fs_cols_to_keep, with = FALSE]
names(fs_moves_daily_df)[names(fs_moves_daily_df) == 'total_FS_moves_adjusted'] <- 'moves'


lo_cols_to_keep <- c('market', 'date', 'total_LO_moves_adjusted')
lo_moves_daily_df <- future_df[date >= Sys.Date() - 7, lo_cols_to_keep, with = FALSE]
names(lo_moves_daily_df)[names(lo_moves_daily_df) == 'total_LO_moves_adjusted'] <- 'moves'



######################
#
#
#
# ASSIGN TO HOURS    #
#
#
#
#
######################

# use markets in one time zone for simplicity

# for gospel
gospel_include_markets <- c('Atlanta, GA', 'Chattanooga, TN', 'Knoxville, TN',
                     'Richmond, VA', 'Baltimore, MD', 'Raleigh-Durham-Chapel-Hill, NC',
                     'Charlotte, NC', 'Jacksonville, FL', 'Tampa, FL', 'Orlando, FL',
                     'Pittsburgh, PA', 'Charleston, SC')

# For performance rev
include_markets <- c('Atlanta', 'Chattanooga', 'Knoxville',
                     'Richmond', 'Baltimore', 'Raleigh-Durham-Chapel-Hill',
                     'Charlotte', 'Jacksonville', 'Tampa', 'Orlando',
                     'Pittsburgh', 'Charleston')

include_markets <- paste(include_markets, collapse = "', '")

# ONLY EST MARKETS INCLUDED AND RES START TIMES ARE CONVERTED TO EST. 
#     * This lets us get a local-time converted idea of when moves occur. 
#

# OLD VERSION USING PERFORMANCE REVENUE VIEW
# move_times_query <- paste0("select
#                            case when condensed_move_type IN('Full Service') then 'full_service' else 'labor_only' end as broad_move_type,
#                            reservation_start at time zone 'EDT' as reservation_start
#                            from performance_revenue
#                            where
#                            complicated_order_status NOT IN('cancelled') AND
#                            order_state = 'executed' AND
#                            reservation_start::date > (CURRENT_DATE - INTERVAL '365 days') AND -- Looking at the last year of order data
#                            market_id IN(select id from markets_market where name IN('",include_markets,"'))
#                            AND active_market = 1")

move_times_query <- paste0("select
                           reservation_start at time zone 'UTC' as reservation_start,
                           super_market_name as market,
                           broad_move_type
                           from _gospel.orders
                           where order_status IN('complete', 'incomplete', 'booked')
                           AND reservation_start::date > CURRENT_DATE - '365 days'::interval
                           AND super_market_name IN('",gospel_include_markets,"')")


move_times <- dbGetQuery(conn = sandbox, statement = move_times_query)
setDT(move_times)


#remove rows that don't make sense, time-wise
move_times <- move_times[hour(reservation_start) >= 4 & hour(reservation_start) <= 23]

# Set aside the future move times for later
booked_move_times <- move_times[as.Date(reservation_start) >= Sys.time()]

#Create vectors of start times by product and remove the df
LO_times <- move_times[broad_move_type == 'labor_only'][['reservation_start']]
FS_times <- move_times[broad_move_type == 'full_service'][['reservation_start']]
rm(move_times)



############
#
#
# Hours Sim
#
#
############
# 



lo_moves_hourly_df <- Simulate.Times(predictions_df = lo_moves_daily_df,
                                     market_col = 'market',
                                     value_col = 'moves',
                                     booked_moves_df = booked_move_times[broad_move_type == 'labor_only'],
                                     sample_times = LO_times)


fs_moves_hourly_df <- Simulate.Times(predictions_df = fs_moves_daily_df,
                                     market_col = 'market',
                                     value_col = 'moves',
                                     booked_moves_df = booked_move_times[broad_move_type == 'full_service'],
                                     sample_times = FS_times)


#######################
#
#
#
# Concatenate the dfs  #
#
#
#
#
#######################

# Identify the move type
lo_moves_hourly_df$move_type <- 'labor_only'
fs_moves_hourly_df$move_type <- 'full_service'


# bind rows
predictions_hourly_df <- bind_rows(lo_moves_hourly_df, fs_moves_hourly_df)

# clean up
rm(lo_moves_hourly_df, fs_moves_hourly_df)



#######################
#
#
#
# 'CREATED ON' Stamp  #
#
#
#
#
#######################


# Stamp with current time
predictions_hourly_df[['created_on']] <- Sys.time()


# Post completion time
print(paste("The modeling process is completed: ", print(Sys.time() - start_time)))



###################
#
#
#
# WRITE TO CSV    #
#
#
#
#
##################


write.csv(predictions_hourly_df, 'orders_forecast_hourly_output.csv')



##################
#
#
#
# CLEAN UP       #
#
#
#
#
##################


# Removal all but predictions #

removal_list <- ls()[ls() != 'predictions_hourly_df']

rm(list = removal_list)
  
  