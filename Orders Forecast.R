
#########################################################################
# This is the script for running the model and producing output
#
#########################################################################

#################
#
#
# PACKAGES      #
#
#
#
#################

list.of.packages <- c("ggplot2", "data.table", "dplyr", "RPostgreSQL", "ggthemes",
                      "broom", "tidyr", "lmom", "caret", "lubridate", 
                      "quantmod", "ggrepel", "doMC", "jsonlite", "foreach")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages, dependencies = TRUE)

sapply(list.of.packages, require, character.only = TRUE)


#################
#
#
# CREDENTIALS.  #
#
#
#
#################


cred <- fromJSON('credentials.json')

cube_dbname <- cred$database$cube$dbname
cube_host <- cred$database$cube$host
cube_pass <- cred$database$cube$password
cube_user <- cred$database$cube$user


kinetic_dbname <- cred$database$kinetic$dbname
kinetic_host <- cred$database$kinetic$host
kinetic_pass <- cred$database$kinetic$password
kinetic_user <- cred$database$kinetic$user



#################
#
#
# CONNECTION    #
#
#
#
#################


cons <- dbListConnections(PostgreSQL())

for (con in cons) {
  dbDisconnect(con)
}
# Connect to Sandbox
kinetic <- dbConnect(PostgreSQL(), user= kinetic_user, 
                     password= kinetic_pass,
                     host=kinetic_host, 
                     port=5432, dbname=kinetic_dbname)

##################
#
#
# ROUND FUNCTION #
#
#
#
################## 

Round.Up <- function(x) {
  new_value <- vector("numeric")
  for(i in 1:length(x)){
    remainder <- x[i] %% 1
    if (x[i] == 0){
      new_value[i] <- x[i] + 1
    } else if (remainder !=0){
      new_value[i] <- x[i] - remainder + 1
    } else {
      new_value[i] <- x[i]
    }
  }
  new_value
}


#####################
#
#
# DEFINE FS MARKETS #
#
#
#
#####################  

full_service_markets <- c('Atlanta, GA',
                          'Austin, TX',
                          'Charlotte, NC',
                          'Chattanooga, TN',
                          'Dallas, TX',
                          'Houston, TX',
                          'Jacksonville, FL',
                          'Kansas City, MO',
                          'Knoxville, TN',
                          'Nashville, TN',
                          'Orlando, FL',
                          'Raleigh-Durham-Chapel-Hill, NC',
                          'Saint Louis, MO',
                          'San Antonio, TX',
                          'Fort Worth, TX',
                          'Baltimore, MD',
                          'Birmingham, AL',
                          'Charleston, SC',
                          'Columbus, OH',
                          'Denver, CO',
                          'Indianapolis, IN',
                          'Louisville, KY',
                          'Pittsburgh, PA')
#################
#
#
# GET DATA.     #
#
#
#
#################


# rev_data_query <- "select 
# case when condensed_move_type IN('Full Service') then 'FS' else 'LO' end as move_type,
# executed_revenue,
# super_market as market,
# reservation_start at time zone 'UTC' as reservation_start,
# labor_billable_hours
# 
# from performance_revenue
# where
# complicated_order_status NOT IN('cancelled') AND
# order_state = 'executed' AND
# -- reservation_start > (CURRENT_DATE - INTERVAL '365 days') AND
# reservation_start > '2015-01-01' AND
# active_market = 1"



rev_data_query <- "select 
a.reservation_start at time zone 'UTC' as reservation_start,
a.order_status,
b.name || ', ' || b.state as market
from reservations_order a
INNER JOIN markets_market b
ON a.market_id = b.id
where 
a.order_status IN('complete', 'incomplete', 'booked')
AND b.accepting_orders = TRUE
AND reservation_start::date between '2016-01-01' and CURRENT_DATE - 1
"

rev_data <- dbGetQuery(conn = kinetic, statement = rev_data_query)
setDT(rev_data)
#print(head(rev_data))

#remove rows that don't make sense, time-wise

rev_data <- rev_data[hour(reservation_start) >= 4 & hour(reservation_start) <= 23]

rev_data$date <- as.Date(rev_data$reservation_start)
rev_data$dow <- rev_data$date %>% weekdays()

start_time <- Sys.time()
# Create Input set
rev_data <- rev_data[date >= '2016-01-01'  
                    ,.(#rev = sum(executed_revenue),
                       n_orders = .N, 
                       year = year(date),
                       week_of_year = week(date),
                       quarter = quarter(date),
                       day_of_month = as.integer(format(date, '%d')),
                       month_name = months(date)), 
                     by = .(market, date, dow)]

# Remove NAs
rev_data <- rev_data %>% na.omit

#Days from Origin
min_date <- min(rev_data$date, na.rm = TRUE)
rev_data$days_from_origin <- as.integer(rev_data$date - min_date)

#Week of Month
rev_data$week_of_month <- Round.Up(rev_data$day_of_month/7) 

# Convert all character variables to factors
for(i in seq_along(rev_data)){
  c <- rev_data[[i]] %>% class
  if (c == 'character'){
    rev_data[[i]] <- rev_data[[i]] %>% as.factor
  } else {
    #print(paste0("Column ", i, " is not a character class"))
  }
}

#################
#
#
# FORECASTING  #
#
#
#
#################


######################################################
######### Set up the Dataframe for Predictions #######
######################################################


# How far in the future to predict
future_date <- Sys.Date() + 60
future_date <- ceiling_date((future_date %m+% months(0)),"month")-days(1)

# vector of dates
new_dates <- seq.Date(from = as.Date('2017-01-01'), to = future_date, by = 'day')
# vector of markets
markets <- as.vector(unique(rev_data$market), "character")

# Dataset of all combinations of market and date vectors
future_df <- data.table(expand.grid(market = markets, date = new_dates))

#quarter
future_df[['quarter']] <- future_df[['date']] %>% quarter

#day_of_month
future_df[['day_of_month']] <- future_df[['date']] %>% format('%d') %>% as.integer

#weekdays
future_df[['dow']] <- future_df[['date']] %>% weekdays

#week_of_month
future_df[['week_of_month']] <- (future_df[['day_of_month']] / 7) %>% Round.Up

#week_of_year
future_df[['week_of_year']] <- future_df[['date']] %>% week

#peak_week(will be identified by-market in the modeling loop)
future_df[['peak_week']] <- FALSE
rev_data[['peak_week']] <- FALSE
for(m in markets){
  
  # Identify the weeks as a peak time
  peak_weeks <- rev_data[market == m, .(order_sum = sum(n_orders)), by = .(week_of_year, year)][order(-order_sum)][1:3][['week_of_year']]
  
  # Peak Week
  rev_data[market == m][['peak_week']] <- rev_data[market == m][['week_of_year']] %in% peak_weeks
  
  # Peak Week
  future_df[market == m][['peak_week']] <- future_df[market == m][['week_of_year']] %in% peak_weeks
}

#month_name
future_df[['month_name']] <- future_df[['date']] %>% months

#year
future_df[['year']] <- future_df[['date']] %>% year

#Days From Origin
origin <- min(rev_data$date)

future_df[['days_from_origin']] <- (future_df[['date']] - origin) %>% as.integer

#fs_market
future_df[['fs_market']] <- future_df[['market']] %in% full_service_markets

### Convert character class to factor ###

for(i in seq_along(future_df)){
  c <- future_df[[i]] %>% class
  if (c == 'character'){
    future_df[[i]] <- future_df[[i]] %>% as.factor
  } else {
    #print(paste0("Column ", i, " is not a character class"))
  }
}

#####################################
######### Modeling ##################
#####################################

# Set controls for Cross validation
fitControl <- trainControl(## k-fold CV
  method = "repeatedcv",
  number = 10,
  ## repeated n times
  repeats = 10)

# list for campturing resample data
resamps <- list()

# Multi- Core
registerDoMC(cores = 2)

# Empty Lists
model_output <- list()
model_output <- 
  foreach(i = 1:length(markets)) %dopar% {
    
    m = markets[i]
    
    # temp table for market
    tmp <- rev_data[market == m]
    
    # set training/test set 
    cutoff <- Sys.Date() - 0 # Not using the last x days in the modeling process
    inTraining <- createDataPartition(tmp$month_name, p = .90, list = FALSE)
    train <- tmp[ inTraining,][date <= cutoff] 
    test  <- tmp[-inTraining,][date <= cutoff] 
    
    #### Model ###
    n_orders_fit <- train(n_orders ~ 
                            quarter + day_of_month + 
                            week_of_month:dow + month_name:dow +
                            week_of_month + dow + peak_week:dow +
                            month_name + week_of_year + peak_week + 
                            I(days_from_origin^2) + year, 
                          method = "gbm",
                          data = train,
                          trControl = fitControl)
    
    prediction <- predict(n_orders_fit, test)
    # zero out Any values that were predicted as less than 0
    prediction[prediction < 0] <- 0
    resamp <- postResample(pred = prediction, obs = test$n_orders)
    
    ######### Make Future Predictions ########
    
    new_predictions <- predict(n_orders_fit, future_df[market == m])
    # zero out the negative values
    new_predictions[new_predictions < 0] <- 0
    
    return(data.table(market = m, 
                rmse = resamp[1], 
                r_squared = resamp[2], 
                predicted_orders = new_predictions,
                date = future_df[market == m][['date']])
              )
  }


# Create full df

future_df <- merge(future_df, bind_rows(model_output), by = c('date', 'market'))

#Measure Modeling Time
modeling_time <- Sys.time() - start_time

# Write output to df
# for(i in 1:length(markets)) {
#   future_df[market == markets[i]][['predicted_orders']] <- model_output[[i]]
# }


##############################################
#### Tracking and Adjustments/Buffer #########
##############################################

#Add in actual orders 
future_df <- merge(future_df, 
                   rev_data[, c('market', 'date', 'n_orders')], 
                   by = c('market', 'date'), all.x = TRUE)

## Set up some variables

# How far back to look
days_back = 35
lookback_date = Sys.Date() - days_back

#cities <- unique(future_df$market)
#z_coef <- list()

#List to keep multipliers used
multipliers <- list()
# New Columns
future_df[['predicted_orders_adjusted']]  <- 0
future_df[['predicted_orders_intermediate']]  <- 0

i <- 0

for(m in markets){
  i <- i + 1

  p_tmp <- future_df[market == m & date >= lookback_date & 
                       is.na(n_orders) == FALSE][, 
                                                 c('dow', 
                                                   'predicted_orders', 
                                                   'n_orders')]
  
  # p <- p_tmp[['predicted_orders']]
  # tmp <- rev_data[market == m & date >= lookback_date ][, c('n_orders', 'dow')]
  # actual <- tmp[['n_orders']]
  
  daily_adj <- p_tmp[, 
                     .(diffs = n_orders - predicted_orders, 
                       predicted_orders, n_orders), 
                     dow][, 
                          .(adjustment = 1 + mean(diffs / predicted_orders)), 
                          dow]
  
  # Make adjustments on the dow level using the daily_adj df values 
  for(d in daily_adj$dow){
    dow_multiplier <- daily_adj[dow == d][['adjustment']]
    future_df[market == m & date >= lookback_date & dow == d][['predicted_orders_intermediate']] <- 
      future_df[market == m & date >= lookback_date & dow == d][['predicted_orders']] * dow_multiplier
  }
  
  p <- future_df[market == m & date >= lookback_date & 
                   is.na(n_orders) == FALSE][['predicted_orders_intermediate']]
  actual <- future_df[market == m & date >= lookback_date & 
                   is.na(n_orders) == FALSE][['n_orders']]
  
  
  # A separate loop to look at predicted intermediate values vs forecast and
  # adjust up until x% of the error is positive(over-estimation). That 
  # puts us in a safer place, supply-wise
  prcnt_under <- 1
  multiplier <- 1
  
  while(prcnt_under > .90){
    n_below_actual = sum( (p * multiplier) < actual )
    n_total = length(p)
    prcnt_under <- n_below_actual / n_total
    multiplier <- multiplier + .01
    multipliers[[i]] <- paste0(m, ": ", multiplier)
  }
  
  #z <- line(diff(actual + p))
  #z <- z$coefficients[2]
  #z_coef[[i]] <- paste0(m, ": ", z)
  
  future_df[market == m & date >= lookback_date][['predicted_orders_adjusted']] <- future_df[market == m & date >= lookback_date][['predicted_orders_intermediate']] * multiplier
}

total_time <- Sys.time() - start_time
#print(paste("Total Time Taken: ", total_time))


#################
#
#
# PRODUCT MIX.  #
#
#
#
#################

#################
#
#
# CONNECTION    #
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
                            AND reservation_start::date > CURRENT_DATE - INTERVAL '7 days'
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


#################
#
#
# ROUNDING UP  #
#
#
#
#################

cols <- c('predicted_orders' ,
          'predicted_orders_adjusted' ,
          'total_FS_moves' ,
          'total_FS_moves_adjusted' ,
          'total_LO_moves_adjusted' ,
          'total_LO_moves')





#######################
#
#
#
# ASSIGN TO BLOCKS    #
#
#
#
#
#######################

# use markets in one time zone for simplicity

include_markets <- c('Atlanta', 'Chattanooga', 'Knoxville',
                     'Richmond', 'Baltimore', 'Raleigh-Durham-Chapel-Hill',
                     'Charlotte', 'Jacksonville', 'Tampa', 'Orlando')
include_markets <- paste(include_markets, collapse = "', '")

# ONLY EST MARKETS INCLUDED AND RES START TIMES ARE CONVERTED TO EST. 
# > This lets us get a local-time converted idea of when moves occur. 
#

move_times_query <- paste0("select 
case when condensed_move_type IN('Full Service') then 'FS' else 'LO' end as move_type,
-- super_market as market,
-- reservation_start,
reservation_start at time zone 'EDT' as reservation_start
-- reservation_start at time zone 'UTC' as reservation_start
from performance_revenue
where
complicated_order_status NOT IN('cancelled') AND
order_state = 'executed' AND
reservation_start::date > (CURRENT_DATE - INTERVAL '365 days') AND -- Looking at the last year of order data
market_id IN(select id from markets_market where name IN('",include_markets,"'))
-- reservation_start > '2016-01-01' AND
AND active_market = 1")



move_times <- dbGetQuery(conn = sandbox, statement = move_times_query)
setDT(move_times)
#print(head(rev_data))

#remove rows that don't make sense, time-wise

move_times <- move_times[hour(reservation_start) >= 4 & hour(reservation_start) <= 23]

#Create vectors of start times by product and remove the df
LO_times <- move_times[move_type == 'LO'][['reservation_start']]
FS_times <- move_times[move_type == 'FS'][['reservation_start']]
rm(move_times)



######
#
#
# Inspect Move Time Distribution
#
#
#
######




# Rough time adjustment from GMT time
adj_FS <- FS_times
#group into availability buckets
grouped_fs <- cut(adj_FS, breaks = c(8, 11, 14, 17, 20), include.lowest = TRUE)
FS_data <- data.table(time = FS_times, 
                      hour_bucket = grouped_fs, 
                      dow = weekdays(FS_times),
                      product = 'FS')

## This plot show that the distribution of times that customers are getting
# FS moves is consistent across the days of the week
#

# ggplot(FS_data[is.na(hour_bucket) == FALSE], aes(x = hour_bucket)) +
#   geom_bar(stat = 'count') +
#   facet_wrap(~dow, scales = 'free')


# Rough time adjustment from GMT time
adj_LO <- LO_times
#group into availability buckets
grouped_lo <- cut(adj_LO, breaks = c(8, 11, 14, 17, 20), include.lowest = TRUE)
LO_data <- data.table(time = LO_times, 
                      hour_bucket = grouped_lo, 
                      dow = weekdays(LO_times),
                      product = 'LO')

combined_products <- rbind(FS_data, LO_data)
combined_products <- dcast(combined_products[time >= '2017-01-01', .(count = .N), by = .(dow, product, hour_bucket)], dow+hour_bucket ~product)

# Remove NAs
combined_products <- combined_products[is.na(hour_bucket) == FALSE]
combined_products <- combined_products[, .(prcnt_FS = FS / (FS + LO)), by = .(dow, hour_bucket)]

# ggplot(combined_products[is.na(hour_bucket) == FALSE], aes(x = dow)) +
#   geom_bar(stat = 'count', aes(fill = product)) +
#   theme_bw()





############
#
#
# Blocks
#
#
############



# Multi- Core
#registerDoMC(cores = 2)


# tmp df and vectors
vector_length <- future_df[market == markets[1], .N]

tmp <- data.table()
#block_8to12 <- vector("numeric", vector_length)
#block_12to15 <- vector("numeric", vector_length)
#block_15to18 <- vector("numeric", vector_length)
#block_18to21 <- vector("numeric", vector_length)

# output list
#output <- list()
#l <- 0

# Loop
iteration_dfs <- list()
iterations <- 50
for(iteration in 1:iterations){
  output <- list()
  l <- 0
  for(m in markets){
    l <- l + 1
    tmp <- data.table(future_df[market == m])
    block_8to12 <- vector("numeric", vector_length)
    block_12to15 <- vector("numeric", vector_length)
    block_15to18 <- vector("numeric", vector_length)
    block_18to21 <- vector("numeric", vector_length)
    
    block_8to12_LO <- vector("numeric", vector_length)
    block_12to15_LO <- vector("numeric", vector_length)
    block_15to18_LO <- vector("numeric", vector_length)
    block_18to21_LO <- vector("numeric", vector_length)
    
    block_8to12_FS <- vector("numeric", vector_length)
    block_12to15_FS <- vector("numeric", vector_length)
    block_15to18_FS <- vector("numeric", vector_length)
    block_18to21_FS <- vector("numeric", vector_length)
    
    # market product mix
    percent_FS <- ifelse(m %in% product_mix[market == m], product_mix[market == m][['percent']], 0)
    
    for(i in 1:nrow(tmp)){
      k <- tmp[i][['predicted_orders_adjusted']]
      j <- 0
      while(j < k){
        j <- j + 1
        random_num <- runif(n = 1, min = 0, max = 1)
        if (random_num > percent_FS){
          #Sample start time from LO history. Puts in LO blocks
          move_time <- sample(LO_times , 1)
          move_hour <- hour(move_time)
          if (move_hour %in% (1:11)) {
            block_8to12_LO[i] <- 1 + block_8to12_LO[i]
            
          } else if (move_hour %in% (12:14)) {
            block_12to15_LO[i] <- 1 + block_12to15_LO[i]
            
          } else if (move_hour %in% (15:17)) {
            block_15to18_LO[i] <- 1 + block_15to18_LO[i]
            
          } else {
            block_18to21_LO[i] <- 1 + block_18to21_LO[i]
          }
        } else {
          #Sample start time from FS history. Put in FS blocks
          move_time <- sample(FS_times , 1)
          move_hour <- hour(move_time)
          if (move_hour %in% (1:11)) {
            block_8to12_FS[i] <- 1 + block_8to12_FS[i]
            
          } else if (move_hour %in% (12:14)) {
            block_12to15_FS[i] <- 1 + block_12to15_FS[i]
            
          } else if (move_hour %in% (15:17)) {
            block_15to18_FS[i] <- 1 + block_15to18_FS[i]
            
          } else {
            block_18to21_FS[i] <- 1 + block_18to21_FS[i]
          }
        }
        ## assigning a time block for the move(3-hour blocks)
        if (move_hour %in% (1:11)) {
          block_8to12[i] <- 1 + block_8to12[i]
          
        } else if (move_hour %in% (12:14)) {
          block_12to15[i] <- 1 + block_12to15[i]
          
        } else if (move_hour %in% (15:17)) {
          block_15to18[i] <- 1 + block_15to18[i]
          
        } else {
          block_18to21[i] <- 1 + block_18to21[i]
        }
      }
      output[[l]] <- data.table(market = m,
                                date = future_df[market == m][['date']],
                                block_8to12,
                                block_12to15,
                                block_15to18,
                                block_18to21,
                                block_8to12_LO,
                                block_12to15_LO,
                                block_15to18_LO,
                                block_18to21_LO,
                                block_8to12_FS,
                                block_12to15_FS,
                                block_15to18_FS,
                                block_18to21_FS)
    }
  }
  
  iteration_dfs[[iteration]] <- bind_rows(output)
  
}




full_block_output <- bind_rows(iteration_dfs)

block_data <- full_block_output[, 
                                .(block_8to12 = mean(block_8to12),
                                  block_12to15 = mean(block_12to15),
                                  block_15to18 = mean(block_15to18),
                                  block_18to21 = mean(block_18to21),
                                  block_8to12_LO = mean(block_8to12_LO),
                                  block_12to15_LO = mean(block_12to15_LO),
                                  block_15to18_LO = mean(block_15to18_LO),
                                  block_18to21_LO = mean(block_18to21_LO),
                                  block_8to12_FS = mean(block_8to12_FS),
                                  block_12to15_FS = mean(block_12to15_FS),
                                  block_15to18_FS = mean(block_15to18_FS),
                                  block_18to21_FS = mean(block_18to21_FS)),
                                by = .(market, date)]


# block_data <- full_block_output[, 
#                                 .(block_8to12 = median(block_8to12),
#                                   block_12to15 = median(block_12to15),
#                                   block_15to18 = median(block_15to18),
#                                   block_18to21 = median(block_18to21),
#                                   block_8to12_LO = median(block_8to12_LO),
#                                   block_12to15_LO = median(block_12to15_LO),
#                                   block_15to18_LO = median(block_15to18_LO),
#                                   block_18to21_LO = median(block_18to21_LO),
#                                   block_8to12_FS = median(block_8to12_FS),
#                                   block_12to15_FS = median(block_12to15_FS),
#                                   block_15to18_FS = median(block_15to18_FS),
#                                   block_18to21_FS = median(block_18to21_FS)),
#                                 by = .(market, date)]
#rm(iteration_dfs, output, full_block_output)


####################
#
#
#
# MERGING BLOCKS   #
#
#
#
#
###################



future_df <- merge(future_df, block_data, by = c('market', 'date'))



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


future_df[['created_on']] <- Sys.time()
print("The modeling process is completed")

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


write.csv(future_df, 'orders_forecast_output.csv')




##################################
#
#
#
# PLOTTING DIFFERENT VERSIONS    #
#
#
#
#
##################################

# Connect
cons <- dbListConnections(PostgreSQL())

for (con in cons) {
  dbDisconnect(con)
}
# Connect to Sandbox
sandbox <- dbConnect(PostgreSQL(), user= cube_user, 
                     password= cube_pass,
                     host=cube_host, 
                     port=5432, dbname=cube_dbname)

# Query

forecast_query <- "select 
*
from
(
select 
date::date as date,
market,
predicted_orders_adjusted,
created_on,
created_on::date as version_date
from order_forecast
) tmp
WHERE
date >= (version_date - 7)
"

my_forecast <- dbGetQuery(conn = sandbox, statement = forecast_query)
setDT(my_forecast)

# Unique created_on
min_created_on <- min(my_forecast$created_on)

min_created_on <- as.Date(min_created_on) - 7



#plot

m <- 'Atlanta, GA'

ggplot() +
  geom_line(data = my_forecast[date >= min_created_on &
                          date <= Sys.Date() & 
                          market == m], 
            aes(x = date, y = predicted_orders_adjusted, 
                colour = created_on), size = 1, alpha = .7) +
  geom_line(data = rev_data[date >= min_created_on & 
                                         date < Sys.Date() &
                                         market == m], 
            aes(x = date, y = n_orders), linetype = 3, size = .9) +
  ylab("orders") +
  ggtitle(m) +
  labs(color = "Forecast Versions",
       caption = "**The black dotted-line represents our actual number of orders") +
  theme_economist() 






##############################################################




#
#
# This is the End of the basic forecasting section
#





##############################################################









# truck query is from the original R script. 
# moves <- dbGetQuery(conn = sandbox, statement = truck_move_query)
# setDT(moves)
# moves[, dow := weekdays(res_date)]
# moves <- moves[, .(count = sum(count)), by = .(market, dow, truck_move)]
# moves[, total_moves := sum(count), by = .(market, dow)]
# moves <- moves[truck_move == TRUE, .(percent = count/total_moves), by = .(market, dow) ]
# dow does not look like it has a large effect on product selection





#
#
#
#
#
#
#
#
#
#
#

#########################################################
#
#
#
# Simulate Move times and Product Type                  #
#
#
#
#########################################################



# # Full Service Markets

# full_service_markets <- c('Atlanta, GA',
#                           'Austin, TX',
#                           'Charlotte, NC',
#                           'Chattanooga, TN',
#                           'Dallas, TX',
#                           'Houston, TX',
#                           'Knoxville, TN',
#                           'Nashville, TN',
#                           'Orlando, FL',
#                           'Raleigh-Durham-Chapel-Hill, NC',
#                           'Jacksonville, FL',
#                           'Saint Louis, MO',
#                           'Kansas City, MO',
#                           'San Antonio, TX')



# LO_rev <- rev_data[move_type == 'LO' & executed_revenue >= 80][, c('executed_revenue', 'reservation_start')]
# FS_rev <- rev_data[move_type == 'FS' & executed_revenue >=  160][, c('executed_revenue', 'reservation_start')]


# ## Set up vectors and start simulation

# # times to run
# times = 200


# # Product Mix
# percent_FS <- .45

# # Create vectors
# vector_length <- nrow(investor_goals)

# n_moves_vec <- vector("character", vector_length)
# sim_rev_vec <- vector("character", vector_length)
# date_vec <- vector("character", vector_length)
# market_vec <- vector("character", vector_length)

# # Create a list for dataframes and start the loop
# iterations <- list()


# #################################

# foreach()




# #################################





# #set.seed(1)
# for(x in 1:times){
#   # Making blocks
#   block_1 <- vector("numeric", vector_length)
#   block_2 <- vector("numeric", vector_length)
#   block_3 <- vector("numeric", vector_length)
#   block_4 <- vector("numeric", vector_length)
#   FS_morning_moves <- vector("numeric", vector_length)
#   FS_afternoon_moves <- vector("numeric", vector_length)
#   FS_moves <- vector("numeric", vector_length)
#   LO_moves <- vector("numeric", vector_length)

#   for(i in 1:nrow(investor_goals)){
#     rev_goal <- investor_goals[['ext_rev_goal']][i]
#     iter_rev <- 0
#     n_moves <- 0
#     move_time <- 0

#     if (investor_goals[['market']][i] %in% full_service_markets){

#       while(iter_rev < rev_goal){

#         random_num <- runif(n = 1, min = 0, max = 1)
#         # Can have LO or FS moves in these markets. % chance to have a LO move in if statement
#         if (random_num > percent_FS){
#           #Sample Revenue and start time from LO history
#           v <- sample(LO_rev[['executed_revenue']] , 1)
#           move_time <- sample(LO_rev[['reservation_start']] , 1)
#           LO_moves[i] <- 1 + LO_moves[i]
#         } else {
#           #Sample Revenue and start time from FS history
#           v <- sample(FS_rev[['executed_revenue']] , 1)
#           move_time <- sample(FS_rev[['reservation_start']] , 1)
#           #Assign the FS move sim'd above to the morning or afternoon shift. 
#           if (hour(move_time) < 14){
#             FS_morning_moves[i] <- 1 + FS_morning_moves[i]
#           } else {
#             FS_afternoon_moves[i] <- 1 + FS_afternoon_moves[i]
#           }
#           FS_moves[i] <- 1 + FS_moves[i]
#         }
#         ## assigning a time block for the move(3-hour blocks)
#         move_hour <- hour(move_time)
#         if (move_hour %in% 1:11) {
#           block_1[i] <- 1 + block_1[i]

#         } else if (move_hour %in% 12:14) {
#           block_2[i] <- 1 + block_2[i]

#         } else if (move_hour %in% 15:17) {
#           block_3[i] <- 1 + block_3[i]

#         } else {
#           block_4[i] <- 1 + block_4[i]
#         }
#         # increase total number of moves by 1
#         n_moves <- n_moves + 1
#         # increase the revenue for this iteration for checking in the 'while' loop
#         iter_rev = iter_rev + v
#       }
#     } else {

#       while(iter_rev < rev_goal){
#     # Only Labor-Only moves available in this market
#         v <- sample(LO_rev[['executed_revenue']] , 1)
#         move_time <- sample(LO_rev[['reservation_start']] , 1)
#         LO_moves[i] <- 1 + LO_moves[i]
#         ## assigning a time block for the move
#         move_hour <- hour(move_time)
#         if (move_hour %in% 1:11) {
#           block_1[i] <- 1 + block_1[i]

#         } else if (move_hour %in% 12:14) {
#           block_2[i] <- 1 + block_2[i]

#         } else if (move_hour %in% 15:17) {
#           block_3[i] <- 1 + block_3[i]

#         } else {
#           block_4[i] <- 1 + block_4[i]
#         }
#         # increase total number of moves by 1
#         n_moves <- n_moves + 1
#         # increase the revenue for this iteration for checking in the 'while' loop
#         iter_rev = iter_rev + v
#       }
#     }
#     date_vec[i] <- investor_goals[['ddate']][i]
#     market_vec[i] <- investor_goals[['market']][i]
#     n_moves_vec[i] <- n_moves
#     sim_rev_vec[i] <- iter_rev
#     #print(paste("moves: ", n_moves, "|", "Rev: ", iter_rev))
#   }
#   #Create a data.table from this iteration and push into the iterations list
#   iterations[[x]] <- data.table(date = as.Date(date_vec),
#                                 market = market_vec,
#                                 sim_moves = as.numeric(n_moves_vec),
#                                 sim_rev = as.numeric(sim_rev_vec),
#                                 block_1 = block_1,
#                                 block_2 = block_2,
#                                 block_3 = block_3,
#                                 block_4 = block_4,
#                                 FS_moves = FS_moves,
#                                 FS_morning_moves = FS_morning_moves,
#                                 FS_afternoon_moves = FS_afternoon_moves,
#                                 LO_moves = LO_moves)
# }

# ## Bind all simulations together
# all_sims <- bind_rows(iterations)


# ## Function for rounding up values(to give buffer and provide whole numbers)
# #
# # This Function also rounds up 0-values to 1

# Round.Up <- function(x) {
#   new_value <- vector("numeric")
#   for(i in 1:length(x)){
#     remainder <- x[i] %% 1
#     if (x[i] == 0){
#         new_value[i] <- x[i] + 1
#     } else if (remainder !=0){
#         new_value[i] <- x[i] - remainder + 1
#     } else {
#         new_value[i] <- x[i]
#       }
#   }
#   new_value
# }



# ### Aggregated Summary across Blocks based on all simulations. Check if it's a full service market

# sim_summary <- all_sims[, 
#          .(sim_moves = round(median(sim_moves)),
#            #median_sim_rev = median(sim_rev),
#            total_moves_block_1 = Round.Up(mean(block_1)),
#            total_moves_block_2 = Round.Up(mean(block_2)),
#            total_moves_block_3 = Round.Up(mean(block_3)),
#            total_moves_block_4 = Round.Up(mean(block_4)),
#            only_FS_morning_moves = ifelse(market %in% full_service_markets, Round.Up(median(FS_morning_moves)), 0),
#            only_FS_afternoon_moves = ifelse(market %in% full_service_markets, Round.Up(median(FS_afternoon_moves)), 0),
#            only_LO_moves = Round.Up(median(LO_moves))
#          ), by = .(date, market)]

# sim_summary[, FS_moves := only_FS_morning_moves + only_FS_afternoon_moves]


# # sim_summary[, base_daily_admirals_needed := Round.Up(max(only_FS_morning_moves, only_FS_afternoon_moves)),
# #             by = names(sim_summary)]








