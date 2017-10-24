

###################
#
#
# REQUIREMENTS    #
#
#
#
###################



source('/Users/williamtewalt/forecast_requirements.R')



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


# Lead Time Query (as object 'lead_time_query')
source('/Users/williamtewalt/Documents/Queries/booking_lead_time_query.R')

# markets accepting_orders
source('/Users/williamtewalt/Documents/Queries/market_accepting_orders_query.R')


lead_times <- dbGetQuery(conn = sandbox,
                         statement = lead_time_query)

markets_accepting_orders <- dbGetQuery(conn = sandbox,
                                       statement = market_accepting_orders_query)


setDT(lead_times)



# subset to markets with at least 100 days of operation
lead_times <- lead_times[market %in% lead_times[, .N, by = market][N > 100][['market']]]

#
# remove the first 30-days from each market
# Since we're looking at ~4 weeks of lead-time, we don't want those days
lead_times <- lead_times[, head(.SD, -30), by = market]


#Split into historical and future dates

lead_times_historic <- lead_times[date <= Sys.Date()]
lead_times_future <- lead_times[date > Sys.Date() &
                                  market %in% markets_accepting_orders[['market']]]



# Define the model


lead_time_lm <- function(df, model){
  df %>%
    split("market") %>%
    map(m)
}


##################
# TRY OUT A LOOP #
##################

predictors <- lead_times_historic[, -'total_executed_moves'] %>% names
# surround col names in tick marks
predictors_with_tick <- paste("`", predictors, "`", sep = "")

# This is the first n colunms that are given in the model
initial_n_columns <- 4

formula_string <- paste(predictors_with_tick[1:initial_n_columns], collapse = "+")

# remove the first few predictors(included above in the initial formula string).
# reverse order the predictors so that we include the furthest day out in the model 
#   first and then build from there 

reversed_lag_predictors <- predictors_with_tick[length(predictors_with_tick): ( initial_n_columns + 1 ) ]

# make a list to place models in
models <- list()
#start i at the position
i <- 1
# copying the formula_string. This is what we'll build on
p_string <- formula_string

# LOOP
for(p in reversed_lag_predictors){
  # Build the formula string
  p_string <- paste(p_string, reversed_lag_predictors[[i]], sep = "+")
  # Create the formula
  f <- as.formula(paste("total_executed_moves ~ ",
                        paste(p_string, collapse = '+')))
  #assign the formula to the model
  m <- ~ lm(f, data = .x)
  # apply the model
  models[[i]] <- lead_time_lm(df = lead_times_historic, model = m)
  # increase i by 1
  i <- i + 1
}



## Predicting from each model

days_out <- 28 # this matches the n days out in the lead time booking view

future_date <- Sys.Date() + (days_out - 1)


outputs <- list()


for(days in 1:days_out){
  # This gives output for each market
  date_target <- future_date - (days - 1)
  tmp <- lead_times_future[date == date_target] #[order(market, date)][, head(.SD, 1), by = .(market)]
  k <- nrow(tmp)
  if ( k > 0){
  # p <- predict(models[[days]], lead_times_future[date == future_date - (days - 1)])
    p <- predict(models[[days]], tmp)
    df <- data.table(
                     market = tmp[['market']], #lead_times_future[date == future_date - (days - 1)][['market']],
                     date = date_target,
                     lead_predictions = p[[1]]
                    )
    outputs[[days]] <- df
    
  } else {
    print(paste('No moves scheduled for ', date_target))
  }
}

lead_time_predictions <- bind_rows(outputs)





