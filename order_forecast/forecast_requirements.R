#
# This script contains the packages and credentials needed to run the following:
#   * Lead time forecast
#   * Order Forecast
#   * 'initialize_forecast_v2' (this runs the two above)
#
# This script should be sourced before running the other two scripts
#
#





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
                      "quantmod", "ggrepel", "doMC", "jsonlite", "foreach", "purrr")
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


##############
#
#
# FUNCTIONS  #
#
#
#
##############


Round.Up <- function(x) {
  
  # Rounds Up values.
  # Zero-value is rounded to 1
  
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



Simulate.Times <- function(predictions_df, market_col, value_col, sample_times){
  
  # Simulate hourly orders.
  # Uses a daily forecast and historical order time distribution as inputs
  # Output is a df of date, market, hour, and predicted moves
  #
  
  # Outside of loop
  dow_vector <- sample_times %>% wday
  market_dfs <- list()
  outputs_df <- list()
  tmp <- data.table()
  iterations = 10000
  
  markets <- unique(predictions_df[[market_col]])
  # Loop
  j <- 1
  for(m in markets){
    tmp <- data.table(predictions_df[market == m])
    # This is a vector of length 24(each hour in the day)
    # As we loop, we will add a move to the corresponding index/hour
    
    tmp_dow <- tmp$date %>% wday
    
    # Multiplying by a 'precision' multiplier here.
    # This helps the hourly output more closely resemble daily.
    # This is due to the rounding of decimal values.
    # Divide by the precision multiplier later
    precision_multiplier <- 10
    tmp_moves <- (tmp[[value_col]] * precision_multiplier )%>% Round.Up
    
    # Loop through the num of rows and create dfs
    # add dfs to the market_dfs list
    for(i in 1:nrow(tmp)){
      
      k <- tmp_moves[i]
      dow <- tmp_dow[i]
      tmp_times <- sample_times[dow_vector == dow]
      sim_times <- sample(x = tmp_times, k * (iterations/precision_multiplier), replace = TRUE) %>% hour
      daily_df <- data.table(sim_times %>% table)[, .(market = m,
                                                      date = tmp$date[i],
                                                      moves = N/(iterations)),
                                                  by = .]
      market_dfs[[i]] <- daily_df
    }
    outputs_df[[j]] <- bind_rows(market_dfs)
    j <- j + 1
  }
  # Concatenate all simulations into one df
  full_output <- bind_rows(outputs_df)
  # rename column
  setnames(full_output, old = ".", new = "hour")
  
  return(full_output)
}