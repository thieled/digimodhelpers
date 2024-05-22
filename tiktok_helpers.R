#' Create Call Grid for TikTok Data Collection
#'
#' This function constructs a grid of account handles or IDs and time frames to collect data from the TikTok API.
#' It makes use of other functions of the digimodhelpers package.
#'
#' @param df A data frame containing the relevant data.
#' @param platform A character vector specifying the platform(s) from which the data is collected.
#'                 Default is c("fb", "ig", "tt", "yt", "tg", "bc", "bs").
#' @param country_var A character string specifying the column name in the data frame that contains country information.
#'                    Default is NULL.
#' @param party_var A character string specifying the column name in the data frame that contains party information.
#'                  Default is NULL.
#' @param name_var A character string specifying the column name in the data frame that contains name information.
#'                 Default is NULL.
#' @param filename_var A character string specifying the column name in the data frame that contains filename information.
#'                     Default is NULL.
#' @param account_var A character string specifying the column name in the data frame that contains the account specifier - ID or handle.
#'                   Default is NULL.
#' @param start_date A character or Date object indicating the starting date of the timeframe.
#' @param end_date A character or Date object indicating the ending date of the timeframe.
#' @param unit A character vector specifying the unit of the time intervals.
#'             Options include "day", "week", "month", "quarter", and "year". Default is "month".
#' @param name_sep A character string specifying the separator for name variables. Default is "-".
#' @param lowercase Logical; if TRUE, filenames will be converted to lowercase. Default is TRUE.
#' @param replace_non_ascii Logical; if TRUE, non-ASCII characters will be replaced in filenames. Default is TRUE.
#' @param filename_sep A character string specifying the separator for filename construction. Default is "_".
#' @param sortBy A character string specifying the parameter by which the data will be sorted.
#' @param parse Logical; if TRUE, the data will be parsed. Default is TRUE.
#' @param data_path A character string indicating the path where the data will be stored.
#' @param drop_existing Logical. Should calls for files that already exist in data_path be excluded from the grid. Default is FALSE.
#'                  Default is NULL, which means the data will be stored in the current directory under the "data" folder.
#' @param count An integer specifying the number of records to fetch. Default is Inf.
#'
#' @return A data frame containing a grid of account ids or handles and time frames for data collection.
#'
#' @export


create_call_grid_tt <- function(df = df,
                                platform = c("fb", "ig", "tt", "yt", "tg", "bc", "bs"),
                                country_var = NULL,
                                party_var = NULL,
                                name_var = NULL,
                                filename_var = NULL,
                                handle_var = NULL,
                                start_date = NULL,
                                end_date = NULL,
                                unit = "month",
                                name_sep = "-",
                                lowercase = TRUE,
                                replace_non_ascii = TRUE,
                                filename_sep = "_",
                                sortBy = "date",
                                parse = TRUE,
                                data_path = NULL,
                                count = Inf,
                                drop_existing = FALSE) {
  
  time_df <- digimodhelpers::slice_timeframes(start_date = start_date,
                                              end_date = end_date,
                                              unit = unit
  )
  
  # Prepare handle df
  handle_df <- drop_redundant(df = df, handle_var = handle_var)
  
  # Prepare filename in handle_df
  handle_df <- digimodhelpers::create_filename(df = handle_df,
                                               platform = platform,
                                               country_var = country_var,
                                               party_var = party_var,
                                               name_var = name_var,
                                               filename_var = filename_var,
                                               name_sep = name_sep,
                                               lowercase = lowercase,
                                               replace_non_ascii = replace_non_ascii,
                                               filename_sep = filename_sep
  )
  
  # Grid
  
  # Ensure that handle_var is named correctly
  grid_list <- list(start_date = time_df[["start_date"]],
                    handle_df[[handle_var]])
  names(grid_list)[[2]] <- handle_var
  
  # Get all combinations of timeframe and accounts
  grid_df <- expand.grid(grid_list) |>
    dplyr::left_join(time_df) |>
    dplyr::left_join(handle_df)
  
  # Add time info to filename
  grid_df[[filename_var]] <-  paste0(
    grid_df[[filename_var]],
    "_FR_",
    sub("\\:", "m", sub("\\:", "h", grid_df[["start_datetime"]])),
    "_TO_",
    sub("\\:", "m", sub("\\:", "h", grid_df[["end_datetime"]])),
    "_DL"
  )
  
  
  # Replace data path if empty
  if(is.null(data_path)) data_path <- "./data"
  
  
  # # Create traktok grid
  if(platform %in% "tt"){
    
    grid_df <- within(grid_df, {
      accounts <- grid_df[[handle_var]]
      start <- grid_df[["start_datetime"]]
      end <- grid_df[["end_datetime"]]
      filename <- grid_df[[filename_var]]
      count <- count
      sortBy <- sortBy
      parse <- parse
      data <- data_path
    })
    
    # Reordering columns
    grid_df <- grid_df[, c("accounts", "start", "end", "filename", "count", "sortBy", "parse", "data")]
    
  }
  
  
  if(drop_existing==TRUE){
    
    grid_df <-  drop_existing(path = data_path,
                              grid = grid_df,
                              filename_var = filename_var)
    
  }
  
  
  return(grid_df)
}


#' Call TikTok API to get video metadata and Log Results
#'
#' This function calls the TikTok API for each row in the provided grid dataframe, logs the results, and optionally returns the results.
#'
#' @param grid_df The grid dataframe containing information about the API calls to be made.
#' @param traceback Logical; indicates whether to show traceback in case of an error. Default is FALSE.
#' @param autolog Logical; indicates whether to start logging. Default is TRUE.
#' @param show_notes Logical; indicates whether to show informational notes during logging. Default is TRUE.
#' @param compact Logical; indicates whether to use a compact logging format. Default is FALSE.
#' @param progress_bar Logical; indicates whether to display a progress bar during API calls. Default is TRUE.
#' @param return_results Logical; indicates whether to return the results of the API calls. Default is TRUE.
#' @param verbose Logical; indicates whether to display verbose output during logging. Default is TRUE.
#' @param fb_token The Facebook API token.
#' @param ig_token The Instagram API token.
#'
#' @return If return_results is TRUE, a list containing the results of the API calls.
#'
#' @export
#'

call_log_tt <- function(grid_df,
                        work_dir,  # Neuer Parameter für das Arbeitsverzeichnis
                        traceback = FALSE,
                        autolog = TRUE,
                        show_notes = TRUE,
                        compact = FALSE,
                        progress_bar = TRUE,
                        return_results = TRUE) {
  
  # Define log file
  now <- format(Sys.time(), "%Y-%m-%dT%Hh%Mm%S")
  dir.create(file.path(grid_df$data[[1]], "log"), showWarnings = FALSE)
  log_file <- paste0(grid_df$data[[1]], "/log", "/call_log_tt_", now, ".log")
  
  # Set up logging
  logger::log_appender(logger::appender_file(log_file))
  logger::log_info("Logging started.")
  
  # Define a function to handle each row of the grid
  process_row <- function(row) {
    accounts <- row$accounts
    start <- row$start
    end <- row$end
    filename <- row$filename
    count <- row$count
    sortBy <- row$sortBy
    parse <- row$parse
    data <- row$data
    
    # Log row information
    logger::log_info(paste0("Calling TT: Account - ", accounts, ", Start - ",
                            start, ", End - ", end, " Filepath - ", data, "/", filename ))
    
    max_retries <- 2  # Maximum number of retries
    retry_count <- 0
    
    while (retry_count < max_retries) {
      tryCatch({
        
        # Format start and end dates
        formatted_start <- format(as.POSIXct(start), "%Y%m%d")
        formatted_end <- format(as.POSIXct(end), "%Y%m%d")
        
        # Call the API function tt_search_api
        result <- traktok::query() |>
          traktok::query_and(field_name = "username",
                             operation = "IN",
                             field_values = accounts) |>
          traktok::tt_search_api(start_date = formatted_start, end_date = formatted_end, max_pages = 10)
        
        if(parse) {
          logger::log_info(paste("Posts fetched:", nrow(result)))
        }
        
        # Save result as JSON
        json_file <- file.path(work_dir, paste0("tt_pull_", filename, "_", now, ".json"))
        jsonlite::write_json(result, json_file)
        logger::log_info(paste("Results saved as JSON:", json_file))
        
        return(result)
        
      }, error = function(e) {
        # Log errors
        logger::log_error(paste("Error in:", filename, "Message:", e$message))
        retry_count <- retry_count + 1
        if (retry_count < max_retries) {
          logger::log_info("Retrying...")
        } else {
          logger::log_error("Max retries reached. Skipping this row.")
        }
        return(NULL)
      }, warning = function(w) {
        logger::log_warn(paste("Warning in", filename, "Message:", w$message))
        return(NULL)
      })
      
      # Increment retry_count
      retry_count <- retry_count + 1
    } # end of while loop
    
    # Return NULL if max retries reached
    return(NULL)
  } # end of process_row function
  
  # Iterate over rows of the grid_df and call process_row for each row
  results <- lapply(split(grid_df, seq(nrow(grid_df))), process_row)
  
  # Close the logging
  logger::log_info("Logging completed.")
  logger::log_appender(NULL)
  
  return(results)
}


#' Call TikTok API to get comment metadata of the collected videos and Log Results
#'
#' This function calls the TikTok API for each row in the provided grid dataframe, logs the results, and optionally returns the results.
#'
#' @param grid_df The grid dataframe containing information about the API calls to be made.
#' @param traceback Logical; indicates whether to show traceback in case of an error. Default is FALSE.
#' @param autolog Logical; indicates whether to start logging. Default is TRUE.
#' @param show_notes Logical; indicates whether to show informational notes during logging. Default is TRUE.
#' @param compact Logical; indicates whether to use a compact logging format. Default is FALSE.
#' @param progress_bar Logical; indicates whether to display a progress bar during API calls. Default is TRUE.
#' @param return_results Logical; indicates whether to return the results of the API calls. Default is TRUE.
#' @param verbose Logical; indicates whether to display verbose output during logging. Default is TRUE.
#' @param fb_token The Facebook API token.
#' @param ig_token The Instagram API token.
#'
#' @return If return_results is TRUE, a list containing the results of the API calls.
#'
#' @export
#'

get_comments_tt <- function(recent_videos, work_dir_input) {
  
  work_dir <- work_dir_input
  
  # Set up logging
  logger::log_appender(logger::appender_file(paste0(work_dir, "/log/", "call_log_tt_comments_", current_time, ".log")))
  logger::log_info("Logging started.")
  
  # Initialize a list to store results for each row
  results_list <- vector("list", nrow(recent_videos))
  
  # Loop over each row of the recent_videos dataframe
  for (i in 1:nrow(recent_videos)) {
    video_id <- recent_videos$video_id[i]
    
    # Log information about the current video being processed
    logger::log_info(paste0("Processing video ID: ", video_id))
    
    tryCatch({
      # Call tt_comments_api function
      result <- traktok::tt_comments_api(video_id, fields = "all", max_pages = 10)
      
      # Add the result to the results list
      results_list[[i]] <- result
      
      # Log success
      logger::log_info(paste("Comments fetched for video ID:", video_id))
      
    }, error = function(e) {
      # Log errors
      logger::log_error(paste("Error processing video ID:", video_id, "Message:", e$message))
    }, warning = function(w) {
      logger::log_warn(paste("Warning processing video ID:", video_id, "Message:", w$message))
    })
  }
  
  # Add the results list as a new column to the dataframe
  recent_videos$comments <- results_list
  
  # Save the entire dataframe as a JSON file
  json_file <- paste0(work_dir, "/tt_comments_of_all_videos_", "FR_", one_week_ago, "_TO_", current_time, ".json")
  jsonlite::write_json(recent_videos, json_file)
  logger::log_info(paste("Results saved as JSON:", json_file))
  
  # Close the logging
  logger::log_info("Logging completed.")
  logger::log_appender(NULL)
  
  return(recent_videos)
}
