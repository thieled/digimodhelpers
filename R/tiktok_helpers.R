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
#'
create_call_grid_tt <- function(df = df,
                                platform = c("fb", "ig", "tt", "yt", "tg", "bc", "bs"),
                                country_var = NULL,
                                party_var = NULL,
                                name_var = NULL,
                                filename_var = NULL,
                                account_var = NULL,
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
  handle_df <- drop_redundant(df = df, account_var = account_var)

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

  # Ensure that account_var is named correctly
  grid_list <- list(start_date = time_df[["start_date"]],
                    handle_df[[account_var]])
  names(grid_list)[[2]] <- account_var

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
      accounts <- grid_df[[account_var]]
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
#' @param work_dir Sets a working dictionary.
#' @param traceback Logical; indicates whether to show traceback in case of an error. Default is FALSE.
#' @param autolog Logical; indicates whether to start logging. Default is TRUE.
#' @param show_notes Logical; indicates whether to show informational notes during logging. Default is TRUE.
#' @param compact Logical; indicates whether to use a compact logging format. Default is FALSE.
#' @param progress_bar Logical; indicates whether to display a progress bar during API calls. Default is TRUE.
#' @param return_results Logical; indicates whether to return the results of the API calls. Default is TRUE.
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


#' Read TikTok JSON post files and split data into timeframes
#'
#' Prepares the TikTok data to be passed on to the comments-collector functions.
#' This function processes JSON files from a specified directory or file paths. It validates the JSON files,
#' parses the filenames to extract information, filters files based on a specified timeframe, and splits the
#' data into multiple data.tables based on timeframe categories.
#'
#' @param dir A character string specifying the directory from which data needs to be parsed.
#' @param cutoff A numeric vector of cutoff times in hours for categorizing the time difference to now.
#' @param recursive A logical value indicating whether to search the directory recursively. Default is \code{FALSE}.
#' @param verbose A logical value indicating whether to print messages during processing. Default is \code{TRUE}.
#' @param include_latest A logical value indicating whether to include the latest time cutoff. Default is \code{TRUE}.
#' @param include_oldest A logical value indicating whether to include the oldest time cutoff. Default is \code{FALSE}.
#' @param input_tz A character value passed to \code{lubridate::as_datetime} indicating the input timezone. Default is \code{"CEST"}.
#'
#' @return A named list of data.tables split by timeframe categories.
#' @export
#'
#' @examples
#' \dontrun{
#' dir <- "path/to/your/directory"
#' cutoff <- c(1, 24, 72, 240)
#' result <- slice_post_timeframes_tt(dir = dir, cutoff = cutoff, recursive = TRUE)
#' }
slice_post_timeframes_tt <- function(dir = NULL,
                                     cutoff = NULL,
                                     recursive = FALSE,
                                     verbose = TRUE,
                                     include_latest = TRUE,
                                     include_oldest = FALSE,
                                     input_tz = "CEST") {

  # Set Time
  now <- lubridate::as_datetime(lubridate::now(), tz = "UTC")
  formatted_now <- format(now, "%Y-%m-%dT%Hh%Mm%S")

  # Set Cutoffs
  cutoffs <- digimodhelpers::create_cutoffs(cutoff)
  if (!is.null(cutoff)) {
    # Set end
    end <- if (include_oldest) max(cutoffs$cutoff) else cutoffs$cutoff[which.max(cutoffs$cutoff) - 1]

    # Set start
    start <- if (include_latest) min(cutoffs$cutoff) else cutoffs$cutoff[which.min(cutoffs$cutoff) + 1]
  } else {
    start <- min(cutoffs$cutoff)
    end <- max(cutoffs$cutoff)
  }

  # Find JSON files
  if (verbose) message("Finding JSON files...")
  file_names <- list.files(dir, full.names = TRUE, recursive = recursive)
  json_files <- file_names[grep("\\.json$", file_names)]
  json_files <- json_files[file.info(json_files)$isdir == FALSE]

  if (length(json_files) == 0) {
    stop("No JSON files found in the specified directory.")
  }

  # Read JSON data and extract columns
  if (verbose) message("Reading JSON data and extracting columns...")
  json_data <- lapply(json_files, function(file) {
    data <- jsonlite::fromJSON(file)
    if (length(data) > 0) {
      data <- data[, c("author_name", "video_id", "create_time")]
      return(data)
    }
  })
  json_data <- json_data[!sapply(json_data, is.null)]

  if (length(json_data) == 0) {
    stop("No valid JSON data found in the specified files.")
  }

  # Combine data
  combined_df <- do.call(rbind, json_data)
  unique_combined_df <- combined_df %>% distinct(video_id, .keep_all = TRUE)

  # Compute time difference to now
  if (verbose) message("Computing time differences...")
  filtered_df <- unique_combined_df %>%
    dplyr::mutate(diff_time_h = as.numeric(difftime(now,
                                             lubridate::as_datetime(create_time, tz = input_tz),
                                             units = "hours")))

  # Filter observations exceeding the timeframe
  filtered_df <- filtered_df %>%
    dplyr::filter(diff_time_h <= end, diff_time_h >= start)

  if (nrow(filtered_df) == 0) {
    stop("No data found within the specified timeframes.")
  }

  # Assign time-frame categories and create sub-directory 'comments' variable
  filtered_df <- filtered_df %>%
    dplyr::mutate(difftime_category = cut(diff_time_h,
                                   breaks = cutoffs$cutoff,
                                   labels = cutoffs$labels,
                                   right = TRUE,
                                   include.lowest = TRUE)) %>%
    dplyr::mutate(save_dir = file.path(dirname(dir), "comments", difftime_category))

  # Split the data.table by 'difftime_category'
  if (verbose) message("Splitting data by timeframe categories...")
  split_df <- split(filtered_df, filtered_df$difftime_category)

  return(split_df)
}


#' Call TikTok API to get comment metadata of the collected videos and log results
#'
#' This function calls the TikTok API for each row in the provided list of data.frames containing video information,
#' collects the comments, logs the results, and saves the updated data.frames with comments as a JSON file.
#'
#' @param dir The directory where the log files and JSON results will be saved.
#' @param split_df A list of data.frames containing video information.
#'
#' @return A list of data.frames with comments added.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Example usage:
#' dir <- "path/to/your/directory"
#' split_df <- list(df1, df2, df3)  # List of data.frames
#' result <- get_comments_tt(dir = dir, split_df = split_df)
#' }
get_comments_tt <- function(dir = NULL,
                            split_df = NULL){

  # Define log file
  now <- format(Sys.time(), "%Y-%m-%dT%Hh%Mm%S")
  dir.create(file.path(paste0(dir, "log"), showWarnings = FALSE))
  log_file <- paste0(dir, "/log", "/call_log_tt_comments_", now, ".log")

  # Set up logging
  logger::log_appender(logger::appender_file(paste0(dir, "/log/", "call_log_tt_comments_", now, ".log")))
  logger::log_info("Logging started.")

  for (i in seq_along(split_df)) {

    df <- split_df[[i]]

    # Initialize a list to store comments for the current dataframe
    comments_list <- vector("list", nrow(df))

    for (j in 1:nrow(df)) {
      video_id <- df$video_id[j]
      # Log information about the current video being processed
      logger::log_info(paste0("Processing video ID: ", video_id))

      # Collect comments for the current video_id
      tryCatch({
        result <- traktok::tt_comments_api(video_id, fields = "all", max_pages = 100)
        comments_list[[j]] <- result
        # Log success
        logger::log_info(paste("Comments fetched for video ID:", video_id))

      }, error = function(e) {
        # Log errors
        logger::log_error(paste("Error processing video ID:", video_id, "Message:", e$message))
      }, warning = function(w) {
        logger::log_warn(paste("Warning processing video ID:", video_id, "Message:", w$message))
      })
    }

    # Add the comments list as a new column to the dataframe
    df$comments <- comments_list

    # Update the dataframe in split_df
    split_df[[i]] <- df
  }

  # Save the entire dataframe as a JSON file
  json_file <- paste0(dir, "/tt_comments_collected_at_", now, ".json")
  jsonlite::write_json(split_df, json_file)
  logger::log_info(paste("Results saved as JSON:", json_file))

  # Close the logging
  logger::log_info("Logging completed.")
  logger::log_appender(NULL)

  return(split_df)
}

