#' Calls an API, logs the calls, warnings, and errors.
#'
#' This function calls an API for each row of a given data frame and logs the calls, warnings, and errors.
#'
#' @param grid_df A data frame containing the parameters for the API calls.
#' @param traceback Logical indicating whether traceback information should be included in the log (default: FALSE).
#' @param autolog Logical indicating whether autolog information should be included in the log (default: TRUE).
#' @param show_notes Logical indicating whether notes should be included in the log (default: TRUE).
#' @param compact Logical indicating whether the log should be compacted (default: FALSE).
#' @param progress_bar Logical indicating whether a progress bar should be displayed during processing (default: TRUE).
#' @param return_results Logical indicating whether the results of the API calls should be returned (default: TRUE).
#'
#' @return A list containing the results of the API calls for each row of the data frame.
#'
#' @export
call_log_ct <- function(grid_df,
                        traceback = FALSE,
                        autolog = TRUE,
                        show_notes = TRUE,
                        compact = FALSE,
                        progress_bar = TRUE,
                        return_results = TRUE) {
  # Define log file
  now <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S")
  dir.create(file.path(grid_df$data[[1]], "/log"), showWarnings = FALSE)
  log_file <- paste0(grid_df$data[[1]], "/log", "/call_log_ct_", now, ".log")

  # Set up logging
  logger::log_appender(logger::appender_file(log_file))
  logger::log_info("Logging started.")

  # Define a function to handle each row of the grid
  process_row <- function(accounts,
                          start,
                          end,
                          filename,
                          count,
                          sortBy,
                          parse,
                          data) {
    # Log row information
    logger::log_info(paste0(
      "Calling CT: Account - ", accounts, ", Start - ",
      start, ", End - ", end, " Filepath - ", data, "/", filename
    ))

    tryCatch(
      {
        # Call the API function crowdtangler::ct_posts
        result <- crowdtangler::ct_posts(
          accounts = accounts,
          start = start,
          end = end,
          filename = filename,
          count = count,
          sortBy = sortBy,
          parse = parse,
          data = data
        )

        if (parse) {
          logger::log_info(paste("Posts fetched:", length(result)))
        }

        return(result)
      },
      error = function(e) {
        # Log errors
        logger::log_error(paste("Error in:", filename, "Message:", e$message))
        # Return NULL in case of error
        return(NULL)
      },
      warning = function(w) {
        logger::log_warn(paste("Warning in", filename, "Message:", w$message))
        return(NULL)
      }
    )
  }

  # Use purrr::pmap to iterate over the rows of the grid
  tryCatch(
    {
      results <- purrr::pmap(grid_df, process_row, .progress = progress_bar)
      return(results)
    },
    error = function(e) {
      # Log errors
      logger::log_error(paste("Error in calling purrr::pmap. Message:", e$message))
      # Return NULL in case of error
      return(NULL)
    }
  )

  # Close the logging
  logger::log_info("Logging completed.")
  logger::log_appender(NULL)

  return(results)
}





#' Call YouTube API for Multiple Accounts and Store Results
#'
#' This function calls the YouTube API for multiple accounts specified in a grid dataframe and stores the results in JSON format.
#'
#' @param grid_df A dataframe containing the grid of parameters for each account, including 'data' for the data directory,
#'  'accounts' for the account IDs, 'start' for the start date, and 'end' for the end date.
#' @param traceback Logical. If TRUE, includes a traceback in error messages. Defaults to FALSE.
#' @param autolog Logical. If TRUE, automatically logs messages to a file. Defaults to TRUE.
#' @param show_notes Logical. If TRUE, shows notes in the log file. Defaults to TRUE.
#' @param compact Logical. If TRUE, uses compact formatting for log messages. Defaults to FALSE.
#' @param progress_bar Logical. If TRUE, displays a progress bar. Defaults to TRUE.
#' @param return_results Logical. If TRUE, returns the results. Defaults to TRUE.
#' @param verbose Logical. If TRUE, displays verbose messages. Defaults to TRUE.
#'
#' @return A list containing the results for each account, or NULL if an error occurs.
#'
#' @details This function iterates over each row of the grid dataframe and calls the `get_channel_vids_ERRORS` function from the 'tuber' package
#' to retrieve video details for each account within the specified date range. Results are stored in JSON format in the data directory specified
#' in the grid dataframe. Error messages and warnings are logged to a file.
#'
#' @examples
#' \dontrun{
#' # Create a grid dataframe
#' grid_df <- data.frame(
#'   data = "data_directory",
#'   accounts = c("account1", "account2"),
#'   start = "2022-01-01",
#'   end = "2022-01-31"
#' )
#'
#' # Call the YouTube API for multiple accounts
#' call_log_yt(grid_df)
#' }
#'
#' @export
call_log_yt <- function(grid_df,
                        traceback = FALSE,
                        autolog = TRUE,
                        show_notes = TRUE,
                        compact = FALSE,
                        progress_bar = TRUE,
                        return_results = TRUE,
                        verbose = TRUE) {

  # Check if data dir exists, create if not
  if(!dir.exists(grid_df$data[[1]])){
    dir.create(file.path(grid_df$data[[1]]), recursive = TRUE, showWarnings = FALSE)
  }

  # Define log file
  now <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S")
  dir.create(file.path(grid_df$data[[1]], "/log"),  recursive = TRUE, showWarnings = FALSE)
  log_file <- paste0(grid_df$data[[1]], "/log", "/call_log_yt_", now, ".log")

  # Set up logging
  logger::log_appender(logger::appender_file(log_file))
  logger::log_info("Logging started.")

  # Define a function to handle each row of the grid
  process_row <- function(accounts,
                          start,
                          end,
                          filename,
                          data) {
    # Log row information
    if(verbose){
      message(paste0(
        "Calling YT: Account - ",
        accounts,
        ", Start - ",
        start,
        ", End - ",
        end
      ))
    }

    logger::log_info(paste0(
      "Calling YT: Account - ",
      accounts,
      ", Start - ",
      start,
      ", End - ",
      end
    ))

    result <- tryCatch(
      {
        # Call the API
        get_channel_vids(
          channel_id = accounts,
          start_date = start,
          end_date = end
        )
      },
      error = function(e) {
        # Log errors
        if(verbose){message(e$message)}
        logger::log_error(paste("Error in:", filename, "Message:", e$message))
        # Return NULL in case of error
        return(NULL)
      },
      warning = function(w) {
        if(verbose){message(w$message)}
        logger::log_warn(paste("Warning in", filename, "Message:", w$message))
        return(NULL)
      }
    )

    ## Store results if not empty
    if(!is.null(result) && length(result) > 0){

      # define file path
      file <- paste0(
        data,
        "/",
        filename,
        sub("\\:", "m", sub("\\:", "h", lubridate::now(tzone = "UTC") |> lubridate::format_ISO8601())),
        ".json"
      )

      # Print message and log info
      if(verbose){message(paste0("Entries fetched: n = ", length(result), ". Saving to - ", file))}
      logger::log_info(paste0("Entries fetched: n = ", length(result), ". Saving to - ", file))

      # Convert to json
      js <- jsonlite::toJSON(result)
      write(x = js, file = file)

      return(result)  # Return the result

    }else{
      # Print message and log info
      if(verbose){message(paste0("Entries fetched: n = ", 0, ". Not saving."))}
      logger::log_info(paste0("Entries fetched: n = ", 0, ". Not saving."))
      return(NULL)
    }


  }


  # Wrap function in safely
  process_row_safe <- purrr::safely(process_row, otherwise = NULL)

  # Use purrr::pmap to iterate over the rows of the grid
  tryCatch({
    results <- purrr::pmap(grid_df, .progress = progress_bar, function(...) {

      result <- process_row_safe(...)

      if (!is.null(result$error)) {
        if(verbose){message(paste("Error in 'get_channel_vids.' Message:", result$error$message, ". NOTE: Invalid account ID?"))}
        logger::log_error(paste("Error in 'get_channel_vids.' Message:", result$error$message, ". NOTE: Invalid account ID?"))
      }

      return(result$result
      ) # Return the result portion of the list
    })
  }, error = function(e) {
    # Log errors
    if(verbose){message(paste("Error in calling 'purrr::pmap.' Message:", e$message))}
    logger::log_error(paste("Error in calling 'purrr::pmap.' Message:", e$message))
    # Return NULL in case of error
    return(NULL)
  })

  # Close the logging
  logger::log_info("Logging completed.")
  logger::log_appender(NULL)

  if(return_results){
    return(results)
  }


}


