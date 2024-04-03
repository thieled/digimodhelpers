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
