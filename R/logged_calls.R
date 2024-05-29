#' Call Crowdtangle API and Log Results
#'
#' This function calls the Crowdtangle API for each row in the provided grid dataframe, logs the results, and optionally returns the results.
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
call_log_ct <- function(grid_df,
                        traceback = FALSE,
                        autolog = TRUE,
                        show_notes = TRUE,
                        compact = FALSE,
                        progress_bar = TRUE,
                        return_results = TRUE,
                        verbose = TRUE,
                        fb_token,
                        ig_token
) {
  # Define log file
  now <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S")
  dir.create(file.path(grid_df$data[[1]], "/log"), showWarnings = FALSE)
  log_file <- paste0(grid_df$data[[1]], "/log", "/call_log_ct_", now, ".log")

  # Set up logging
  logger::log_appender(logger::appender_file(log_file))
  logger::log_info("Logging started.")
  if(verbose) message("Logging started.")

  # Force set facebook or instagram token
  if(any(substr(grid_df$filename, 1, 2) == "fb")){
    Sys.setenv(CROWDTANGLE_TOKEN = fb_token)
  }else{
    Sys.setenv(CROWDTANGLE_TOKEN = ig_token)
  }

  # Define a function to handle each row of the grid
  process_row <- function(accounts,
                          list,
                          start,
                          end,
                          filename,
                          count,
                          sortBy,
                          parse,
                          data) {
    # Log row information
    logger::log_info(paste0(
      "Calling CT: Account - ", accounts,
      ", List - ", list,
      ", Start - ", start,
      ", End - ", end,
      " Filepath - ", data,
      "/", filename
    ))
    if(verbose) message(paste0(
      "Calling CT: Account - ", accounts,
      ", List - ", list,
      ", Start - ", start,
      ", End - ", end,
      " Filepath - ", data,
      "/", filename
    ))

    tryCatch(
      {
        # Call the API function crowdtangler::ct_posts
        result <- crowdtangler::ct_posts(
          accounts = accounts,
          list = list,
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
          if(verbose) message(paste("Posts fetched:", length(result)))
        }

        return(result)
      },
      error = function(e) {
        # Log errors
        logger::log_error(paste("Error in:", filename, "Message:", e$message))
        if(verbose) warning(paste("Error in:", filename, "Message:", e$message))
        # Return NULL in case of error
        return(NULL)
      },
      warning = function(w) {
        logger::log_warn(paste("Warning in", filename, "Message:", w$message))
        if(verbose) warning(paste("Warning in", filename, "Message:", w$message))
        return(NULL)
      }
    )
  }

  # Store token as variable in grid_df
  # grid_df$token <- token

  # Wrap function in safely
  process_row_safe <- purrr::safely(process_row, otherwise = NULL)

  # Use purrr::pmap to iterate over the rows of the grid
  tryCatch({

    results <- purrr::pmap(grid_df, .progress = progress_bar, function(...) {

      result <- process_row_safe(...)

      if (!is.null(result$error)) {
        if(verbose){message(paste("Error in 'get_channel_vids.' Message:", result$error$message))}
        logger::log_error(paste("Error in 'get_channel_vids.' Message:", result$error$message))
      }

      return(result$result) # Return the result portion of the list

    })
  }, error = function(e) {
    # Log errors
    if(verbose){message(paste("Error in calling 'purrr::pmap.' Message:", e$message))}
    logger::log_error(paste("Error in calling 'purrr::pmap.' Message:", e$message))
    # Return NULL in case of error
    return(NULL)
  })

  # # Use purrr::pmap to iterate over the rows of the grid
  # tryCatch(
  #   {
  #     results <- purrr::pmap(grid_df, process_row, .progress = progress_bar)
  #     return(results)
  #   },
  #   error = function(e) {
  #     # Log errors
  #     logger::log_error(paste("Error in calling purrr::pmap. Message:", e$message))
  #     if(verbose) warning(paste("Error in calling purrr::pmap. Message:", e$message))
  #     # Return NULL in case of error
  #     return(NULL)
  #   }
  # )
  #
  # Close the logging
  logger::log_info("Logging completed.")
  if(verbose) message("Logging completed.")
  logger::log_appender(NULL)

  if(return_results){
    return(results)
  }
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
  #create_dirs_if(dirs = grid_df$data)


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
        "_",
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





#' Call YouTube Comment API for Multiple Video IDs with Logging
#'
#' This function calls the YouTube Comment API for multiple video IDs, retrieves comments, and logs the process.
#'
#' @param video_ids A character vector containing YouTube video IDs for which comments need to be retrieved.
#' @param data_dir Directory path where log files and fetched comments will be saved.
#' @param auth A character string specifying the authentication method. Default is "key".
#' @param simplify A logical value indicating whether to simplify the resulting data structure. Default is FALSE.
#' @param max_n An integer specifying the maximum number of comments to fetch per video. Default is Inf.
#' @param verbose A logical value indicating whether to print detailed progress messages. Default is TRUE.
#' @param progress_bar A logical value indicating whether to display a progress bar. Default is TRUE.
#' @param return_results A logical value indicating whether to return the fetched comments as a list. Default is TRUE.
#'
#' @return If \code{return_results = TRUE}, a list containing the fetched comments for each video ID; otherwise, NULL.
#'
#' @details This function iterates over the provided video IDs, calls the YouTube Comment API for each video,
#'  retrieves comments, and saves the fetched comments to JSON files.
#' It logs the process, including progress messages, warnings, and errors. If any error occurs during the process,
#' it logs the error and returns NULL.
#'
#' @export
#' @seealso \code{\link{get_comments_yt}}
#'

call_log_yt_comments <- function(video_ids,
                                 data_dir = NULL,
                                 auth = "key",
                                 simplify = FALSE,
                                 max_n = Inf,
                                 verbose = TRUE,
                                 progress_bar = TRUE,
                                 return_results = TRUE
){

  # Check if data dir exists, create if not
  if(!is.null(data_dir)){
    if(!dir.exists(data_dir)){
      dir.create(file.path(data_dir), recursive = TRUE, showWarnings = FALSE)
    }
  }

  # Define log file
  now <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S")
  dir.create(file.path(data_dir, "/log"),  recursive = TRUE, showWarnings = FALSE)
  log_file <- file.path(data_dir, "log", paste0("call_log_yt_", now, ".log"))

  # Set up logging
  logger::log_appender(logger::appender_file(log_file))
  logger::log_info("Logging started.")

  # Define a function to process one
  process_one <- function(video_id,
                          data_dir = data_dir,
                          simplify = simplify,
                          max_n = max_n,
                          verbose = verbose,
                          auth = auth
  ) {
    # Log row information
    if(verbose) message(paste0("Get YT comments for ", video_id))
    logger::log_info(paste0("Get YT comments for ", video_id))

    result <- tryCatch(
      {
        # Call the API
        digimodhelpers::get_comments_yt(
          video_id = video_id,
          simplify = simplify,
          max_n = max_n,
          verbose = verbose,
          auth = auth
        )
      },
      error = function(e) {
        # Log errors
        if(verbose){message(e$message)}
        logger::log_error(paste("Error calling:", video_id, "Message:", e$message))
        # Return NULL in case of error
        return(NULL)
      },
      warning = function(w) {
        if(verbose){message(w$message)}
        logger::log_warn(paste("Warning calling", video_id, "Message:", w$message))
        return(NULL)
      }
    )

    ## Store results if not empty
    if(!is.null(result) && length(result) > 0){

      n_fetched <- result |> purrr::map(~length(.x$items)) |> unlist() |> sum()

      file <- file.path(data_dir, paste0("yt_comm_", video_id, "_DL_",
                                         sub("\\:", "m", sub("\\:", "h", lubridate::now(tzone = "UTC") |> lubridate::format_ISO8601())),
                                         ".json"))

      # Print message and log info
      if(verbose){message(paste0("Fetched n = ", n_fetched, " comments. Saving to - ", file))}
      logger::log_info(paste0("Fetched n = ", n_fetched, " comments. Saving to - ", file))

      # Convert to json
      js <- jsonlite::toJSON(result)
      write(x = js, file = file)

      return(result)  # Return the result

    }else{
      # Print message and log info
      if(verbose) message(paste0("Fetched n = ", 0, "comments. Not saving."))
      logger::log_info(paste0("Fetched n = ", 0, "comments. Not saving."))
      return(NULL)
    }

  }


  # Wrap function in safely
  process_one_safe <- purrr::safely(process_one, otherwise = NULL)


  # Use purrr::map to iterate over video ids
  tryCatch({
    results <- purrr::map(video_ids,
                          data_dir = data_dir,
                          simplify = simplify,
                          max_n = max_n,
                          verbose = verbose,
                          auth = auth,
                          .progress = progress_bar, function(...) {

                            result <- process_one_safe(...)

                            if (!is.null(result$error)) {
                              if(verbose){message(paste("Error in 'get_comments_yt' Message:", result$error$message))}
                              logger::log_error(paste("Error in 'get_comments_yt' Message:", result$error$message))
                            }

                            return(result$result) # Return the result portion of the list
                          })
  }, error = function(e) {
    # Log errors
    if(verbose){message(paste("Error in calling 'purrr::map.' Message:", e$message))}
    logger::log_error(paste("Error in calling 'purrr::map.' Message:", e$message))
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

