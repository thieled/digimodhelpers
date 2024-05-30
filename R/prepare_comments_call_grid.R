#' Read JSON post files and split data into timeframes
#'
#' Prepares the post data to be past on to the comments-collector functions.
#' This function processes JSON files from a specified directory or file paths. It validates the JSON files,
#' parses the filenames to extract information, filters files based on a specified timeframe, and splits the
#' data into multiple data.tables based on timeframe categories.
#'
#' @param dir A character string specifying the directory from which data needs to be parsed.
#' @param filepaths A character vector of file paths to JSON files. If specified, overrides the `dir` parameter.
#' @param cutoff A numeric vector of cutoff times in hours for categorizing the time difference to now.
#' @param recursive A logical value indicating whether to search the directory recursively. Default is FALSE.
#' @param verbose A logical value indicating whether to print messages during processing. Default is TRUE.
#' @param include_latest A logical value indicating whether to include the latest time cutoff. Default is TRUE.
#' @param inclued_oldest A logical value indicating whether to include the oldest time cutoff. Default is FALSE.
#' @param input_tz A character value passed to lubridate::as_datetime indicating the input timezone. Default is "CEST".
#'
#' @return A named list of data.tables split by timeframe categories.
#' @export
#'

slice_post_timeframes <- function(dir = NULL,
                                  filepaths = NULL,
                                  cutoff = NULL,
                                  recursive = FALSE,
                                  verbose = TRUE,
                                  include_latest = TRUE,
                                  inclued_oldest = TRUE,
                                  input_tz = "CEST") {
  # Set time now
  now <- lubridate::as_datetime(lubridate::now(), tz = "UTC")

  # Set cutoffs
  cutoffs <- create_cutoffs(cutoff)

  if(!is.null(cutoff)){
    # Set end
    if(inclued_oldest) end = max(cutoffs$cutoff) else end = cutoffs$cutoff[which.max(cutoffs$cutoff)-1]

    # Set start
    if(include_latest) start = min(cutoffs$cutoff) else start = cutoffs$cutoff[which.min(cutoffs$cutoff)+1]
  } else {
    start <- min(cutoffs$cutoff)
    end <- max(cutoffs$cutoff)
  }

  # Find jsons
  files_df <- parse_filenames(path = dir,
                              filepaths = filepaths,
                              recursive = recursive)



  # Drop files out of timeframe
  files_df$difftonow <- as.numeric(
    difftime(now,
             lubridate::with_tz(lubridate::as_datetime(files_df$to_datetime, input_tz), "UTC"),
             units = "hours", tz = "UTC")
  )

  # Drop rows greater than last_cutoff
  files_df <- files_df[files_df$difftonow <= end, ]

  # Check validity of jsons
  warning_flag <- FALSE

  tryCatch({
    remove_invalid_jsons(filepaths = files_df$full_filepath)
  }, warning = function(w) {
    warning_flag <<- TRUE
  })

  # Remove error files from ct path
  if(files_df[["plat"]][[1]] %in% c("ig", "fb")){
    tryCatch({
      remove_error_jsons(filepaths = files_df$full_filepath)
    }, warning = function(w) {
      warning_flag <<- TRUE
    })
  }

  # If a warning was returned, re-run filename parser
  if (warning_flag) {
    if(verbose) message("Invalid or Error JSONs were moved. Re-running filename parser.")

    # 2 Find jsons
    files_df <- parse_filenames(path = dir, recursive = recursive)

    # 3 Drop files out of timeframe
    files_df$difftonow <- as.numeric(difftime(now, lubridate::as_datetime(tz = "UTC", files_df$to_datetime)))

    # Drop rows greater than last_cutoff
    files_df <- files_df[files_df$difftonow <= end, ]
  }

  # Parse jsons
  post_dt <- parse_data(filepaths = files_df$full_filepath)

  # Rename variables
  if ("published_at" %in% colnames(post_dt)) {
    data.table::setnames(post_dt, "published_at", "published_time")
  }

  # Keep latest unique
  suppressMessages(
    post_dt[, published_time := lubridate::with_tz(lubridate::as_datetime(published_time, input_tz), "UTC")]
  )
  data.table::setindex(post_dt, published_time)

  post_dt[, item_id := as.character(item_id)]
  post_dt <- unique(post_dt, by = "item_id", cols = c("filepath", "published_time"))

  # 8 Compute difftime to now
  post_dt[, diff_time_h := as.numeric(difftime(lubridate::as_datetime(lubridate::now(), tz = "UTC"),
                                               published_time, units = "hours",
                                               tz = "UTC"))]

  # Drop observations exceeding the timeframe
  post_dt <- post_dt[post_dt$diff_time_h <= end, ]
  post_dt <- post_dt[post_dt$diff_time_h >= start, ]

  # Assign time-frame categories
  post_dt[, difftime_category := cut(diff_time_h,
                                     breaks = cutoffs$cutoff,
                                     labels = cutoffs$labels,
                                     right = TRUE,
                                     include.lowest = TRUE)]

  # Create sub-directory 'comments' variable
  post_dt[, save_dir := file.path(dirname(filepath), "comments", difftime_category)]

  # Split the data.table by 'difftime_category'
  split_dt <- split(post_dt, by = "difftime_category", drop = TRUE, sorted = TRUE)

  return(split_dt)
}








#' Check If Comments Were Already Collected
#'
#' This function checks whether items in a given data.table (`dt`) have already been collected by comparing `item_id` values with those found in comment filenames in a specified directory.
#'
#' @param dt A `data.table` containing an `item_id` column and a `save_dir` column. The first element of the `save_dir` column is used to determine the directory to check for collected items.
#'
#' @return A `data.table` with rows removed where the `item_id` is found in the filenames in the specified directory.
#'
#' @details The function retrieves the `save_dir` from the `dt`, uses a safe version of `parse_comment_filenames` to parse filenames in the directory, and removes rows from `dt` where the `item_id` is found in the parsed filenames.
comments_call_deduplicate <- function(dt){

  # Retrieve save directory from dt
  dir = dt[["save_dir"]][1]

  # Parse
  safely_parse_comment_fn <- purrr::safely(parse_comment_filenames, otherwise = NULL)
  comm_file_df <- safely_parse_comment_fn(dir)$result

  if(!is.null(comm_file_df)){
    dt <- dt[!item_id %in% comm_file_df[["item_id"]]]
  }

  return(dt)
}
