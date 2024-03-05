
#' Create Filenames for Social Media Data Collection
#'
#' This function constructs filenames for social media data collection based on specified parameters.
#'
#' @param df A data frame containing the data.
#' @param platform A character vector specifying the platform(s) from which the data is collected. Default is c("fb", "ig", "tt", "yt", "tg", "bc", "bs").
#' @param country_var A character string specifying the column name in the data frame that contains country information. Default is NULL.
#' @param party_var A character string specifying the column name in the data frame that contains party information. Default is NULL.
#' @param name_var A character string specifying the column name in the data frame that contains name information. Default is NULL.
#' @param filename_var A character string specifying the column name to store the constructed filenames. Default is NULL.
#' @param name_sep A character string specifying the separator for name variables. Default is "-".
#' @param lowercase Logical; if TRUE, filenames will be converted to lowercase. Default is TRUE.
#' @param replace_non_ascii Logical; if TRUE, non-ASCII characters will be replaced in filenames. Default is TRUE.
#' @param filename_sep A character string specifying the separator for filename construction. Default is "_".
#'
#' @return The input data frame \code{df} with an additional column containing constructed filenames.
#'
#' @examples
#' df <- data.frame(country = c("USA", "UK", "Germany"),
#'                  party = c("Democrat", "Republican", "Green"),
#'                  name = c("Joe Biden", "Donald Trump", "Angela Merkel"))
#'
#' create_filename(df,
#' platform = "fb",
#' country_var = "country",
#' party_var = "party",
#' name_var = "name",
#' filename_var = "filename")
#'
#' @export
create_filename <- function(df,
                            platform = c("fb", "ig", "tt", "yt", "tg", "bc", "bs"),
                            country_var = NULL,
                            party_var = NULL,
                            name_var = NULL,
                            filename_var = NULL,
                            name_sep = "-",
                            lowercase = T,
                            replace_non_ascii = T,
                            filename_sep = "_"
){

  # Error messages
  name_index <- match(name_var, names(df))
  if (is.null(name_var)||is.na(name_index))
    stop("name_var column not found or invalid")

  # Create filename
  df[[filename_var]] <-  paste(

    platform,

    if(lowercase == T) tolower( if(replace_non_ascii == T) textclean::replace_non_ascii( if(!is.null(country_var)) df[[country_var]] else "") ), # country

    if(lowercase == T) tolower( if(replace_non_ascii == T) textclean::replace_non_ascii( if(!is.null(party_var)) gsub("[^[:alnum:]]", "", df[[party_var]]) else "") ), # party

    if(lowercase == T) tolower( if(replace_non_ascii == T) textclean::replace_non_ascii( if(!is.null(name_var)) gsub("[^[:alnum:]]", name_sep, df[[name_var]]) else "") ), # name variable

    sep = filename_sep

  )


  return(df)

}




#' Drop Redundant Rows from a Data Frame
#'
#' This function removes redundant rows from a data frame based on a specified variable.
#'
#' @param df A data frame to be processed.
#' @param handle_var A character string specifying the column name in the data frame based on which redundancy is checked. Default is NULL.
#'
#' @return A data frame with redundant rows removed based on the specified variable.
#'
#' @examples
#' df <- data.frame(handle = c("herbertkickl", NA, "wernerkogler", "wernerkogler"))
#' drop_redundant(df, handle_var = "handle")
#'
#' @export
drop_redundant <- function(df,
                           handle_var = NULL){

  # Error messages
  handle_index <- match(handle_var, names(df))
  if (is.null(handle_var) || is.na(handle_index))
    stop("handle_var column not found or invalid")

  # Drop missings and drop duplicates
  df <- df |>
    dplyr::filter(!is.na(.data[[handle_var]])) |>
    dplyr::distinct(.data[[handle_var]], .keep_all = TRUE)

  return(df)

}



#' Slice Timeframes
#'
#' Generate a sequence of timeframes based on specified start and end dates.
#'
#' @param start_date The starting date for the sequence of timeframes. Default is NULL.
#' @param end_date The ending date for the sequence of timeframes. Default is NULL.
#' @param unit The time unit for slicing the timeframes: "day", "week", "month", "quarter", "year". Default is "day".
#'
#' @return A data frame containing the sliced timeframes with columns:
#'   \itemize{
#'     \item \code{start_date}: The start date of each timeframe.
#'     \item \code{start_datetime}: The start datetime of each timeframe in ISO 8601 format.
#'     \item \code{end_date}: The end date of each timeframe.
#'     \item \code{end_datetime}: The end datetime of each timeframe in ISO 8601 format.
#'   }
#'
#' @details This function generates a sequence of timeframes based on the specified start and end dates.
#' If start_date or end_date is not provided, default values are used. If end_date is in the future,
#' it is replaced by the current date. If start_date is after end_date, it is adjusted accordingly.
#'
#' @examples
#' slice_timeframes(start_date = "2023-01-01", end_date = "2023-12-31", unit = "month")
#'
#' @export
slice_timeframes <- function(start_date = NULL,
                             end_date = NULL,
                             unit = c("day", "week", "month", "quarter", "year")) {

  # Check if end_date is empty
  if (is.null(end_date)) {
    end_date <- lubridate::today()
    cat("end_date is empty. replace by today, 00:00. \n")
  }

  end_date <- as.Date(end_date)

  # Check if end_date is in the future
  if (end_date > lubridate::today()) {
    end_date <- lubridate::today()
    cat("end_date is in the future or empty. replace by today, 00:00. \n")
  }

  # Check if start_date is empty
  if (is.null(start_date)) {
    start_date <- lubridate::today() - lubridate::days(7)
    cat("start_date is empty. replace by 1 week before today. \n")
  }

  # Check if start_date is after end_date
  if (as.Date(start_date) > end_date){
    start_date <- end_date - lubridate::days(7)
    cat("start_date is after end_date. replace by 1 week before end_date. \n")
  }

  # Set start date in Date format
  start_date <- as.Date(start_date)

  # Check if start_date is in the future
  if (start_date >= lubridate::today()) {
    start_date <- end_date - lubridate::days(7)
    cat("start_date is in the future. replace by 1 week before end date. \n")
  }

  # Get date sequence
  seq_dates <- seq(start_date, end_date, by = unit) # generate sequence of dates
  start_days <- lubridate::ceiling_date(lubridate::ymd(seq_dates), unit = unit) # get round start day of next timeframe
  start_days <- c(as.Date(start_date), start_days[-length(start_days)]) # use original start date; remove last element
  start_datetime <- start_days |> lubridate::as_datetime()  |> lubridate::format_ISO8601() # set 00h00m00s as start time; bring into the correct format
  end_datetime <- (lubridate::ceiling_date(lubridate::ymd(start_days), unit = unit) - lubridate::milliseconds(1)) |>  lubridate::format_ISO8601() # get last day of timeframes; set format

  start_days
  end_days <- lubridate::ceiling_date(lubridate::ymd(start_days), unit = unit)
  end_days <- c(end_days[-length(end_days)], as.Date(end_date)) # replace last end date by end date set by user
  end_datetime <- (end_days - lubridate::milliseconds(1)) |>  lubridate::format_ISO8601()

  # Replace last datetime with now if later than now
  if (end_datetime[which.max(lubridate::as_datetime(end_datetime))] > lubridate::now(tzone = "UTC")) {
    end_datetime[which.max(lubridate::as_datetime(end_datetime))] <- lubridate::now(tzone = "UTC") |> lubridate::format_ISO8601()
  }

  end_days <- lubridate::as_date(lubridate::as_datetime(end_datetime))

  df <- data.frame(start_date = start_days,
                   start_datetime = start_datetime,
                   end_date = end_days,
                   end_datetime = end_datetime
  )

  return(df)
}



#' Create Call Grid for Social Media Data Collection
#'
#' This function constructs a grid of account handles and time frames to collect data from various APIs,
#' especially useful for social media platforms.
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
#' @param handle_var A character string specifying the column name in the data frame that contains handle information.
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
#' @return A data frame containing a grid of account handles and time frames for data collection.
#'
#' @examples
#' df <- tibble::tribble(
#'   ~country, ~party, ~name, ~handle,
#'   "at", "fpö", "Herbert Kickl", "herbertkickl",
#'   "at", "övp", "TBD", NA,
#'   "at", "Grüne", "Werner Kogler", "wernerkogler"
#' )
#' create_call_grid(df = df,
#'                  platform = "fb",
#'                  country_var = "country",
#'                  party_var = "party",
#'                  name_var = "name",
#'                  handle_var = "handle",
#'                  filename_var = "filename",
#'                  start_date = "2023-08-25",
#'                  end_date = "2024-01-24",
#'                  unit = "quarter")
#'
#' @export
create_call_grid <- function(df = df,
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
                             drop_existing = FALSE
) {

  time_df <- slice_timeframes(start_date = start_date,
                              end_date = end_date,
                              unit = unit
  )

  # Prepare handle df
  handle_df <- drop_redundant(df = df, handle_var = handle_var)

  # Prepare filename in handle_df
  handle_df <- create_filename(df = handle_df,
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


  # # Create crowdtangle grid
  if(platform %in% c("fb", "ig")){

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





#' Checks for already downloaded JSON files and drops corresponding calls from the grid.
#'
#' This function checks for already downloaded JSON files in a specified path based on filename patterns.
#' It then updates the input grid to drop calls that correspond to existing JSON files.
#'
#' @param path The path where the JSON files are stored.
#' @param grid The data frame containing the calls to be filtered.
#' @param filename_var The variable name in the grid that contains the filenames (default: "filename").
#' @param recursive Logical indicating whether to search for files recursively in subdirectories (default: FALSE).
#'
#' @return Returns a modified grid data frame with calls corresponding to existing JSON files dropped.
#'
#' @export
drop_existing <- function(path,
                          grid,
                          filename_var = "filename",
                          recursive = FALSE) {
  if (dir.exists(path)[[1]]) {
    existing_jsons <- list.files(
      path = path,
      pattern = ".json$",
      recursive = recursive,
      full.names = TRUE,
      ignore.case = TRUE
    )
  }

  # Simplify filenames
  existing_jsons <- gsub("[^[:alnum:]]", "_", gsub("DL.*", "DL", gsub(".*ct_pull_", "", existing_jsons, perl = TRUE)))

  grid[["existing_json"]] <- gsub("[^[:alnum:]]", "_", grid[[filename_var]]) %in% existing_jsons

  cat(paste0("Dropping calls by filename, n = ", sum(grid$existing_json)))

  # Filter
  grid <- grid[grid$existing_json == FALSE, ]

  # Drop column
  grid <- grid[, !(names(grid) == "existing_json")]

  return(grid)
}




#' Create Update Grid
#'
#' This function generates an update grid for various platforms based on provided parameters.
#'
#' @param df The data frame containing relevant information for creating the update grid.
#' @param platform A character vector specifying the platforms to include in the update grid.
#'   Valid values are "fb" (Facebook), "ig" (Instagram), "tt" (TikTok), "yt" (YouTube), "tg" (Telegram), "bc" (BitChute), and "bs" (BitScope).
#' @param country_var The variable in the data frame representing country information.
#' @param party_var The variable in the data frame representing party information.
#' @param name_var The variable in the data frame representing name information.
#' @param filename_var The variable in the data frame representing filename information.
#' @param handle_var The variable in the data frame representing handle information.
#' @param start_date The start date for the update grid.
#' @param end_date The end date for the update grid.
#' @param name_sep The separator used in constructing filenames.
#' @param lowercase Logical indicating whether filenames should be converted to lowercase.
#' @param replace_non_ascii Logical indicating whether non-ASCII characters should be replaced in filenames.
#' @param filename_sep The separator used in filenames.
#' @param sortBy The variable used for sorting the update grid.
#' @param parse Logical indicating whether to parse the update grid.
#' @param data_path The path to the data.
#' @param count The count for the update grid.
#' @param drop_existing Logical indicating whether to drop existing files.
#'
#' @return A data frame containing the generated update grid.
#'
#' @details This function generates an update grid for specified platforms using the provided parameters.
#' It first finds the latest stored files, renames variables, merges timeframes, adds time information to filenames,
#' and creates the update grid based on the specified parameters.
#'
#' @export
create_update_grid <- function(df = df,
                               platform = c("fb", "ig", "tt", "yt", "tg", "bc", "bs"),
                               country_var = NULL,
                               party_var = NULL,
                               name_var = NULL,
                               filename_var = NULL,
                               handle_var = NULL,
                               start_date = NULL,
                               end_date = NULL,
                               name_sep = "-",
                               lowercase = TRUE,
                               replace_non_ascii = TRUE,
                               filename_sep = "_",
                               sortBy = "date",
                               parse = TRUE,
                               data_path = NULL,
                               count = Inf,
                               drop_existing = FALSE
) {

  # Find latest stored files
  latest_dt <- find_latest(data_path)

  # Rename variables
  data.table::setnames(latest_dt, "account_handle", handle_var)
  data.table::setnames(latest_dt, "date", "start_date")
  # Set now as end date, and latest found date as start_date, format into right format
  latest_dt[, start_datetime := start_date |> lubridate::format_ISO8601()]
  latest_dt[, end_datetime := lubridate::now() |> lubridate::format_ISO8601()]

  keep_cols <- colnames(latest_dt)[colnames(latest_dt) %in% c(handle_var,
                                                              "start_datetime",
                                                              "end_datetime")]
  latest_dt <- latest_dt[, keep_cols, with = FALSE]
  latest_df <- tibble::as_tibble(latest_dt)


  # Prepare handle df
  handle_df <- drop_redundant(df = df, handle_var = handle_var)

  # Prepare filename in handle_df
  handle_df <- create_filename(df = handle_df,
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

  # Merge timeframes
  grid_df <- dplyr::left_join(handle_df,
                       latest_df)


  # Replace empty start_datetime and end_dtatetime
  grid_df <- grid_df |> dplyr::mutate(start_datetime = ifelse(is.na(start_datetime),
                                                              lubridate::as_datetime(start_date) |> lubridate::format_ISO8601(),
                                                              start_datetime),
                                      end_datetime = ifelse(is.na(end_datetime),
                                                            lubridate::now() |> lubridate::format_ISO8601(),
                                                            end_datetime)
  )

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


  # # Create crowdtangle grid
  if(platform %in% c("fb", "ig")){

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


