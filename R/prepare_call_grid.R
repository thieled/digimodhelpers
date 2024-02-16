
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
#' @import dplyr
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





#' Slice Timeframes into Intervals
#'
#' This function slices timeframes into intervals specified by the user.
#'
#' @param start_date A character or Date object indicating the starting date of the timeframe. If NULL, it defaults to 1 week before today.
#' @param end_date A character or Date object indicating the ending date of the timeframe. If NULL, it defaults to today.
#' @param unit A character vector specifying the unit of the time intervals. Options include "day", "week", "month", "quarter", and "year". Default is "day".
#'
#' @return A data frame containing sliced timeframes with start and end dates, along with corresponding start and end datetimes.
#'
#' @examples
#' x <- as.Date("2023-01-01")
#' y <- as.Date("2025-11-20")
#' slice_timeframes(x, y, unit = "quarter")
#'
#' @export
slice_timeframes <- function(start_date = NULL,
                             end_date = NULL,
                             unit = c(
                               "day",
                               "week",
                               "month",
                               "quarter",
                               "year"
                             )
) {

  if (is.null(start_date)) {
    start_date <- lubridate::today() - 7
    cat("start_date is empty. replace by 1 week before today. \n")
  }

  start_date <- as.Date(start_date)

  if (start_date >= lubridate::today()) {
    start_date <- lubridate::today()
    cat("start_date is in the future. replace by 1 week before today. \n")
  }

  if (is.null(end_date)) {
    end_date <- lubridate::today()
    cat("end_date is empty. replace by today, 00:00. \n")
  }

  end_date <- as.Date(end_date)

  if (end_date > lubridate::today()) {
    end_date <- lubridate::today()
    cat("end_date is in the future or empty. replace by today, 00:00. \n")
  }

  # Get date sequence
  seq_dates <- seq(start_date, end_date, by = unit)

  last_day <- lubridate::ceiling_date(lubridate::ymd(seq_dates), unit = unit) - lubridate::days(1)

  if (last_day[which.max(last_day)] > lubridate::today()) last_day[which.max(last_day)] <- lubridate::today() # replace last day with today if later than today

  df <- data.frame(start_date = seq_dates,
                   start_datetime = paste0(seq_dates, "T00:00:00"),
                   end_date = last_day,
                   end_datetime = paste0(last_day, "T00:00:00"))

  return(df)

}





#' Create Call Grid for Social Media Data Collection
#'
#' This function creates a grid for social media data collection based on specified parameters.
#'
#' @param df A data frame containing the data.
#' @param platform A character vector specifying the platform(s) from which the data is collected. Default is c("fb", "ig", "tt", "yt", "tg", "bc", "bs").
#' @param country_var A character string specifying the column name in the data frame that contains country information. Default is NULL.
#' @param party_var A character string specifying the column name in the data frame that contains party information. Default is NULL.
#' @param name_var A character string specifying the column name in the data frame that contains name information. Default is NULL.
#' @param filename_var A character string specifying the column name in the data frame that contains filename information. Default is NULL.
#' @param handle_var A character string specifying the column name in the data frame that contains handle information. Default is NULL.
#' @param start_date A character or Date object indicating the starting date of the timeframe.
#' @param end_date A character or Date object indicating the ending date of the timeframe.
#' @param unit A character vector specifying the unit of the time intervals. Options include "day", "week", "month", "quarter", and "year". Default is "month".
#' @param name_sep A character string specifying the separator for name variables. Default is "-".
#' @param lowercase Logical; if TRUE, filenames will be converted to lowercase. Default is TRUE.
#' @param replace_non_ascii Logical; if TRUE, non-ASCII characters will be replaced in filenames. Default is TRUE.
#' @param filename_sep A character string specifying the separator for filename construction. Default is "_".
#'
#' @return A data frame containing the grid for social media data collection, including start and end dates, handles, and filenames.
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
                             filename_sep = "_"
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
  handle_df[["handle"]] <- handle_df[[handle_var]]

  grid_df <- expand.grid(list(start_date = time_df[["start_date"]],
                              handle = handle_df[[handle_var]])) |>
    dplyr::left_join(time_df) |>
    dplyr::left_join(handle_df)

  grid_df[["filename_new"]] <-  paste0(
    grid_df[[filename_var]],
    "_FROM_",
    sub("\\:", "m", sub("\\:", "h", grid_df[["start_datetime"]])),
    "_UNTIL_",
    sub("\\:", "m", sub("\\:", "h", grid_df[["end_datetime"]])),
    "_SCRAPED"
  )

  return(grid_df)
}





