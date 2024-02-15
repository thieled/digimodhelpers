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
    grid_df[["start_date"]],
    "_UNTIL_",
    grid_df[["end_date"]],
    "_SCRAPED"
  )

  return(grid_df)
}
