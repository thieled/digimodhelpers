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

