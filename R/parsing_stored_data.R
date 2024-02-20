#' Parse filenames of JSON files containing social media data
#'
#' This function parses information stored in the filenames of JSON files containing collected social media data.
#'
#' @param path A character string specifying the directory path where the JSON files are located.
#' @param recursive Logical indicating whether to search for files recursively in subdirectories. Default is \code{FALSE}.
#'
#' @return A data frame containing parsed information from the filenames of the JSON files.
#'
#' @examples
#' parse_filenames(path = ".")
#'
#'
#' @export
parse_filenames <- function(path,
                            recursive = FALSE) {

  if (dir.exists(path)[[1]]) {
    full_filepath <- list.files(
      path = path,
      pattern = ".json$",
      recursive = recursive,
      full.names = TRUE,
      ignore.case = TRUE
    )
  }

  if (dir.exists(path)[[1]]) {
    existing_jsons <- list.files(
      path = path,
      pattern = ".json$",
      recursive = recursive,
      full.names = FALSE,
      ignore.case = TRUE
    )
  }

  # Drop the ct_pull prefix
  cleaned_filename <-  gsub(".*ct_pull_", "", existing_jsons, perl = TRUE)

  # Platform
  plat <- stringr::str_extract(cleaned_filename, pattern = "^(fb|ig|tt|yt|tg|bc|bs)")

  # From date
  from_date <- lubridate::as_date(stringr::str_extract(cleaned_filename, pattern = "(?<=FR_)\\d{4}[:punct:]\\d{2}[:punct:]\\d{2}"))

  # From time
  from_time <-  stringr::str_extract(cleaned_filename, pattern = "(?<=FR_\\d{4}[:punct:]\\d{2}[:punct:]\\d{2}[T|[:punct:]])\\d{2}[h|[:punct:]]\\d{2}[m|[:punct:]]\\d{2}") |>
    stringr::str_replace("[h|[:punct:]]", "\\:") |>
    stringr::str_replace_all("[m|[:punct:]]", "\\:")

  # From datetime
  from_datetime <- stringr::str_c(from_date, from_time, sep = "T") |>
    lubridate::as_datetime() |>
    lubridate::format_ISO8601()

  # To date
  to_date <- lubridate::as_date(stringr::str_extract(cleaned_filename, pattern = "(?<=TO_)\\d{4}[:punct:]\\d{2}[:punct:]\\d{2}"))

  # To time
  to_time <-  stringr::str_extract(cleaned_filename, pattern = "(?<=TO_\\d{4}[:punct:]\\d{2}[:punct:]\\d{2}[T|[:punct:]])\\d{2}[h|[:punct:]]\\d{2}[m|[:punct:]]\\d{2}") |>
    stringr::str_replace("[h|[:punct:]]", "\\:") |>
    stringr::str_replace_all("[m|[:punct:]]", "\\:")

  # To datetime
  to_datetime <- stringr::str_c(to_date, to_time, sep = "T") |>
    lubridate::as_datetime() |>
    lubridate::format_ISO8601()

  # Download date
  dl_date <- lubridate::as_date(stringr::str_extract(cleaned_filename, pattern = "(?<=DL_)\\d{4}[:punct:]\\d{2}[:punct:]\\d{2}"))

  # Download time
  dl_time <-  stringr::str_extract(cleaned_filename, pattern = "(?<=DL_\\d{4}[:punct:]\\d{2}[:punct:]\\d{2}[T|[:punct:]])\\d{2}[h|[:punct:]]\\d{2}[m|[:punct:]]\\d{2}") |>
    stringr::str_replace("[h|[:punct:]]", "\\:") |>
    stringr::str_replace_all("[m|[:punct:]]", "\\:")

  # Download datetime
  dl_datetime <- stringr::str_c(dl_date, dl_time, sep = "T") |>
    lubridate::as_datetime() |>
    lubridate::format_ISO8601()

  # Country
  country <- stringr::str_extract(cleaned_filename, pattern = "(?<=(fb|ig|tt|yt|tg|bc|bs)_)\\w{2}")

  # Country, Party, Person string
  person <- stringr::str_extract(cleaned_filename, pattern = "(?<=(fb|ig|tt|yt|tg|bc|bs)_).*(?=_FR)")

  # Create data.frame
  df <- tibble::tibble(
    plat,
    country,
    person,
    from_date,
    from_datetime,
    to_date,
    to_datetime,
    dl_date,
    dl_datetime,
    full_filepath
  )

  return(df)
}
