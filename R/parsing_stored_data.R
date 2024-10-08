#' Parse filenames of JSON files containing social media data
#'
#' This function parses information stored in the filenames of JSON files containing collected social media data.
#'
#' @param path A character string specifying the directory path where the JSON files are located.
#' @param filepaths A character vector of file paths to JSON files. If specified, overrides the `path` parameter.
#' @param recursive Logical indicating whether to search for files recursively in subdirectories. Default is \code{FALSE}.
#'
#' @return A data frame containing parsed information from the filenames of the JSON files.
#'
#' @export
parse_filenames <- function(path = NULL,
                            filepaths = NULL,
                            recursive = FALSE) {


  # Check if either dir or filepaths are provided
  if (is.null(path) && is.null(filepaths)) {
    stop("Either 'path' or 'filepaths' must be provided.")
  }

  # Check if directory exists
  if (!is.null(path) && !dir.exists(path)) {
    stop(paste0("There is no such directory ", path))
  }


  # Call "parse filenames" function from digimodhelpers - extract info from json filenames
  if (!is.null(path)) {

    # Get full filepath
    if (dir.exists(path)[[1]]) {
      full_filepath <- list.files(
        path = path,
        pattern = ".json$",
        recursive = recursive,
        full.names = TRUE,
        ignore.case = TRUE
      )
    }

    # Get filename only
    if (dir.exists(path)[[1]]) {
      existing_jsons <- list.files(
        path = path,
        pattern = ".json$",
        recursive = recursive,
        full.names = FALSE,
        ignore.case = TRUE
      )
    }

    files_df <- parse_filename_strings(filepaths = existing_jsons)
    files_df$full_filepath <- full_filepath


  } else {

    files_df <- parse_filename_strings(filepaths = filepaths)

 }

  return(files_df)

}



#' Parse Filename Strings to Extract Metadata
#'
#' This function parses a vector of filenames to extract various metadata components such as platform, dates, times, country, and person/party information. The metadata is returned in a tidy data frame.
#'
#' @param filepaths A character vector of file paths or filenames to be parsed.
#'
#' @return A tibble with the following columns:
#' \describe{
#'   \item{\code{plat}}{A character vector representing the platform (e.g., "fb", "ig", "tt", "yt", "tg", "bc", "bs").}
#'   \item{\code{country}}{A character vector representing the country code extracted from the filename.}
#'   \item{\code{person}}{A character vector representing the country_party_account extracted from the filename.}
#'   \item{\code{party}}{A character vector representing the party extracted from the filename.}
#'   \item{\code{account_owner}}{A character vector representing the account extracted from the filename.}
#'   \item{\code{from_date}}{A Date vector representing the start date extracted from the filename.}
#'   \item{\code{from_datetime}}{A character vector representing the start datetime in ISO8601 format.}
#'   \item{\code{to_date}}{A Date vector representing the end date extracted from the filename.}
#'   \item{\code{to_datetime}}{A character vector representing the end datetime in ISO8601 format.}
#'   \item{\code{dl_date}}{A Date vector representing the download date extracted from the filename.}
#'   \item{\code{dl_datetime}}{A character vector representing the download datetime in ISO8601 format.}
#'   \item{\code{basenames}}{A character vector of the base names of the filenames.}
#'   \item{\code{full_filepath}}{A character vector of the full file paths provided as input.}
#' }
#'
#'
#' @export

parse_filename_strings <- function(filepaths){

  basenames <-basename(filepaths)

  # Drop the ct_pull prefix
  cleaned_filename <- gsub(".*ct_pull_", "", basenames, perl = TRUE)

  # Platform
  plat <- stringr::str_extract(cleaned_filename, pattern = "^(fb|ig|tt|yt|tg|bc|bs)")

  # From date
  from_date <- lubridate::as_date(stringr::str_extract(cleaned_filename, pattern = "(?<=FR_)\\d{4}[:punct:]\\d{2}[:punct:]\\d{2}"))

  # From time
  from_time <- stringr::str_extract(cleaned_filename, pattern = "(?<=FR_\\d{4}[:punct:]\\d{2}[:punct:]\\d{2}[T|[:punct:]])\\d{2}[h|[:punct:]]\\d{2}[m|[:punct:]]\\d{2}") |>
    stringr::str_replace("[h|[:punct:]]", "\\:") |>
    stringr::str_replace_all("[m|[:punct:]]", "\\:")

  # From datetime
  from_datetime <- stringr::str_c(from_date, from_time, sep = "T") |>
    lubridate::as_datetime() |>
    lubridate::format_ISO8601()

  # To date
  to_date <- lubridate::as_date(stringr::str_extract(cleaned_filename, pattern = "(?<=TO_)\\d{4}[:punct:]\\d{2}[:punct:]\\d{2}"))

  # To time
  to_time <- stringr::str_extract(cleaned_filename, pattern = "(?<=TO_\\d{4}[:punct:]\\d{2}[:punct:]\\d{2}[T|[:punct:]])\\d{2}[h|[:punct:]]\\d{2}[m|[:punct:]]\\d{2}") |>
    stringr::str_replace("[h|[:punct:]]", "\\:") |>
    stringr::str_replace_all("[m|[:punct:]]", "\\:")

  # To datetime
  to_datetime <- stringr::str_c(to_date, to_time, sep = "T") |>
    lubridate::as_datetime() |>
    lubridate::format_ISO8601()

  # Download date
  dl_date <- lubridate::as_date(stringr::str_extract(cleaned_filename, pattern = "(?<=DL_)\\d{4}[:punct:]\\d{2}[:punct:]\\d{2}"))

  # Download time
  dl_time <- stringr::str_extract(cleaned_filename, pattern = "(?<=DL_\\d{4}[:punct:]\\d{2}[:punct:]\\d{2}[T|[:punct:]])\\d{2}[h|[:punct:]]\\d{2}[m|[:punct:]]\\d{2}") |>
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

  # Party and account
  regpat <- "^.*?_(.*?)_(.*)$"

  # Use regmatches and regexec to extract the matches
  matches <- regmatches(person, regexec(regpat, person))

  # Extract the party and account parts
  party <- sapply(matches, function(x) ifelse(length(x) > 1, x[2], ""))
  account_owner <- sapply(matches, function(x) ifelse(length(x) > 2, x[3], ""))

  # Create data.frame
  df <- tibble::tibble(
    plat,
    country,
    person,
    party,
    account_owner,
    from_date,
    from_datetime,
    to_date,
    to_datetime,
    dl_date,
    dl_datetime,
    basenames,
    full_filepath = filepaths
  )

  return(df)
}







#' Parse Latest Data
#'
#' This function parses the latest data from specified file paths and extracts relevant information.
#' It supports parsing data from different platforms like Instagram, Facebook, and YouTube.
#'
#' @param path The path to the directory containing the data files.
#' @param cleanup Logical. Should the directory `path` checked for corrupt jsons and error jsons before parsing? Slows up process.
#' @return A data.table containing the parsed data.
#'
#' @details
#' This function first calls the "parse filenames" function from the `digimodhelpers` package to extract information from JSON filenames.
#' Then, it identifies the latest datetime based on end date and download date.
#' Next, it parses the data from CrowdTangle or YouTube depending on the platform, and extracts required columns.
#' Finally, it renames specific columns to standardize column names across platforms.
#'
#' @import data.table
#'
#' @export
#'
parse_latest <- function(path,
                         cleanup = FALSE) {


  # Check path for invalid .jsons - and move invalid ones into subfolder
  if(cleanup) remove_invalid_jsons(path)

  # Call "parse filenames" function from digimodhelpers - extract info from json filenames
  files_df <- parse_filenames(path)

  if(cleanup) {
        # Remove error files from ct path
        if(files_df[["plat"]][[1]] %in% c("ig", "fb")){
          remove_error_jsons(path)
          # And call parse_filenames again
          files_df <- parse_filenames(path)
        }
  }

  # set datetime as datetime
  files_df[["to_datetime"]] <- lubridate::as_datetime(files_df[["to_datetime"]])
  files_df[["dl_datetime"]] <- lubridate::as_datetime(files_df[["dl_datetime"]])

  # Convert df to data.table
  data.table::setDT(files_df)

  # Find the latest datetime - by end date and download date
  latest_dt <- files_df[, .SD[to_datetime == max(to_datetime)], by = person]
  latest_dt <- latest_dt[, .SD[dl_datetime == max(dl_datetime)], by = person]

  # extract the file paths
  f <- latest_dt[["full_filepath"]]


  ###  Parse data from crowdtangle

  if(files_df[["plat"]][[1]] %in% c("ig", "fb")){


    # Parse and bind all latest jsons
    file_dt <- data.table::rbindlist( # bind as data.table
      out <- RcppSimdJson::fload(f, # use super-fast RcppSimdJson parser
                                 empty_array = data.frame(), # define what to do with empty observations
                                 empty_object = data.frame()
      ) |>
        purrr::map2(c("result"), `[[`) |> # extract "result" element from parsed json list
        purrr::map2(c("posts"), `[[`), # extract "posts"
      use.names = TRUE,
      fill = TRUE,
      idcol = "file" # stores the filename
    )

    # Unlist 'account' column
    file_dt[, account := purrr::map(file_dt[, account], ~ unlist(.x))]

    # Unnest 'account' column
    file_dt <- tidytable::unnest_wider(file_dt,
                                       account,
                                       names_sep = "_",
                                       names_repair = "minimal")

    # Extract FB account ID from account_url;
    # NOTE: the original 'account_id' variable seems to be specific to CT
    file_dt[, account_id := gsub(".*\\/(\\d+)$", "\\1", account_url)]


    # Define columns to keep
    keep_cols <- colnames(file_dt)[colnames(file_dt) %in% c(
      "file",
      "date",
      "account_id",
      "account_handle",
      "platformId"
    )]

    # Drop unneccessary columns
    file_dt <- file_dt[, keep_cols, with = FALSE]

    names(file_dt)

    # Columns to rename
    old_names <- c(
      "platformId",
      "date"
    )
    new_names <- c("item_id",
                   "published_time")

    # Rename columns
    data.table::setnames(file_dt, old_names, new_names)

    ## Note for later: Download time info can be merged from filenames, stored in files_df
    ## TO DO: Store dl time in .json? - Problem: Would need to change crowdtangler again

  }


  ### Parse data from youtube
  if(files_df[["plat"]][[1]] %in% c("yt")){


    # Parse and bind jsons
    file_dt <- data.table::rbindlist(
      RcppSimdJson::fload(f,
                          empty_array = data.frame(),
                          empty_object = data.frame()),
      fill = TRUE, use.names = T, idcol = "file")

    # Using map to filter elements with length greater than 1
    file_dt[, snippet := purrr::map(file_dt[, snippet], ~Filter(function(x) length(x) == 1, .x))]

    # Unlist 'snippet' column
    file_dt[, snippet := purrr::map(file_dt[, snippet], ~ unlist(.x))]

    # Unnest 'snippet' column
    file_dt <- tidytable::unnest_wider(file_dt,
                                       snippet,
                                       names_sep = "_",
                                       names_repair = "minimal")

    # Define cols to keep
    required_cols <- c(
      "file",
      "download_time",
      "id",
      "snippet_publishedAt",
      "snippet_channelId"
    )

    keep_cols <- colnames(file_dt)[colnames(file_dt) %in% required_cols]

    # Subset data.table
    file_dt <- file_dt[, keep_cols, with = FALSE]

    # Columns to rename
    old_names <- c(
      "id",
      "snippet_publishedAt",
      "snippet_channelId")
    new_names <- c("item_id",
                   "published_time",
                   "account_id")

    # Rename columns
    data.table::setnames(file_dt, old_names, new_names)

  }


  return(file_dt)
}



#' Find the latest file for each account in the specified path
#'
#' This function parses the latest files for each account found in the specified path.
#' It returns a data.table containing the latest file for each unique account.
#'
#' @param path A character string specifying the directory path where the files are located.
#' @param cleanup Logical. Should the directory `path` checked for corrupt jsons and error jsons before parsing? Slows up process.
#' @return A data.table with columns representing account details and the latest file information.
#' @export
find_latest <- function(path,
                        cleanup = FALSE) {
  if (!dir.exists(path)) {
    stop("Path does not exist. Please provide a valid path.")
  }

  # Parse the latest files for each account
  dt <- parse_latest(path, cleanup = cleanup)

  # Convert "date" to POSIXct if it's not already
  if (!inherits(dt$published_time, "POSIXct")) {
    dt[, published_time := as.POSIXct(lubridate::as_datetime(published_time, tz = "UTC"))]
  }

  # Order the data.table by date
  data.table::setorder(dt, account_id, published_time)

  # Group by account_handle and select the row with the maximum date for each group
  result <- dt[, .SD[which.max(published_time)], by = account_id]

  return(result)

}






#' Parse data from specified directory
#'
#' This function parses data from the specified directory. It first checks if the directory exists. If not, it throws an error.
#' Then, it removes invalid JSON files from the directory, if any. Next, it parses the filenames and extracts information
#' from them. Depending on the platform (Facebook, Instagram, or YouTube), it parses the data accordingly using the
#' appropriate methods.
#'
#' @param dir A character string specifying the directory from which data needs to be parsed. Default is NULL.
#' @param filepaths A character vector specifying the filepaths to be parsed. Default is NULL.
#' @param cleanup Logical. Should the directory `path` checked for corrupt jsons and error jsons before parsing? Slows up process.
#'
#' @return A data.table containing parsed data from the specified directory or filepaths.
#'
#' @export
parse_data <- function(dir = NULL, filepaths = NULL, cleanup = FALSE) {

  # Check if either dir or filepaths are provided
  if (is.null(dir) && is.null(filepaths)) {
    stop("Either 'dir' or 'filepaths' must be provided.")
  }

  # Check if directory exists
  if (!is.null(dir) && !dir.exists(dir)) {
    stop(paste0("There is no such directory ", dir))
  }

  # Check and remove invalid JSONs
  if(cleanup) remove_invalid_jsons(dir = dir, filepaths = filepaths)

  # Call "parse filenames" function from digimodhelpers - extract info from json filenames
  if (!is.null(dir)) {
    files_df <- parse_filenames(path = dir)
  } else {
    files_df <- parse_filename_strings(filepaths = filepaths)
  }

  # extract the file paths
  f <- files_df[["full_filepath"]]
  names(f) <- f

  ###  Parse data from crowdtangle

  if(files_df[["plat"]][[1]] %in% c("ig", "fb")){

    # Parse and bind all latest jsons
    file_dt <- data.table::rbindlist( # bind as data.table
      out <- RcppSimdJson::fload(f, # use super-fast RcppSimdJson parser
                                 empty_array = data.frame(), # define what to do with empty observations
                                 empty_object = data.frame()
      ) |>
        purrr::map2(c("result"), `[[`) |> # extract "result" element from parsed json list
        purrr::map2(c("posts"), `[[`), # extract "posts"
      use.names = TRUE,
      fill = TRUE,
      idcol = "filepath" # stores the filename
    )

    # Store filename
    file_dt[, file := basename(filepath)]

    # Unlist 'account' column
    file_dt[, account := purrr::map(file_dt[, account], ~ unlist(.x))]

    # Unnest 'account' column
    file_dt <- tidytable::unnest_wider(file_dt,
                                       account,
                                       names_sep = "_",
                                       names_repair = "minimal")


    # Unlist 'media' column
    file_dt[, statistics := purrr::map(file_dt[, statistics], ~ unlist(.x))]

    # Unnest 'account' column
    file_dt <- tidytable::unnest_wider(file_dt,
                                       statistics,
                                       names_sep = "_",
                                       names_repair = "minimal")


    # Extract FB account ID from account_url;
    # NOTE: the original 'account_id' variable seems to be specific to CT
    file_dt[, account_id := gsub(".*\\/(\\d+)$", "\\1", account_url)]


    # # Define columns to keep
    # keep_cols <- colnames(file_dt)[colnames(file_dt) %in% c(
    #   "file",
    #   "filepath",
    #   "date",
    #   "account_id",
    #   "account_handle",
    #   "platformId"
    # )]

    # Drop unneccessary columns
    #  file_dt <- file_dt[, keep_cols, with = FALSE]


    # Columns to rename
    old_names <- c(
      "platformId",
      "date"
    )
    new_names <- c("item_id",
                   "published_time")

    # Rename columns
    data.table::setnames(file_dt, old_names, new_names)

    ## Note for later: Download time info can be merged from filenames, stored in files_df
    ## TO DO: Store dl time in .json? - Problem: Would need to change crowdtangler again

  }


  ### Parse data from youtube
  if(files_df[["plat"]][[1]] %in% c("yt")){

    # Parse and bind jsons
    file_dt <- data.table::rbindlist(
      RcppSimdJson::fload(f,
                          empty_array = data.frame(),
                          empty_object = data.frame()),
      fill = TRUE, use.names = T,
      idcol = "filepath" # stores the filename
    )

    # Store filename
    file_dt[, file := basename(filepath)]

    ## Tags
    # Function to safely extract and collapse tags
    extract_and_collapse_tags <- function(snippet) {
      tags <- snippet[["tags"]]
      if (is.null(tags)) {
        return(NA_character_)
      } else {
        return(paste(tags, collapse = ", "))
      }
    }
    # Extract the collapsed tags and create the new column
    file_dt[, tags := purrr::map_chr(snippet, extract_and_collapse_tags)]

    # Using map to filter elements with length greater than 1
    file_dt[, snippet := purrr::map(file_dt[, snippet], ~Filter(function(x) length(x) == 1, .x))]

    ## Rest of snippet column

    # Unlist 'snippet' column
    file_dt[, snippet := purrr::map(file_dt[, snippet], ~ unlist(.x))]

    # Unnest 'snippet' column
    file_dt <- tidytable::unnest_wider(file_dt,
                                       snippet,
                                       names_sep = "_",
                                       names_repair = "minimal")
    ## Statistics column

    # Unlist 'statistics' column
    file_dt[, statistics := purrr::map(file_dt[, statistics], ~ unlist(.x))]

    # Unnest 'snippet' column
    file_dt <- tidytable::unnest_wider(file_dt,
                                       statistics,
                                       names_sep = "_",
                                       names_repair = "minimal")


    #names(file_dt)

    # Define cols to keep
    required_cols <- c(
      "file",
      "filepath",
      "kind",
      "etag",
      "id",

      "tags",

      "snippet_publishedAt",
      "snippet_channelId",
      "snippet_title",
      "snippet_description",
      "snippet_channelTitle",
      "snippet_categoryId",
      "snippet_liveBroadcastContent",
      "snippet_defaultLanguage",
      "snippet_defaultAudioLanguage",
      "statistics_viewCount",
      "statistics_likeCount",
      "statistics_favoriteCount",
      "statistics_commentCount",

      "download_time",
      "download_time_zone"
    )

    keep_cols <- colnames(file_dt)[colnames(file_dt) %in% required_cols]

    # Subset data.table
    file_dt <- file_dt[, keep_cols, with = FALSE]

    # Columns to rename
    old_names <- c(
      "id",
      "snippet_publishedAt",
      "snippet_channelId",
      "snippet_title",
      "snippet_description",
      "snippet_channelTitle",
      "snippet_categoryId",
      "snippet_liveBroadcastContent",
      "snippet_defaultLanguage",
      "snippet_defaultAudioLanguage",
      "statistics_viewCount",
      "statistics_likeCount",
      "statistics_favoriteCount",
      "statistics_commentCount"
    )

    new_names <- c("item_id",

                   "published_at",
                   "channel_id",
                   "title",
                   "description",
                   "channel_title",
                   "category_id",
                   "live_broadcast",
                   "default_language",
                   "default_audio_language",
                   "view_count",
                   "like_count",
                   "favorite_count",
                   "comment_count"
    )

    # Rename columns
    data.table::setnames(file_dt, old_names, new_names)

  }


  return(file_dt)
}









#' Parse Comment Filenames
#'
#' This function parses filenames of JSON files containing comments from various platforms.
#' It extracts metadata such as platform identifier, download date, download time, and item ID.
#'
#' @param path A character string specifying the directory path where JSON files are located. Default is `NULL`.
#' @param recursive A logical value indicating whether to search for files recursively within subdirectories. Default is `FALSE`.
#'
#' @return A `tibble` containing the following columns:
#' \describe{
#'   \item{item_id}{The extracted item ID from the filename.}
#'   \item{plat}{The platform identifier extracted from the filename (e.g., `fb`, `ig`, `tt`, `yt`, `tg`, `bc`, `bs`).}
#'   \item{is_comment}{Logical. Indicating that file contains comments.}
#'   \item{full_filepath}{The full file path of each JSON file.}
#'   \item{filenames}{The base filename of each JSON file.}
#'   \item{dirname}{The name of the folder in which the JSONs are stored.}
#' }
#'
#' @details The function checks if the provided directory path exists and then lists all JSON files in the directory (and subdirectories if `recursive` is `TRUE`). It extracts the platform identifier from the beginning of the filename, cleans up the filename to extract the download date and time, and constructs a download datetime. It also extracts the item ID from the cleaned filename.
#'
#' @export
parse_comment_filenames <- function(path = NULL, recursive = FALSE) {

  # Check if either dir or filepaths are provided
  if (is.null(path)) {
    stop("No 'path' provided.")
  }

  # Check if directory exists
  if (!is.null(path) && !dir.exists(path)) {
    stop(paste0("There is no such directory ", path))
  }

  # Get full filepath
  if (dir.exists(path)[[1]]) {
    full_filepath <- list.files(
      path = path,
      pattern = ".json$",
      recursive = recursive,
      full.names = TRUE,
      ignore.case = TRUE
    )
  }

  # Get the filename
  filenames <- basename(full_filepath)

  # Get name of folder
  dirname <- basename(dirname(full_filepath))


  # Platform
  plat <- stringr::str_extract(filenames, pattern = "^(fb|ig|tt|yt|tg|bc|bs)")

  # Comment y/n
  is_comment <- stringr::str_detect(filenames, ".*comm_")

  # Drop the prefix
  cleaned_filenames <- gsub(".*comm_", "", filenames, perl = TRUE)

  # Download date
  dl_date <- lubridate::as_date(stringr::str_extract(cleaned_filenames, pattern = "(?<=DL_)\\d{4}[:punct:]\\d{2}[:punct:]\\d{2}"))

  # Download time
  dl_time <- stringr::str_extract(cleaned_filenames, pattern = "(?<=DL_\\d{4}[:punct:]\\d{2}[:punct:]\\d{2}[T|[:punct:]])\\d{2}[h|[:punct:]]\\d{2}[m|[:punct:]]\\d{2}") |>
    stringr::str_replace("[h|[:punct:]]", "\\:") |>
    stringr::str_replace_all("[m|[:punct:]]", "\\:")

  # Download datetime
  dl_datetime <- stringr::str_c(dl_date, dl_time, sep = "T") |>
    lubridate::as_datetime(tz = "UTC") |>
    lubridate::format_ISO8601()

  item_id <- stringr::str_extract(cleaned_filenames, pattern = ".+(?=_DL_\\d{4}.+)")

  # Create data.frame
  df <- tibble::tibble(
    item_id,
    plat,
    is_comment,
    full_filepath,
    filenames,
    dirname
  )

  return(df)
}




#' Parse and Unnest YouTube Comment Data
#'
#' This function parses and unnests YouTube comment data from JSON files.
#' It extracts relevant fields from the comments and returns a `data.table`
#' with one row per top-level comment.
#'
#' @param dir A character vector specifying the directory containing JSON files.
#'        Either 'dir' or 'filepaths' must be provided.
#' @param cleanup Logical. Should the directory `path` checked for corrupt jsons and error jsons before parsing? Slows up process.
#' @param filepaths NOT SUPPORTED YET: A character vector specifying the filepaths to be parsed. Default is NULL.
#' @return A `data.table` with the parsed and unnested YouTube comment data.
#'         The resulting table includes columns for the comment text, author details,
#'         video information, and additional metadata.
#'
#' @export
parse_yt_comments <- function(dir = NULL, filepaths = NULL, cleanup = FALSE) {

  # Check if either dir or filepaths are provided
  if (is.null(dir) && is.null(filepaths)) {
    stop("Either 'dir' or 'filepaths' must be provided.")
  }

  # Check if directory exists
  if (!is.null(dir) && !dir.exists(dir)) {
    stop(paste0("There is no such directory ", dir))
  }

  # Check and remove invalid JSONs
  if(cleanup) remove_invalid_jsons(dir = dir, filepaths = filepaths)

  # Call "parse filenames" function from digimodhelpers - extract info from json filenames
  if (!is.null(dir)) {
    safely_parse_comment_fn <- purrr::safely(parse_comment_filenames, otherwise = NULL)
    files_df <- safely_parse_comment_fn(dir)$result
  }

  # extract the file paths
  f <- files_df[["full_filepath"]]
  names(f) <- f


  ### Parse data from youtube
  # NOTE ADD CHECK AUTOMATIC RECOGNITION OF COMMENT DATA
  if(files_df[["plat"]][[1]] %in% c("yt")){

    file_dt <- data.table::rbindlist(
      RcppSimdJson::fload(f,
                          empty_array = data.frame(),
                          empty_object = data.frame()),
      fill = TRUE, use.names = TRUE, idcol = "filepath"
    )

    # Create unique ID - filepath + etag
    file_dt[, file_id := stringr::str_c(filepath, "_etag_", etag)]

    # Get filename
    file_dt[, filename := basename(filepath)]

    # Get time_window
    file_dt[, time_window := basename(dirname(filepath))]

    # Extract download time info
    dlinfo_dt <- file_dt[, .(filepath,
                             file_id,
                             filename,
                             time_window,
                             download_time,
                             download_time_zone,
                             download_software)]
    dlinfo_dt <- unique(dlinfo_dt, by = "file_id")


    # Exctract items column as named list
    items_l <- file_dt$items
    names(items_l) <- file_dt$file_id

    # rowbind
    items_dt <- data.table::rbindlist(items_l, use.names = T, fill = T, idcol = "file_id")

    # Unlist 'snippet' column
    items_dt[, snippet := purrr::map(items_dt[, snippet], ~ unlist(.x))]

    # Unnest 'snippet' column
    items_dt <- tidytable::unnest_wider(items_dt,
                                        snippet,
                                        names_sep = "_",
                                        names_repair = "minimal")


    ## Note: There are replies, which I won't unnest for the moment

    # Keep them separate
    replies_dt <- items_dt[, .(id, replies)]
    replies_dt[, id := unlist(id)]

    # Define columns to keep
    required_cols <- c(
      "file_id",
      "id",
      "snippet_channelId",
      "snippet_topLevelComment.snippet.videoId",
      "snippet_topLevelComment.snippet.textDisplay",
      "snippet_topLevelComment.snippet.textOriginal",
      "snippet_topLevelComment.snippet.authorDisplayName",
      "snippet_topLevelComment.snippet.authorProfileImageUrl",
      "snippet_topLevelComment.snippet.authorChannelUrl",
      "snippet_topLevelComment.snippet.authorChannelId.value",
      "snippet_topLevelComment.snippet.likeCount",
      "snippet_topLevelComment.snippet.viewerRating",
      "snippet_topLevelComment.snippet.canRate",
      "snippet_topLevelComment.snippet.updatedAt",
      "snippet_canReply",
      "snippet_totalReplyCount"
    )

    keep_cols <- colnames(items_dt)[colnames(items_dt) %in% required_cols]

    # Subset data.table
    items_dt <- items_dt[, ..keep_cols]

    # Convert list columns to appropriate atomic types
    items_dt <- items_dt[, lapply(.SD, function(col) {
      if (is.list(col)) {
        col <- unlist(col, recursive = FALSE)
        # Convert to atomic vector if possible
        col <- tryCatch(as.numeric(col), warning = function(w) as.character(col), error = function(e) as.character(col))
      }
      return(col)
    }), .SDcols = names(items_dt)]


    # Deduplicate dts
    items_dt <- unique(items_dt, by = "id")

    # Merge replies
    items_dt <- merge(items_dt, replies_dt, by = "id", all.x = T)

    # Columns to rename
    old_names <- c(
      "id",
      "replies",
      "snippet_channelId",
      "snippet_topLevelComment.snippet.videoId",
      "snippet_topLevelComment.snippet.textDisplay",
      "snippet_topLevelComment.snippet.textOriginal",
      "snippet_topLevelComment.snippet.authorDisplayName",
      "snippet_topLevelComment.snippet.authorProfileImageUrl",
      "snippet_topLevelComment.snippet.authorChannelUrl",
      "snippet_topLevelComment.snippet.authorChannelId.value",
      "snippet_topLevelComment.snippet.likeCount",
      "snippet_topLevelComment.snippet.viewerRating",
      "snippet_topLevelComment.snippet.canRate",
      "snippet_topLevelComment.snippet.updatedAt",
      "snippet_canReply",
      "snippet_totalReplyCount"
    )

    new_names <- c(
      "c_id",
      "c_replies",
      "v_channel_id",
      "v_video_id",
      "c_text_display",
      "c_text_original",
      "c_author_name",
      "c_author_profile_img",
      "c_author_url",
      "c_author_id",
      "c_like_count",
      "c_viewer_rating",
      "c_can_rate",
      "c_updated_time",
      "c_can_reply",
      "c_n_replies"
    )

    # Rename columns
    setnames(items_dt, old_names, new_names)

    # Merge DL info
    items_dt <- merge(items_dt, dlinfo_dt, by = "file_id", all.x = T)

  }

  return(items_dt)

}


