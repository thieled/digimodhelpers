# Eliminating 'no visible binding' note
utils::globalVariables(c(
  "person",
  "dl_datetime",
  "to_datetime",
  ".data",
  "start_datetime",
  "end_datetime",
  "account_handle",
  "contentDetails.videoPublishedAt",
  "account",
  "account_id",
  "account_url",
  "published_time",
  "items",
  "doubts_var",
  "handle",
  "list_id",
  "id",
  "snippet",
  "valid_json",
  "tags",
  "statistics",
  "download_time",
  "download_time_zone",
  "topLevelComment"
))




#' Remove Invalid JSON Files
#'
#' This function removes invalid JSON files by either moving them to a "corrupted_files" sub-directory
#' or raising an error if no JSON files are found or if the directory does not exist.
#'
#' @param dir (character) Path to the directory containing JSON files.
#' @param filepaths (character) Vector of file paths to specific JSON files. Default is NULL.
#' @return NULL
#' @details The function checks if either a directory containing JSON files ('dir') or direct file paths to JSON files ('filepaths') are provided. It then proceeds to validate the JSON files using RcppSimdJson::validateJSON. Invalid JSON files are moved to a "corrupted_files" sub-directory created in the same directory as the original file.
#'
#' @export
remove_invalid_jsons <- function(dir = NULL, filepaths = NULL) {

  # Check if either dir or filepaths are provided
  if (is.null(dir) && is.null(filepaths)) {
    stop("Either 'dir' or 'filepaths' must be provided.")
  }

  # Check if directory exists
  if (!is.null(dir) && !dir.exists(dir)) {
    stop("Directory does not exist.")
  }

  # Initialize vector to store JSON paths
  json_paths <- character(0)

  # If filepaths argument is provided, append them to json_paths
  if (!is.null(filepaths)) {
    json_paths <- c(json_paths, filepaths)
  }

  # Find JSON files in the directory
  if (!is.null(dir)) {
    json_paths <- c(json_paths, Sys.glob(file.path(dir, "*.json")))
  }

  # Check if any JSON files found
  if (length(json_paths) == 0) {
    stop("No JSON files found.")
  }

  # Create a data frame to store JSON file paths
  json_df <- data.frame(json_paths = json_paths)

  # Function to safely validate JSON files
  validate_safely <- purrr::safely(RcppSimdJson::validateJSON, otherwise = FALSE)

  # Validate JSON files and store the results
  json_df$valid_json <- purrr::map_lgl(json_df$json_paths, function(x) {
    r <- validate_safely(x)
    return(r$result)
  })

  # Find invalid JSON files
  if (any(json_df$valid_json == FALSE)) {
    # Create 'corrupted_files' sub-directory if it does not exist
    purrr::walk(json_df$json_paths[!json_df$valid_json], function(file_path) {
      parent_dir <- dirname(file_path)
      corrupted_dir <- file.path(parent_dir, "corrupted_files")
      if (!dir.exists(corrupted_dir)) {
        dir.create(corrupted_dir, recursive = TRUE)
      }
      # Move invalid file to 'corrupted_files' sub-directory
      file.rename(file_path, file.path(corrupted_dir, basename(file_path)))
    })

    # Warning message
    warning(paste0("Invalid .json files detected. Moved to 'corrupted_files' sub-directory."))
  }
}




#' Remove JSON Files with Error Status
#'
#' This function removes JSON files with error status (status != 200) by either moving them to a "corrupted_files" sub-directory
#' or raising an error if no JSON files are found or if the directory does not exist.
#'
#' @param dir (character) Path to the directory containing JSON files.
#' @param filepaths (character) Vector of file paths to specific JSON files.
#' @return NULL
#' @details The function checks if either a directory containing JSON files ('dir') or direct file paths to JSON files ('filepaths') are provided. It then proceeds to load the JSON files and checks if they have an error status. Files with error status are moved to a "corrupted_files" sub-directory created in the same directory as the original file.
#'
#' @examples
#' # Remove JSON files with error status from a directory
#' remove_error_jsons("path/to/directory")
#'
#' # Remove JSON files with error status using specific file paths
#' remove_error_jsons(filepaths = c("path/to/file1.json", "path/to/file2.json"))
#'
#' @export
remove_error_jsons <- function(dir = NULL, filepaths = NULL) {
  # Check if either dir or filepaths are provided
  if (is.null(dir) && is.null(filepaths)) {
    stop("Either 'dir' or 'filepaths' must be provided.")
  }

  # Check if directory exists
  if (!is.null(dir) && !dir.exists(dir)) {
    stop("Directory does not exist.")
  }

  # Initialize vector to store JSON paths
  json_paths <- character(0)

  # If filepaths argument is provided, append them to json_paths
  if (!is.null(filepaths)) {
    json_paths <- c(json_paths, filepaths)
  }

  # Find JSON files in the directory
  if (!is.null(dir)) {
    json_paths <- c(json_paths, Sys.glob(file.path(dir, "*.json")))
  }

  # Check if any JSON files found
  if (length(json_paths) == 0) {
    stop("No JSON files found.")
  }

  # Create a data frame to store JSON file paths
  json_df <- data.frame(json_paths = json_paths)

  # Function to check if JSON files have error status
  check_error_status <- function(file_path) {
    status <- tryCatch({
      json_data <- RcppSimdJson::fload(file_path)
      status <- json_data$status
      status
    }, error = function(e) {
      return(NA)
    })
    return(status != 200)
  }

  # Validate JSON files for error status and store the results
  json_df$has_error_status <- purrr::map_lgl(json_df$json_paths, check_error_status)

  # Find JSON files with error status
  if (any(json_df$has_error_status)) {
    # Create 'corrupted_files' sub-directory if it does not exist
    purrr::walk(json_df$json_paths[json_df$has_error_status], function(file_path) {
      parent_dir <- dirname(file_path)
      corrupted_dir <- file.path(parent_dir, "corrupted_files")
      if (!dir.exists(corrupted_dir)) {
        dir.create(corrupted_dir, recursive = TRUE)
      }
      # Move files with error status to 'corrupted_files' sub-directory
      file.rename(file_path, file.path(corrupted_dir, basename(file_path)))
    })

    # Warning message
    warning(paste0("JSON files with error status found. Moved to 'corrupted_files' sub-directory."))
  }
}




#' Set Parent Directory Based on System Information
#'
#' This function sets the parent directory based on the system's node name and login information.
#' It takes specifications for up to three different systems and matches the current system's
#' information to set the appropriate directory. If no match is found, it sets the directory to
#' the current working directory.
#'
#' @param nodename_A Character. Node name of the first system. Must be provided.
#' @param login_A Character. Login name of the first system. Must be provided.
#' @param dir_A Character. Directory path for the first system. Must be provided.
#' @param nodename_B Character. Node name of the second system. Must be provided.
#' @param login_B Character. Login name of the second system. Must be provided.
#' @param dir_B Character. Directory path for the second system. Must be provided.
#' @param nodename_C Character. Node name of the third system. Optional.
#' @param login_C Character. Login name of the third system. Optional.
#' @param dir_C Character. Directory path for the third system. Optional.
#'
#' @return A character string representing the parent directory.
#' If no match is found, returns the current working directory.
#' If the specified directory does not exist, an error is thrown.
#'
#' @examples
#' \dontrun{
#' set_parentdir(nodename_A = "node1", login_A = "user1", dir_A = "/path/to/dir1",
#'               nodename_B = "node2", login_B = "user2", dir_B = "/path/to/dir2")
#'
#' set_parentdir(nodename_A = "node1", login_A = "user1", dir_A = "/path/to/dir1",
#'               nodename_B = "node2", login_B = "user2", dir_B = "/path/to/dir2",
#'               nodename_C = "node3", login_C = "user3", dir_C = "/path/to/dir3")
#' }
#'
#' @export
set_parentdir <- function(nodename_A = NULL,
                          login_A = NULL,
                          dir_A = NULL,
                          nodename_B = NULL,
                          login_B = NULL,
                          dir_B = NULL,
                          nodename_C = NULL,
                          login_C = NULL,
                          dir_C = NULL
){

  # Stop if missing specifications
  if(is.null(nodename_A)) stop("Please provide nodename_A.")
  if(is.null(login_A)) stop("Please provide login_A.")
  if(is.null(dir_A)) stop("Please provide dir_A.")

  if(is.null(nodename_B)) stop("Please provide nodename_B.")
  if(is.null(login_B)) stop("Please provide login_B.")
  if(is.null(dir_B)) stop("Please provide dir_B.")

  exists("par_dir")

  # Get System info
  sysinfo <- Sys.info()

  # Set directory if on machine A
  if(sysinfo[["nodename"]] == nodename_A && sysinfo[["login"]] == login_A)  par_dir <- file.path(dir_A)

  # Set directory if on machine B
  if(sysinfo[["nodename"]] == nodename_B && sysinfo[["login"]] == login_B)  par_dir <- file.path(dir_B)

  # Evaluate third option if specified
  if(!is.null(nodename_C) && !is.null(login_C) && !is.null(dir_C)){
    if(sysinfo[["nodename"]] == nodename_C && sysinfo[["login"]] == login_C) par_dir <- file.path(dir_C)
  }

  # Set to current working directory if no match
  if(!exists("par_dir")){
    warning("No directory specified. Setting current working directory.")
    par_dir <- getwd()
  }

  # Error if parent directory invalid
  if(!dir.exists(par_dir)) stop(paste("Parent directory is invalid:", par_dir))

  # Return
  return(par_dir)
}



#' Create Breaks and Labels for Time Intervals
#'
#' This function constructs a vector of breaks and corresponding labels for specified time intervals.
#'
#' @param cutoff A numeric vector of cutoff values for the intervals. If not provided, defaults to `NULL`.
#'
#' @return A list containing two elements:
#' \itemize{
#'   \item \code{cutoff}: A numeric vector of cutoff values for the intervals, sorted in ascending order and containing \code{Inf}.
#'   \item \code{labels}: A character vector of labels for the intervals.
#' }
#'
#' @details
#' If \code{cutoff} is \code{NULL} or empty, the function sets \code{cutoff} to \code{Inf}.
#' It then constructs the breaks vector starting from the first element of \code{cutoff} and
#' includes all elements of \code{cutoff}. The labels are constructed based on the intervals
#' defined by \code{cutoff}. If the start of the interval (\code{.x}) is less than 0 or equals
#'  \code{Inf}, no dash "-" is included in the label.

#' @export
create_cutoffs <- function(cutoff = NULL) {
  # Check if cutoff is empty, and set it to Inf if it is
  if (length(cutoff) == 0) {
    cutoff <- c(Inf)
  } else {
    # Ensure cutoff is sorted in ascending order and contains Inf
    cutoff <- sort(cutoff)
  }
  # Construct breaks vector
  breaks <- cutoff

  # Construct labels vector using purrr::map2
  labels <- purrr::map2_chr(
    c(cutoff[1], cutoff[-length(cutoff)]),  # Start of intervals
    cutoff,                        # End of intervals
    ~ paste0("T", match(.y, cutoff), "_", ifelse((.x < 0 || .x == Inf), "0", .x), if (.x < 0) "" else "-", ifelse(.y == Inf, "Inf", .y), "h")
  )

  # Return a list containing breaks and labels
  list(cutoff = cutoff, labels = labels)
}


