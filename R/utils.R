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
#' This function checks JSON files in a specified directory for validity. If any invalid JSON files are found, they are moved to a sub-directory named "corrupted_files" within the specified directory.
#'
#' @param dir A character string specifying the directory containing the JSON files.
#' @return This function does not return anything; it removes invalid JSON files by moving them to the "corrupted_files" sub-directory.
#' @export
remove_invalid_jsons <- function(dir) {
  # Check if directory exists
  if (!dir.exists(dir)) {
    stop("Directory does not exist.")
  }

  # Find JSON files
  json_paths <- Sys.glob(paste0(dir, "/*.json"))

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
    corrupted_dir <- file.path(dir, "corrupted_files")
    if (!dir.exists(corrupted_dir)) {
      dir.create(corrupted_dir)
    }

    # Filter out invalid files
    invalid_files <- json_df[json_df$valid_json == FALSE, , drop = FALSE]

    # Move invalid files to 'corrupted_files' sub-directory
    purrr::walk(invalid_files$json_paths, function(file){
      file.rename(file, file.path(corrupted_dir, basename(file)))
    })

    # Warning message
    warning(paste0("Invalid .json files detected in ", dir, ". Moved to '", corrupted_dir, "'. Count: ", nrow(invalid_files)))
  }
}




#' Remove and Move Corrupted JSON Files
#'
#' This function scans a specified directory for JSON files, checks their validity based on the `status` field,
#' and moves any corrupted files (with `status` not equal to 200) to a subdirectory named `corrupted_files`.
#'
#' @param dir A character string specifying the directory containing the JSON files to be checked.
#' @return This function does not return a value. It moves corrupted JSON files to a subdirectory
#' and prints a warning message if any corrupted files are found.
#' @details The function first verifies the existence of the specified directory. It then searches
#' for all JSON files in the directory and validates them based on their `status` field. JSON files
#' with a `status` field not equal to 200 are considered corrupted and moved to a `corrupted_files`
#' subdirectory within the specified directory.
#' @export
remove_error_jsons <- function(dir) {
  # Check if directory exists
  if (!dir.exists(dir)) {
    stop("Directory does not exist.")
  }

  # Find JSON files
  json_paths <- Sys.glob(paste0(dir, "/*.json"))

  # Create a data frame to store JSON file paths
  json_df <- data.frame(json_paths = json_paths)

  # Function to safely validate JSON files
  validate_status_safely <- purrr::safely(function(x) RcppSimdJson::fload(x) |> purrr::map2(c("status"), `[[`) == 200, otherwise = FALSE)

  # Validate JSON files and store the results
  json_df$valid_json <- validate_status_safely(json_df$json_paths)$result

  # Find invalid JSON files
  if (any(json_df$valid_json == FALSE)) {
    # Create 'corrupted_files' sub-directory if it does not exist
    corrupted_dir <- file.path(dir, "corrupted_files")
    if (!dir.exists(corrupted_dir)) {
      dir.create(corrupted_dir)
    }

    # Filter out invalid files
    invalid_files <- json_df[json_df$valid_json == FALSE, , drop = FALSE]

    # Move invalid files to 'corrupted_files' sub-directory
    purrr::walk(invalid_files$json_paths, function(file) {
      file.rename(file, file.path(corrupted_dir, basename(file)))
    })

    # Warning message
    warning(paste0("JSON files with Error status (!=200) found in ", dir, ". Moved to '", corrupted_dir, "'. Count: ", nrow(invalid_files)))
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

