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
  "valid_json"
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

