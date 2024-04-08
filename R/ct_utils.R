#' Create Crowdtangle List File from Account Dataframe
#'
#' This function creates a list file from a dataframe containing account information
#' about Facebook or Instagram handles, in the format expected by Crowdtangle batch upload
#' to create a list.
#'
#'
#' @param df A dataframe containing account information.
#' @param account_var The name of the column containing the handles.
#' @param drop_private Logical, indicating whether to drop rows containing "private" or "profile" in the 'doubts_var' column. Default is TRUE.
#' @param doubts_var Character. Name of a variable, containing written notes from manual annotation whether an account is a "profile" or "private".
#' @param list_name_var Variable indicating the list names.
#' @param list_name The name to be assigned to the "List" column.
#' @param platform Platform name. Can be "fb" for Facebook or "ig" for Instagram.
#' @param file Path to save the resulting dataframe as a CSV file.
#'
#' @return Subsetted dataframe with renamed columns.
#' @export
#'
ct_create_list_file <- function(df,
                                account_var = NULL,
                                drop_private = TRUE,
                                doubts_var = NULL,
                                list_name_var = NULL,
                                list_name = NULL,
                                platform = c("fb", "ig"),
                                file = NULL) {

  if(is.null(df) || is.null(account_var)){
    stop(paste0("Please provide a 'df' including the FB/IG handles as 'account_var'."))
  }

  # drop redundant handles
  account_df <- drop_redundant(df = df,
                               account_var = account_var)

  # Remove rows where "private" or "profile" is mentioned in doubts column
  if(drop_private){

    if(is.null(doubts_var)){
      stop(paste0("Please specify a 'doubts_var', containing notes if a account is a private profile."))
    }else{

      account_df$drop <- grepl("private|profile", account_df[[doubts_var]], ignore.case = TRUE)
      account_df <- account_df[!account_df$drop, ]
    }
  }

  # Create platform URL prefix to account handles
  if(length(platform) != 1){
    stop(paste0("Please specify 1 'platform' - 'fb' or 'ig'."))
  }else{

    account_df$profile_url <- paste0(ifelse(platform == "fb",
                                            "https://www.facebook.com/",
                                            "https://www.instagram.com/"),
                                     trimws(account_df[[account_var]]))
  }

  # Assign list name
  if (!is.null(list_name_var)) {
    account_df$list_name <- account_df[[list_name_var]]
  } else {
    if (!is.null(list_name)) {
      account_df$list_name <- list_name
    } else {
      stop("Please provide a 'list_name' or a variable indicating the list names 'list_name_var'.")
    }
  }

  # Subset the dataframe and rename the columns within the subset
  subset_df <- data.frame(`Page or Account URL` = account_df[["profile_url"]],
                          List = account_df[["list_name"]],
                          check.names = FALSE)

  # Save as .csv if file path provided
  if (!is.null(file)) {
    if (!dir.exists(dirname(file))) {
      stop("Please provide a valid file path.")
    } else {
      readr::write_csv(x = subset_df, file = file)
    }
  }

  return(subset_df)
}
