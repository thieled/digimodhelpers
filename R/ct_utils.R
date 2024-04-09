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




#' Get accounts from a Crowdtangle list
#'
#' This function retrieves accounts from a Crowdtangle list using the Crowdtangle API.
#'
#' @param list A character string specifying the ID of the Crowdtangle list.
#' @param token A character string specifying the Crowdtangle API token. If not provided,
#'  the token will be retrieved from the environment variable "CROWDTANGLE_TOKEN".
#' @param count An integer specifying the maximum number of accounts to retrieve. Default is Inf.
#'
#' @return A dataframe containing the retrieved accounts.
#'
#' @export
ct_getlistaccounts <- function(list = NULL,
                               token = NULL,
                               count = Inf) {

  base_url <- "https://api.crowdtangle.com/"

  if (is.null(token)) {
    token <- Sys.getenv("CROWDTANGLE_TOKEN")
  }
  if (identical(token, "")) {
    stop("You need a token to continue. See ?ct_auth()")
  }

  if (is.null(list)){
    stop("Please provide the crowdtangle list id in 'list'.")
  }

  if (is.null(count)) {
    count <- 100L
  }
  pages <- ceiling(count / 100L)
  if (count > 100L) count <- 100L

  i <- 1

  url <- httr::modify_url(base_url, path = paste0("lists/", list, "/accounts"))
  res <- httr::GET(url, httr::add_headers("x-api-token" = token))
  con <- httr::content(res)
  out <- con$result$accounts

  while (identical(length(url), 1L)) {

    new_res <- httr::GET(url, httr::add_headers("x-api-token" = token))

    new_con <- httr::content(new_res)

    if (!is.null(new_con[["message"]]) &&
        !identical(new_con[["code"]], 32L)) {
      warning(new_con[["message"]])
    }

    # Append content lists
    out_new <- new_con$result$accounts
    out <- base::append(out, out_new)

    if (identical(new_con[["code"]], 32L)) {
      message("Rate limit reached. Waiting 1 minute...")
      Sys.sleep(61)
    } else {
      message("Pulled page ", i, "...")
      url <- new_con$result$pagination[["nextPage"]]
      i <- i + 1
      pages <- pages - 1
    }
    if (identical(pages, 0)) url <- character()
  }

  out <- dplyr::bind_rows(out)

  # Cheap fix for a bug in pagination handling that returns duplicates
  out <- dplyr::distinct(out)

  return(out)
}




#' Get all accounts from Crowdtangle lists
#'
#' This function retrieves all accounts from Crowdtangle lists using the Crowdtangle API.
#'
#' @param token A character string specifying the Crowdtangle API token.
#'
#' @return A dataframe containing all retrieved accounts from Crowdtangle lists.
#'
#' @export
#'
ct_get_all_listaccounts <- function(token = NULL){

  # Get lists
  list_ids <- crowdtangler::ct_getlists(token = token)

  # Use purrr::pmap to iterate over each list_id - safely
  ct_getlistaccounts_safe <- purrr::safely(ct_getlistaccounts, otherwise = NULL)
  results <- purrr::map(list_ids$id, .f = ct_getlistaccounts_safe, token = token, .progress = T)

  if (!is.null(results$error)) {
    warning(paste0("Error in 'ct_getlistaccounts':", results$error$message))
  }

  # Drop the "error" part.
  results <- results |> purrr::map2(c("result"), `[[`)

  # Set list_ids as names
  names(results) <- list_ids$id

  # Bind results
  dt <- data.table::rbindlist(results, use.names = T, fill = T, idcol = "list_id")

  return(dt)
}







#' Merge Crowdtangle list IDs with Account Dataframe
#'
#' This function merges Crowdtangle list IDs with an account dataframe based on account handles.
#'
#' @param acc_df A dataframe containing account information.
#' @param token A character string specifying the Crowdtangle API token.
#' @param handle_var The name of the variable containing account handles in the 'acc_df' dataframe.
#'
#' @return A dataframe with Crowdtangle list IDs merged with the original account dataframe.
#'
#' @export
#'
ct_merge_list_id <- function(acc_df,
                             token,
                             handle_var = NULL){

  # Get all acconts in lists for token
  list_accounts_df <- ct_get_all_listaccounts(token) |> dplyr::as_tibble()

  # Set platform prefix for variable names
  if(any(list_accounts_df$platform == "Facebook")){
    plat <- "fb"
  }else{
    plat <- "ig"
  }

  # Subset
  list_accounts_df <- list_accounts_df |> dplyr::select(handle,
                                                        list_id,
                                                        id)
  # Rename handle_var
  if(is.null(handle_var) || !is.character(handle_var)){
    stop("Please specify 'handle_var' as character.")
  }
  names(list_accounts_df)[names(list_accounts_df) == "handle"] <- handle_var

  # Rename list_id and id
  names(list_accounts_df)[names(list_accounts_df) == "list_id"] <- paste0(plat, "_ct_", "list_id")
  names(list_accounts_df)[names(list_accounts_df) == "id"] <- paste0(plat, "_ct_", "id")

  # Merge dfs
  acc_df_merged <- dplyr::left_join(acc_df,
                                    list_accounts_df,
                                    by = handle_var)

  return(acc_df_merged)
}



