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
