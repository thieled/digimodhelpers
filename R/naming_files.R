
#' Create Filenames for Social Media Data Collection
#'
#' This function constructs filenames for social media data collection based on specified parameters.
#'
#' @param df A data frame containing the data.
#' @param platform A character vector specifying the platform(s) from which the data is collected. Default is c("fb", "ig", "tt", "yt", "tg", "bc", "bs").
#' @param country_var A character string specifying the column name in the data frame that contains country information. Default is NULL.
#' @param party_var A character string specifying the column name in the data frame that contains party information. Default is NULL.
#' @param name_var A character string specifying the column name in the data frame that contains name information. Default is NULL.
#' @param filename_var A character string specifying the column name to store the constructed filenames. Default is NULL.
#' @param name_sep A character string specifying the separator for name variables. Default is "-".
#' @param lowercase Logical; if TRUE, filenames will be converted to lowercase. Default is TRUE.
#' @param replace_non_ascii Logical; if TRUE, non-ASCII characters will be replaced in filenames. Default is TRUE.
#' @param filename_sep A character string specifying the separator for filename construction. Default is "_".
#'
#' @return The input data frame \code{df} with an additional column containing constructed filenames.
#'
#' @examples
#' df <- data.frame(country = c("USA", "UK", "Germany"),
#'                  party = c("Democrat", "Republican", "Green"),
#'                  name = c("Joe Biden", "Donald Trump", "Angela Merkel"))
#'
#' create_filename(df,
#' platform = "fb",
#' country_var = "country",
#' party_var = "party",
#' name_var = "name",
#' filename_var = "filename")
#'
#' @export
create_filename <- function(df,
                            platform = c("fb", "ig", "tt", "yt", "tg", "bc", "bs"),
                            country_var = NULL,
                            party_var = NULL,
                            name_var = NULL,
                            filename_var = NULL,
                            name_sep = "-",
                            lowercase = T,
                            replace_non_ascii = T,
                            filename_sep = "_"
){

  # Error messages
  name_index <- match(name_var, names(df))
  if (is.null(name_var)||is.na(name_index))
    stop("name_var column not found or invalid")

  # Create filename
  df[[filename_var]] <-  paste(

    platform,

    if(lowercase == T) tolower( if(replace_non_ascii == T) textclean::replace_non_ascii( if(!is.null(country_var)) df[[country_var]] else "") ), # country

    if(lowercase == T) tolower( if(replace_non_ascii == T) textclean::replace_non_ascii( if(!is.null(party_var)) gsub("[^[:alnum:]]", "", df[[party_var]]) else "") ), # party

    if(lowercase == T) tolower( if(replace_non_ascii == T) textclean::replace_non_ascii( if(!is.null(name_var)) gsub("[^[:alnum:]]", name_sep, df[[name_var]]) else "") ), # name variable

    sep = filename_sep

  )


  return(df)

}


