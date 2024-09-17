#' Retrieve video details from a YouTube channel
#'
#' This function retrieves video details from a YouTube channel, including information such as video ID, title, description, and statistics.
#'
#' @param channel_id The ID of the YouTube channel from which to retrieve video details.
#' @param start_date Optional. The start date to filter videos by their published date. Defaults to NULL.
#' @param end_date Optional. The end date to filter videos by their published date. Defaults to NULL.
#' @param max_results Optional. Maximum number of video details to retrieve. Defaults to Inf.
#' @param progress_bar Optional. Print a progress bar. Default is false.
#' @param verbose Logical. Default is false.
#' @param part Optional. A character vector specifying the parts that the API response will include. Possible values are: "contentDetails", "id", "liveStreamingDetails", "localizations", "player", "recordingDetails", "snippet", "statistics", "status", "topicDetails". Defaults to include all parts.
#' @param auth Optional. The authentication method to be used. Defaults to "key".
#' @param ... Additional arguments passed to tuber_GET.
#' @return A list containing video details.
#' @details This function first retrieves channel resources using the `list_channel_resources` function from the 'tuber' package. It then extracts the playlist ID and retrieves playlist items using the `get_playlist_items` function. Finally, it retrieves details for each video using the `get_video_details` function. If no videos are found or an error occurs during the process, NULL is returned.
#' @examples
#' \dontrun{
#' # Authenticate using API key
#' tuber::yt_set_key("YOUR_API_KEY")
#'
#' # Retrieve video details for a channel
#' videos <- get_channel_vids(channel_id = "CHANNEL_ID", max_results = 10)
#' }
#' @export

get_channel_vids <- function(channel_id = channel_id,
                             start_date = NULL,
                             end_date = NULL,
                             max_results = Inf,
                             progress_bar = FALSE,
                             verbose = FALSE,
                             part = c("contentDetails", "id", "liveStreamingDetails", "localizations", "player", "recordingDetails", "snippet", "statistics", "status", "topicDetails"),
                             auth = "key",
                             ...){

  channel_res <- tryCatch({
    tuber::list_channel_resources(filter = list(channel_id = channel_id), part="contentDetails", auth = auth)
  }, error = function(e) {
    stop(paste0("Error when calling 'list_channel_resources' on ", channel_id, ": ", e))
    # Return NULL in case of error
    return(NULL)
  }
  )

  if (inherits(channel_res, "response")) {

    ## TO Do: implement automatic re-authentification here

    stop(paste("Error when calling 'list_channel_resources' on ", channel_id, ": HTTP failure:", channel_res$status_code))
  }

  if (channel_res$pageInfo$totalResults == 0) {
    stop(paste0("Error: No results for ", channel_id, ". Invalid channel ID?"))
  }

  # Extract playlist ID
  if (!is.null(channel_res)) {
    # Uploaded playlists:
    playlist_id <- channel_res$items[[1]]$contentDetails$relatedPlaylists$uploads

    playlist_items <- tryCatch({
      get_playlist_items_FIX(filter = list(playlist_id = playlist_id), max_results = max_results, simplify = TRUE, auth = auth)
    }, error = function(e) {
      stop(paste0("Error when calling 'get_playlist_items' on ", playlist_id, ": ", e))
    }
    )

    # Subset items to time frame
    if(!is.null(playlist_items) && (!is.null(start_date) || !is.null(end_date))){

      playlist_items$contentDetails.videoPublishedAt <- lubridate::as_datetime(playlist_items$contentDetails.videoPublishedAt)

      if(!is.null(start_date)){
        playlist_items <- subset(playlist_items, contentDetails.videoPublishedAt >= lubridate::as_datetime(start_date))
      }

      # very stupid mistake here...
      if(!is.null(end_date)){
        playlist_items <- subset(playlist_items, contentDetails.videoPublishedAt <= lubridate::as_datetime(end_date))
      }

    }

    # Get video IDs
    if(!is.null(playlist_items)){
      vid_ids <- playlist_items$contentDetails.videoId
      n <- length(vid_ids)
    }


    # Error-proof function to get video details
    trycatch_get_video_details <- function(video_id, part = part, auth = auth){

      v_det <- tryCatch({
        tuber::get_video_details(video_id = video_id, part = part, auth = auth)
      }, error = function(e) {
        stop(paste0("Error when calling 'get_playlist_items' on ", vid_ids, ": ", e))
        return(NULL)
      }, warning = function(w) {
        warning(paste0("Warning when calling 'get_playlist_items' on ", vid_ids, ": ", w))
        return(NULL)
      }
      )

      if(!is.null(v_det)){

        v_det <- c(v_det$items[[1]],
                   download_time = as.character(as.POSIXct(lubridate::now(), tz = "UTC")),
                   download_time_zone = "UTC",
                   download_software = "tuber",
                   yt_api = "v3")

      }

      return(v_det)

    }

    # Get video details
    if(verbose) message(paste0("Fetching details from n = ", n, " videos on channel_id: ", channel_id, " ..."))
    if(progress_bar) {
      details <- pbapply::pblapply(vid_ids, trycatch_get_video_details, part = part, auth = auth)
    }else{
      details <- lapply(vid_ids, trycatch_get_video_details, part = part, auth = auth)
    }

    return(details)

  } else {
    if(verbose) message("No playlist items found.")
    return(NULL)
  }

}





#' FIXED VERSION OF Get Playlist Items
#'
#' @param filter string; Required.
#' named vector of length 1
#' potential names of the entry in the vector:
#' \code{item_id}: comma-separated list of one or more unique playlist item IDs.
#' \code{playlist_id}: YouTube playlist ID.
#'
#' @param part Required. Comma separated string including one or more of the
#' following: \code{contentDetails, id, snippet, status}. Default:
#' \code{contentDetails}.
#' @param max_results Maximum number of items that should be returned.
#' Integer. Optional. Default is 50.
#' If over 50, all the results are returned.
#' @param simplify returns a data.frame rather than a list.
#' @param page_token specific page in the result set that should be
#' returned, optional
#' @param video_id  Optional. request should return only the playlist
#' items that contain the specified video.
#' @param \dots Additional arguments passed to \code{\link{tuber_GET}}.
#'
#' @return playlist items
#' @import tuber
#' @export
#' @references \url{https://developers.google.com/youtube/v3/docs/playlists/list}
#'
#' @examples
#' \dontrun{
#'
#' # Set API token via yt_oauth() first
#'
#' get_playlist_items(filter =
#'                        c(playlist_id = "PLrEnWoR732-CN09YykVof2lxdI3MLOZda"))
#' get_playlist_items(filter =
#'                        c(playlist_id = "PL0fOlXVeVW9QMO3GoESky4yDgQfK2SsXN"),
#'                        max_results = 51)
#' }

get_playlist_items_FIX <- function(filter = NULL, part = "contentDetails",
                               max_results = 50, video_id = NULL,
                               page_token = NULL, simplify = TRUE, ...) {

  # if (max_results < 0 || max_results > 50) {
  #   stop("max_results must be a value between 0 and 50.")
  # }

  valid_filters <- c("item_id", "playlist_id")
  if (!(names(filter) %in% valid_filters)) {
    stop("filter can only take one of the following values: item_id, playlist_id.")
  }

  if (length(filter) != 1) {
    stop("filter must be a vector of length 1.")
  }

  translate_filter <- c(item_id = "id", playlist_id = "playlistId")
  filter_name <- translate_filter[names(filter)]
  names(filter) <- filter_name

  querylist <- list(part = part,
                    maxResults = max(min(max_results, 50), 1),
                    pageToken = page_token, videoId = video_id)
  querylist <- c(querylist, filter)

  res <- tuber:::tuber_GET(path = "playlistItems",
                           query = querylist,
                           ...)

  if (max_results > 50) {

    page_token <- res$nextPageToken


    while (is.character(page_token)) {
      a_res <- tuber:::tuber_GET(path = "playlistItems",
                                 query = list(
                                   part = part,
                                   playlistId = unname(filter[["playlistId"]]), ## <--- double brackets
                                   maxResults = 50,
                                   pageToken = page_token
                                 ),
                                 ...) ## <--- pass arguments to tuber_GET
      res <- c(res, a_res)
      page_token <- a_res$nextPageToken
    }
  }

  if (simplify) {
    allResultsList <- unlist(res[which(names(res) == "items")], recursive = FALSE)
    allResultsList <- lapply(allResultsList, unlist)
    res <-
      do.call(
        plyr::rbind.fill,
        lapply(
          allResultsList,
          function(x) as.data.frame(t(x), stringsAsFactors = FALSE)
        )
      )
  }

  return(res)
}




#' Fetch YouTube Comments for a Video
#'
#' This function fetches comments for a specified YouTube video using the YouTube Data API.
#' It retrieves comments in multiple pages and stops fetching when the specified maximum number
#' of comments is reached. If an error occurs during the API call, the function stops and returns an error message.
#'
#' @param video_id A character string specifying the YouTube video ID.
#' @param simplify A logical value indicating whether to simplify the results into a data table. Default is `TRUE`.
#' @param max_n An integer specifying the maximum number of comments to retrieve. Default is `Inf`.
#' @param verbose A logical value indicating whether to print messages about the fetching progress. Default is `TRUE`.
#' @param ... Additional arguments passed to the `tuber:::tuber_GET` function.
#' @return A list of comments or a data table if `simplify = TRUE`. Returns an error message if an error occurs.
#' @export
#' @examples
#' \dontrun{
#' comments <- get_comments_yt(video_id = "your_video_id", max_n = 100)
#' }
get_comments_yt <- function(video_id = NULL,
                            simplify = TRUE,
                            max_n = Inf,
                            verbose = TRUE,
                            ...){

  # Function to fetch data for a single page
  fetch_page <- function(page_token = NULL, ...) {
    query_list <- list(
      videoId = video_id,
      part = "id,replies,snippet",
      pageToken = page_token
    )
    tuber:::tuber_GET(path = "commentThreads", query = query_list, ...)
  }

  # Initialize an empty list to collect results
  all_res <- list()

  # Fetch the first page
  if(verbose) message(paste0("Fetching comments for video ", video_id))
  res <- tryCatch({
    fetch_page(...)
  }, error = function(e) {
    stop(paste0("Error when fetching comments for ", video_id, ": ", e))
    # Return NULL in case of error
    return(NULL)
  }
  )

  # Store download time
  res <- c(res,
           download_time = as.character(as.POSIXct(lubridate::now(), tz = "UTC")),
           download_time_zone = "UTC",
           download_software = "tuber",
           yt_api = "v3")

  # Create all results list
  all_res <- append(all_res, list(res))

  # Count observations
  n_collected <- length(res$items)

  # Fetch subsequent pages if they exist
  page_token <- res$nextPageToken
  while (is.character(page_token) && (n_collected <= max_n)) {
    a_res <- fetch_page(page_token, ...)

    a_res <- c(a_res,
               download_time = as.character(as.POSIXct(lubridate::now(), tz = "UTC")),
               download_time_zone = "UTC",
               download_software = "tuber",
               yt_api = "v3")

    all_res <- append(all_res, list(a_res))

    # Pagination token and count
    n_collected <- n_collected + length(a_res$items)
    page_token <- a_res$nextPageToken
    if(verbose) message(paste0("Fetched n = ", n_collected, " comments for video ", video_id))
  }

  # If the last fetched page exceeds the max_n limit, truncate the extra items
  if (n_collected > max_n) {

    excess_items <- n_collected - max_n
    last_page_index <- length(all_res)
    all_res[[last_page_index]]$items <- utils::head(all_res[[last_page_index]]$items, -excess_items)
  }

  ### Reshape as data.table?

  if(simplify){

    # Function to move "download_time" to each item in the 'items' list
    move_download_time <- function(l) {
      # Extract top level info
      download_time <- l[["download_time"]]
      download_time_zone <- l[["download_time_zone"]]

      # Add info on each element of items
      l[["items"]] <- purrr::map(l[["items"]], ~{
        .x[["download_time"]] <- download_time
        .x[["download_time_zone"]] <- download_time_zone
        .x
      })

      return(l)
    }

    # Apply this function to each top-level element in 'all_res'
    all_res <- purrr::map(all_res, move_download_time)

    # Extract the 'snippet' element and remove upper level (pagination)
    items <- all_res|>
      purrr::map(~.x$items) |>
      purrr::flatten()

    # Extract the 'snippet' elements
    snippets <- items |>
      purrr::map(~.x$snippet)

    # Assign the id as name
    names(snippets) <- items |>
      purrr::map(~.x$id)

    # Extract all elements except 'topLevelComment' and create a data.table
    snippets_flat <- snippets |>
      purrr::map(~.x[!names(.x) %in% c("topLevelComment")]) |>
      data.table::rbindlist(fill = TRUE, use.names = TRUE, idcol = "id")

    # Extract the 'topLevelComment' element from each snippet
    toplevelcomments <- snippets |>
      purrr::map(~.x$topLevelComment) |>
      purrr::map(~.x$snippet)

    # Bind topLevelComment
    snippets_flat[, topLevelComment := toplevelcomments]
    snippets_flat[, download_time := purrr::map(items, ~.x$download_time)]
    snippets_flat[, download_time_zone := purrr::map(items, ~.x$download_time_zone)]

    # Unlist 'snippet' column
    snippets_flat[, topLevelComment := purrr::map(snippets_flat[, topLevelComment], ~ unlist(.x))]

    # Unnest 'snippet' column
    snippets_flat <- tidytable::unnest_wider(snippets_flat,
                                             topLevelComment,
                                             names_sep = "_",
                                             names_repair = "minimal")

    all_res <- snippets_flat

  }

  return(all_res)

}








#' Search YouTube
#'
#' Search for videos, channels and playlists. (By default, the function
#' searches for videos.)
#' NOTE: The fix ensures that additional arguments are correctly passed on to tuber_GET.
#'
#' @param term Character. Search term; required; no default
#' For using Boolean operators, see the API documentation.
#' Here's some of the relevant information:
#' "Your request can also use the Boolean NOT (-) and OR (|) operators to
#' exclude videos or to
#' find videos that are associated with one of several search terms. For
#' example, to search
#' for videos matching either "boating" or "sailing", set the q parameter
#' value to boating|sailing.
#' Similarly, to search for videos matching either "boating" or "sailing"
#' but not "fishing",
#' set the q parameter value to boating|sailing -fishing"
#' @param max_results Maximum number of items that should be returned.
#' Integer. Optional. Can be between 0 and 50. Default is 50.
#' Search results are constrained to a maximum of 500 videos if type is
#' video and we have a value of \code{channel_id}.
#' @param channel_id Character. Only return search results from this
#' channel; Optional.
#' @param channel_type Character. Optional. Takes one of two values:
#' \code{'any', 'show'}. Default is \code{'any'}
#' @param event_type Character. Optional. Takes one of three values:
#' \code{'completed', 'live', 'upcoming'}
#' @param location  Character.  Optional. Latitude and Longitude within
#' parentheses, e.g. "(37.42307,-122.08427)"
#' @param location_radius Character.  Optional. e.g. "1500m", "5km",
#' "10000ft", "0.75mi"
#' @param published_after Character. Optional. RFC 339 Format.
#' For instance, "1970-01-01T00:00:00Z"
#' @param published_before Character. Optional. RFC 339 Format.
#' For instance, "1970-01-01T00:00:00Z"
#' @param relevance_language Character. Optional. The relevance_language
#' argument instructs the API to return search results that are most relevant to
#' the specified language. The parameter value is typically an ISO 639-1
#' two-letter language code. However, you should use the values zh-Hans for
#' simplified Chinese and zh-Hant for traditional Chinese. Please note that
#' results in other languages will still be returned if they are highly relevant
#' to the search query term.
#' @param type Character. Optional. Takes one of three values:
#' \code{'video', 'channel', 'playlist'}. Default is \code{'video'}.
#' @param video_caption Character. Optional. Takes one of three values:
#' \code{'any'} (return all videos; Default), \code{'closedCaption', 'none'}.
#' Type must be set to video.
#' @param video_type Character. Optional. Takes one of three values:
#' \code{'any'} (return all videos; Default), \code{'episode'}
#' (return episode of shows), 'movie' (return movies)
#' @param video_syndicated Character. Optional. Takes one of two values:
#' \code{'any'} (return all videos; Default), \code{'true'}
#' (return only syndicated videos)
#' @param region_code Character. Required. Has to be a ISO 3166-1 alpha-2 code
#'  (see \url{https://www.iso.org/obp/ui/#search}).
#' @param video_definition Character. Optional.
#' Takes one of three values: \code{'any'} (return all videos; Default),
#' \code{'high', 'standard'}
#' @param video_license Character. Optional.
#' Takes one of three values: \code{'any'} (return all videos; Default),
#' \code{'creativeCommon'} (return videos with Creative Commons
#' license), \code{'youtube'} (return videos with standard YouTube license).
#' @param relevance_language Character. Default is "en".
#' @param simplify Boolean. Return a data.frame if \code{TRUE}.
#' Default is \code{TRUE}.
#' If \code{TRUE}, it returns a list that carries additional information.
#' @param page_token specific page in the result set that should be
#' returned, optional
#' @param get_all get all results, iterating through all the results
#' pages. Default is \code{TRUE}.
#' Result is a \code{data.frame}. Optional.
#' @param \dots Additional arguments passed to \code{\link{tuber_GET}}.
#'
#' @return data.frame with 16 elements: \code{video_id, publishedAt,
#' channelId, title, description,
#' thumbnails.default.url, thumbnails.default.width, thumbnails.default.height,
#' thumbnails.medium.url,
#' thumbnails.medium.width, thumbnails.medium.height, thumbnails.high.url,
#' thumbnails.high.width,
#' thumbnails.high.height, channelTitle, liveBroadcastContent}
#'
#' @export
#'
#' @references \url{https://developers.google.com/youtube/v3/docs/search/list}
#'
#' @examples
#'
#' \dontrun{
#'
#' # Set API token via yt_oauth() first
#'
#' yt_search(term = "Barack Obama")
#' yt_search(term = "Barack Obama", published_after = "2016-10-01T00:00:00Z")
#' yt_search(term = "Barack Obama", published_before = "2016-09-01T00:00:00Z")
#' yt_search(term = "Barack Obama", published_before = "2016-03-01T00:00:00Z",
#'                                published_after = "2016-02-01T00:00:00Z")
#' yt_search(term = "Barack Obama", published_before = "2016-02-10T00:00:00Z",
#'                                published_after = "2016-01-01T00:00:00Z")
#' }
yt_search_FIX <- function(
    term = NULL,
    max_results = 50,
    channel_id = NULL,
    channel_type = NULL,
    type = "video",
    event_type = NULL,
    location = NULL,
    location_radius = NULL,
    published_after = NULL,
    published_before = NULL,
    video_definition = "any",
    video_caption = "any",
    video_license = "any",
    video_syndicated = "any",
    region_code = NULL,
    relevance_language = "en",
    video_type = "any",
    simplify = TRUE,
    get_all = TRUE,
    page_token = NULL,
    ...
) {

  if (!is.character(term)) stop("Must specify a search term.\n")

  if (max_results < 0 | max_results > 50) {
    stop("max_results only takes a value between 0 and 50.")
  }

  if (type == "video" && !(video_license %in% c("any", "creativeCommon", "youtube"))) {
    stop("video_license can only take values: any, creativeCommon, or youtube.")
  }

  if (type == "video" && !(video_syndicated %in% c("any", "true"))) {
    stop("video_syndicated can only take values: any or true.")
  }

  if (type == "video" && !(video_type %in% c("any", "episode", "movie"))) {
    stop("video_type can only take values: any, episode, or movie.")
  }

  if (is.character(published_after)) {
    if (is.na(as.POSIXct(published_after,  format = "%Y-%m-%dT%H:%M:%SZ"))) {
      stop("The date is not properly formatted in RFC 339 Format.")
    }
  }

  if (is.character(published_before)) {
    if (is.na(as.POSIXct(published_before, format = "%Y-%m-%dT%H:%M:%SZ"))) {
      stop("The date is not properly formatted in RFC 339 Format.")
    }
  }

  if (type != "video") {
    video_caption <- video_license <- video_definition <-
      video_type <- video_syndicated <- NULL
  }
  if (!is.null(location) && is.null(location_radius)) {
    stop("Location radius must be specified with location")
  }

  querylist <- list(part = "snippet",
                    q = term,
                    maxResults = max_results,
                    channelId = channel_id,
                    type = type,
                    channelType = channel_type,
                    eventType = event_type,
                    location = location,
                    locationRadius = location_radius,
                    publishedAfter = published_after,
                    publishedBefore = published_before,
                    videoDefinition = video_definition,
                    videoCaption = video_caption,
                    videoType = video_type,
                    videoSyndicated = video_syndicated,
                    videoLicense = video_license,
                    regionCode = region_code,
                    relevanceLanguage = relevance_language,
                    pageToken = page_token)

  # Sending NULLs to Google seems to short its wiring
  querylist <- querylist[names(querylist)[sapply(querylist, function(x) !is.null(x))]]

  all_results <- list()

  repeat {
    res <- tuber:::tuber_GET("search", querylist, ...)

    if (type == "video") {
      simple_res <- lapply(res$items, function(x) {
        c(video_id = x$id$videoId, unlist(x$snippet))
      })
    } else {
      simple_res <- lapply(res$items, function(x) unlist(x$snippet))
    }

    all_results <- c(all_results, simple_res)

    page_token <- res$nextPageToken

    if (is.null(page_token) || !get_all) break

    querylist$pageToken <- page_token
  }

  fin_res <- plyr::ldply(all_results, rbind)

  if (identical(simplify, TRUE)) {
    return(fin_res)
  }

  return(res)
}

