#' Retrieve video details from a YouTube channel
#'
#' This function retrieves video details from a YouTube channel, including information such as video ID, title, description, and statistics.
#'
#' @param channel_id The ID of the YouTube channel from which to retrieve video details.
#' @param start_date Optional. The start date to filter videos by their published date. Defaults to NULL.
#' @param end_date Optional. The end date to filter videos by their published date. Defaults to NULL.
#' @param max_results Optional. Maximum number of video details to retrieve. Defaults to Inf.
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
        playlist_items <- subset(playlist_items, contentDetails.videoPublishedAt > lubridate::as_datetime(start_date))
      }

      if(!is.null(end_date)){
        playlist_items <- subset(playlist_items, contentDetails.videoPublishedAt > lubridate::as_datetime(end_date))
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
    message(paste0("Fetching details from n = ", n, " videos on channel_id: ", channel_id, " ..."))
    details <- pbapply::pblapply(vid_ids, trycatch_get_video_details, part = part, auth = auth)

    return(details)

  } else {
    message("No playlist items found.")
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

  res <- tuber:::tuber_GET("playlistItems", querylist, ...)

  if (max_results > 50) {
    page_token <- res$nextPageToken
    while (is.character(page_token)) {
      a_res <- tuber:::tuber_GET("playlistItems", list(
        part = part,
        playlistId = unname(filter["playlistId"]),
        maxResults = 50,
        pageToken = page_token
      ), ...)
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

  res
}
