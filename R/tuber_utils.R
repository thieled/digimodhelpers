#' Get Video Details from YouTube Channel
#'
#' This function retrieves video details from a YouTube channel based on provided parameters.
#'
#' @param channel_id The ID of the YouTube channel.
#' @param start_date The start date to filter videos. Defaults to NULL (no filtering).
#' @param end_date The end date to filter videos. Defaults to NULL (no filtering).
#' @param max_results Maximum number of results to retrieve. Defaults to Inf (retrieve all results).
#' @param part A character vector specifying the parts of the resource to retrieve.
#' Defaults to c("contentDetails", "id", "liveStreamingDetails", "localizations",
#' "player", "recordingDetails", "snippet", "statistics", "status", "topicDetails").
#' @param ... Additional arguments to be passed to underlying functions.
#'
#' @return A list containing details of videos from the YouTube channel.
#'
#' @examples
#' \dontrun{
#' get_channel_vids(channel_id = "your_channel_id", start_date = "2023-01-01", end_date = "2023-12-31")
#' }
#'
#' @export
#'
get_channel_vids <- function(
    channel_id = channel_id,
    start_date = NULL,
    end_date = NULL,
    max_results = Inf,
    part = c("contentDetails", "id", "liveStreamingDetails", "localizations", "player", "recordingDetails", "snippet", "statistics", "status", "topicDetails"),
    ...) {
  channel_res <- tryCatch(
    {
      tuber::list_channel_resources(filter = list(channel_id = channel_id), part = "contentDetails")
    },
    error = function(e) {
      message(paste0("Error when calling 'list_channel_resources' on ", channel_id, ": ", e))
      # Return NULL in case of error
      return(NULL)
    }
  )

  # Extract playlist ID
  if (!is.null(channel_res)) {
    # Uploaded playlists:
    playlist_id <- channel_res$items[[1]]$contentDetails$relatedPlaylists$uploads

    playlist_items <- tryCatch(
      {
        tuber::get_playlist_items(filter = list(playlist_id = playlist_id), max_results = max_results, simplify = TRUE)
      },
      error = function(e) {
        message(paste0("Error when calling 'get_playlist_items' on ", playlist_id, ": ", e))
        # Return NULL in case of error
        return(NULL)
      }
    )

    # Subset items to time frame
    if (!is.null(playlist_items) && (!is.null(start_date) || !is.null(end_date))) {
      playlist_items$contentDetails.videoPublishedAt <- lubridate::as_datetime(playlist_items$contentDetails.videoPublishedAt)

      if (!is.null(start_date)) {
        subset(playlist_items, contentDetails.videoPublishedAt > as.Date(start_date))
      }

      if (!is.null(end_date)) {
        subset(playlist_items, contentDetails.videoPublishedAt > as.Date(end_date))
      }
    }

    # Get video IDs
    if (!is.null(playlist_items)) {
      vid_ids <- playlist_items$contentDetails.videoId
    }


    # Error-proof function to get video details
    trycatch_get_video_details <- function(video_id, part = "snippet") {
      v_det <- tryCatch(
        {
          details <- tuber::get_video_details(video_id = video_id, part = part)
          details <- c(details,
            download_time = as.character(as.POSIXct(lubridate::now(), tz = "UTC")),
            download_time_zone = "UTC",
            download_software = "tuber",
            yt_api = "v3"
          )
          return(details)
        },
        error = function(e) {
          message(paste0("Error when calling 'get_playlist_items' on ", vid_ids, ": ", e))
          # Return NULL in case of error
          return(NULL)
        }
      )
    }

    # Get video details
    message(paste0("Fetching video details from channel_id: ", channel_id, " ..."))
    details <- pbapply::pblapply(vid_ids, trycatch_get_video_details, part = part)

    return(details)
  } else {
    return(NULL)
  }
}
