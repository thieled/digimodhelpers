
#' Fuzzy-match strings against a harmonized reference by groups
#'
#' @description
#' Matches a messy text column in `input` to a harmonized label column in `target`,
#' optionally returning IDs from both sides. Matching is constrained within groups
#' shared by both data frames (e.g., country, EP group). Exact matches are attempted
#' first, followed by optional fuzzy matching. Poor matches (distance > threshold)
#' have their label/target ID set to `NA`. Additional options allow deduplication
#' by input or target IDs.
#'
#' @param input A data frame containing the messy text to be matched.
#' @param target A data frame containing the harmonized labels.
#' @param input_col A string giving the column name in `input` with messy text values.
#' @param target_col A string giving the column name in `target` with harmonized labels.
#' @param input_id_col A string naming an optional ID column in `input` to pass through.
#'   Default `NULL`.
#' @param target_id_col A string naming an optional ID column in `target` to return for matches.
#'   Default `NULL`.
#' @param group_vars A character vector of column names present in both `input` and `target`
#'   that define matching groups. Matching is only performed within groups.
#'   Default `NULL`.
#' @param fuzzy Logical. Whether to perform fuzzy matching for non-exact rows.
#'   Default `TRUE`.
#' @param stopwords A character vector of tokens to remove before distance calculation.
#'   Default `NULL`.
#' @param method A string giving the string distance method passed to `stringdist`.
#'   Default `"jw"`.
#' @param p A numeric tuning parameter for the distance method (e.g., Jaro–Winkler).
#'   Default `0.2`.
#' @param threshold A numeric distance cutoff in range 0-1. Matches with distance greater
#'   than this are considered poor and set to `NA`. Default `0.20`.
#' @param normalize Either `"none"` or `"length"`. Whether to normalize distances.
#'   Default `"none"`.
#' @param tolower Logical. If `TRUE`, text is converted to lowercase before matching.
#'   Default `TRUE`.
#' @param verbose Logical. If `TRUE`, prints a brief summary of matches. Default `TRUE`.
#' @param best_by_input_id Logical. If `TRUE` and `input_id_col` is provided, deduplicates
#'   the output by input ID, keeping only the row with the lowest distance (exact matches
#'   preferred, `NA` treated as worst). Default `FALSE`.
#' @param best_by_target_id Logical. If `TRUE` and `target_id_col` is provided, flags
#'   duplicate matches by target ID. For each target ID, the match with the lowest distance
#'   is kept as-is, and all others are flagged with `poor_match = TRUE`. No rows are dropped.
#'   Default `FALSE`.
#'
#' @return A data frame with the following columns:
#'   * `group_vars` (if supplied),
#'   * `input_id` (if supplied),
#'   * `input` (original values from input),
#'   * `matched_target` (best match from target),
#'   * `target_id` (if supplied),
#'   * `exact_match` (logical, whether exact match),
#'   * `distance` (string distance to match),
#'   * `poor_match` (logical, TRUE if flagged as poor or duplicate).
#' @importFrom stats setNames
#' @export

fuzzy_match_df <- function(
    input,
    target,
    input_col,
    target_col,
    input_id_col  = NULL,
    target_id_col = NULL,
    group_vars = NULL,
    fuzzy = TRUE,
    stopwords = NULL,
    method = "jw",
    p = 0.2,
    threshold = 0.20,
    normalize = c("none","length"),
    tolower = TRUE,
    verbose = TRUE,
    best_by_input_id = FALSE,
    best_by_target_id = FALSE
) {
  if (!requireNamespace("stringdist", quietly = TRUE)) {
    stop("Package 'stringdist' is required; please install it (Suggests).")
  }
  if (!requireNamespace("fastmatch", quietly = TRUE)) {
    stop("Package 'fastmatch' is required; please install it (Suggests).")
  }

  input  <- base::as.data.frame(input,  stringsAsFactors = FALSE)
  target <- base::as.data.frame(target, stringsAsFactors = FALSE)
  normalize <- base::match.arg(normalize)

  if (!is.data.frame(input))  stop("'input' must be a data.frame.")
  if (!is.data.frame(target)) stop("'target' must be a data.frame.")
  if (!input_col %in% names(input))   stop("`input_col` not found in `input`.")
  if (!target_col %in% names(target)) stop("`target_col` not found in `target`.")
  if (!is.null(input_id_col)  && !input_id_col  %in% names(input))  stop("`input_id_col` not found in `input`.")
  if (!is.null(target_id_col) && !target_id_col %in% names(target)) stop("`target_id_col` not found in `target`.")

  if (is.null(group_vars)) group_vars <- character(0)
  if (length(group_vars)) {
    miss_in  <- setdiff(group_vars, names(input))
    miss_tar <- setdiff(group_vars, names(target))
    if (length(miss_in))  stop("Group vars missing in `input`: ", paste(miss_in, collapse = ", "))
    if (length(miss_tar)) stop("Group vars missing in `target`: ", paste(miss_tar, collapse = ", "))
  }

  norm_text <- function(x) if (tolower) base::tolower(x) else x
  strip_tokens <- function(vec, sw) {
    if (is.null(sw)) return(vec)
    base::vapply(
      vec,
      function(s) base::paste(base::setdiff(base::strsplit(s, "\\s+")[[1]], sw), collapse = " "),
      FUN.VALUE = character(1)
    )
  }

  input_key <- if (length(group_vars)) {
    base::interaction(input[, group_vars, drop = FALSE], drop = TRUE, lex.order = TRUE)
  } else factor(rep(1L, nrow(input)))
  target_key <- if (length(group_vars)) {
    base::interaction(target[, group_vars, drop = FALSE], drop = TRUE, lex.order = TRUE)
  } else factor(rep(1L, nrow(target)))

  out_input          <- input[[input_col]]
  out_input_id       <- if (is.null(input_id_col)) base::rep(NA_character_, nrow(input)) else input[[input_id_col]]
  out_matched_target <- base::rep(NA_character_, nrow(input))
  out_target_id      <- base::rep(NA_character_, nrow(input))
  out_exact          <- base::rep(FALSE,        nrow(input))
  out_distance_raw   <- base::rep(NA_real_,     nrow(input))
  out_poor           <- base::rep(NA,           nrow(input))

  input_split_idx  <- split(seq_len(nrow(input)),  input_key, drop = TRUE)
  target_split_idx <- split(seq_len(nrow(target)), target_key, drop = TRUE)

  for (g in names(input_split_idx)) {
    ii <- input_split_idx[[g]]
    tt <- target_split_idx[[g]]
    if (is.null(tt) || length(tt) == 0L) next

    in_vec_raw  <- input[[input_col]][ii]
    tar_vec_raw <- target[[target_col]][tt]
    id_vec_raw  <- if (is.null(target_id_col)) rep(NA_character_, length(tt)) else target[[target_id_col]][tt]

    in_norm  <- norm_text(in_vec_raw)
    tar_norm <- norm_text(tar_vec_raw)

    exact_idx <- fastmatch::fmatch(in_norm, tar_norm, nomatch = NA_integer_)
    out_exact[ii] <- !is.na(exact_idx)

    match_idx <- exact_idx
    dist_vec  <- base::rep(NA_real_, length(ii))

    need_fuzzy <- is.na(exact_idx)
    if (any(need_fuzzy) && isTRUE(fuzzy)) {
      in_need   <- in_norm[need_fuzzy]
      tar_cands <- tar_norm
      in_need2   <- strip_tokens(in_need,   stopwords)
      tar_cands2 <- strip_tokens(tar_cands, stopwords)

      dm <- stringdist::stringdistmatrix(in_need2, tar_cands2, method = method, p = p)
      min_idx <- base::apply(dm, 1L, base::which.min)
      min_dst <- base::apply(dm, 1L, base::min)

      match_idx[need_fuzzy] <- min_idx
      dist_vec[need_fuzzy]  <- min_dst
    }

    dist_vec[!is.na(exact_idx)] <- 0

    have_match <- !is.na(match_idx)
    if (any(have_match)) {
      out_matched_target[ii[have_match]] <- tar_vec_raw[match_idx[have_match]]
      if (!is.null(target_id_col)) out_target_id[ii[have_match]] <- id_vec_raw[match_idx[have_match]]
      out_distance_raw[ii] <- dist_vec
    }

    if (normalize == "length") {
      denom <- base::rep(1, length(ii))
      if (any(have_match)) {
        mt <- out_matched_target[ii]
        denom <- base::ifelse(!is.na(mt), base::pmin(base::sqrt(base::nchar(mt)), 5), 1)
        denom[denom == 0 | base::is.na(denom)] <- 1
      }
      out_distance_raw[ii] <- dist_vec / denom
    } else {
      out_distance_raw[ii] <- dist_vec
    }

    pmask <- !is.na(out_distance_raw[ii]) & have_match
    poor  <- base::rep(NA, length(ii))
    poor[pmask] <- out_distance_raw[ii][pmask] > threshold

    if (any(poor %in% TRUE)) {
      idx_bad <- ii[which(poor %in% TRUE)]
      out_matched_target[idx_bad] <- NA_character_
      if (!is.null(target_id_col)) out_target_id[idx_bad] <- NA_character_
    }

    out_poor[ii] <- poor
  }

  out <- base::data.frame(
    input_id       = out_input_id,
    input          = out_input,
    matched_target = out_matched_target,
    target_id      = if (is.null(target_id_col)) base::rep(NA_character_, nrow(input)) else out_target_id,
    exact_match    = out_exact,
    distance       = out_distance_raw,
    poor_match     = out_poor,
    stringsAsFactors = FALSE
  )

  if (length(group_vars)) {
    out <- base::cbind(input[, group_vars, drop = FALSE], out, stringsAsFactors = FALSE)
  }

  # ---- NEW: deduplicate by input_id (keep best) ----
  dedup_best <- function(df, id_var) {
    if (!id_var %in% names(df)) return(df)
    if (all(is.na(df[[id_var]]))) return(df)
    df |>
      dplyr::mutate(
        .dist  = dplyr::if_else(is.na(distance), Inf, distance),
        .exact = dplyr::if_else(exact_match, 0L, 1L)
      ) |>
      dplyr::arrange(.data[[id_var]], .exact, .dist) |>
      dplyr::distinct(.data[[id_var]], .keep_all = TRUE) |>
      dplyr::select(-.dist, -.exact)
  }

  if (isTRUE(best_by_input_id) && !is.null(input_id_col)) {
    out <- dedup_best(out, "input_id")
  }

  # ---- REVISED: best_by_target_id -> flag worse duplicates instead of dropping ----
  if (isTRUE(best_by_target_id) && !is.null(target_id_col) && "target_id" %in% names(out)) {
    out <- out |>
      # adjusted distance: NA treated as worst via Inf (so gets flagged if a better exists)
      dplyr::mutate(.adj_dist = dplyr::if_else(is.na(distance), Inf, distance)) |>
      dplyr::group_by(target_id) |>
      dplyr::mutate(
        .best_dist = dplyr::if_else(
          is.na(target_id),
          .adj_dist,                                    # keep as-is for NA target_id
          suppressWarnings(min(.adj_dist, na.rm = TRUE))# best (lowest) distance per target_id
        ),
        .flag_worse = !is.na(target_id) & (.adj_dist > .best_dist)
      ) |>
      # set poor_match = TRUE for worse duplicates; otherwise leave as-is (including NA)
      dplyr::mutate(
        poor_match = dplyr::if_else(.flag_worse, TRUE, poor_match, missing = poor_match)
      ) |>
      dplyr::ungroup() |>
      dplyr::select(-.adj_dist, -.best_dist, -.flag_worse)
  }

  if (isTRUE(verbose)) {
    n_total   <- nrow(input)
    n_exact   <- sum(out$exact_match, na.rm = TRUE)
    n_goodfz  <- sum(!out$exact_match & !is.na(out$distance) & out$poor_match %in% FALSE, na.rm = TRUE)
    n_poor    <- sum(out$poor_match %in% TRUE, na.rm = TRUE)
    n_nomatch <- sum(is.na(out$matched_target))
    base::message("\nFuzzy Match Summary:")
    base::message("  - Exact matches: ", n_exact, "/", n_total)
    base::message("  - Fuzzy matches (good): ", n_goodfz, "/", n_total)
    if (n_poor > 0)    base::message("  - Poor matches (set to NA or flagged): ", n_poor)
    if (n_nomatch > 0) base::message("  - Unmatched total: ", n_nomatch)
    if (isTRUE(best_by_input_id) && !is.null(input_id_col)) {
      base::message("  - Deduplicated by input_id")
    }
    if (isTRUE(best_by_target_id) && !is.null(target_id_col)) {
      base::message("  - Flagged worse duplicates by target_id (kept all rows)")
    }
  }

  out
}
