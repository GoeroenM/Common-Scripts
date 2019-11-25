#********************************************************************************#
#                                                                                #
# Basic data manipulation functions that can be applicable in many contexts      #
#                                                                                #
#********************************************************************************#

# Duplicates ------------------------------------------------------------------------------------------------------

# merge all rows with duplicate values on key_cols, concatenating unique, non-NA values,
# or performing sum/mean/max/min on numeric non-NA numeric values
#
# args:
#       - dt: a data.table (e.g., a promoBinary)
#       - key_cols: vector of column names on the basis of which duplicates will be merged,
#           i.e. in the output there will be no duplicates on the combination of these cols
#       - concat_cols: when merging duplicates, concatenate unique, non-NA values on these columns
#           If left at NULL, will default to all columns not in key_cols nor in sum, mean, max, min
#       - sum_cols, mean_cols, max_cols, min_cols: when merging duplicates, perform these operations on these (numeric) columns
#           NA values are removed before performing any operation
#       - weighted_average & weighted_average_by: for cols in 'weightedAverage' (e.g. Baseline_Price), 
#           take a weighted average based on values on 'weighted_average_by' (e.g. Baseline_Value)
#       - sep: used for colsToConcat
#       - concat_max_values: if in cell there's more than this many values we're concatenating,
#           instead return "< N Values >", where N is the amount
#       - concat_guess_if_empty: if T, if concat is empty (or NULL), it is automatically set to all columns not provided
#           in any other arg. If F, nonprovided columns are dropped.
#       - verbose: if F, no output is printed
#
# output: dt but with all duplicates merged
MergeDuplicatedRows <- function(dt, key_cols = NULL, concat_cols = NULL, sum_cols = NULL, mean_cols = NULL, 
                                max_cols = NULL, min_cols = NULL, weighted_average = NULL, weighted_average_by = NULL,
                                sep = ";", concat_max_values = Inf, concat_guess_if_empty = TRUE, verbose = TRUE) {
  # validate input
  for (input in c("key_cols", "concat_cols", "sum_cols", "mean_cols", "max_cols", "min_cols", "weighted_average")) {
    if (!(is.null(get(input)) || is.vector(get(input)))) stop(input, " is not a vector")
  }
  for (input in c("weighted_average", "sep")) {
    if (!(is.null(get(input)) || is.character(get(input)))) stop(input, " is not a string")
  }
  if (!is.null(weighted_average_by) && !weighted_average_by %in% sum_cols) {
    sum_cols <- c(sum_cols, weighted_average_by)
    if (verbose) cat("Adding ", weighted_average_by, " to values to sum.\n")
  }
  
  
  if (sum(duplicated(dt[, key_cols, with = F])) == 0) {
    if (verbose) cat("No duplicates found.\n")
    return(dt)
  }
  stopifnot(!(!is.null(weighted_average) && is.null(weighted_average_by)))
  
  dt <- copy(dt)
  col_order <- copy(names(dt))
  col_classes <- get_column_classes(dt)
  n_rows <- dt[, .N] # to print how many we merged
  
  if (is.null(key_cols) || key_cols == "" || is.na(key_cols)) {
    if (verbose) cat("Merging all rows\n")
    dt[, `___MERGE_BY___` := 1]
    key_cols <- "___MERGE_BY___"
  }
  
  if (is.null(concat_cols)) {
    missing <- setdiff(names(dt), c(key_cols, mean_cols, max_cols, min_cols, sum_cols, weighted_average))
    
    if (length(missing) > 0) {
      if (concat_guess_if_empty) {
        concat_cols <- missing
        if (verbose)  cat("No concat_cols specified, using concat_cols for all columns not listed",
                          "in any other parameter: ", str_c(concat_cols, collapse = ", "), "\n")
      } else {
        if (verbose)  cat("Following columns are not listed in any parameter and will be dropped: ", 
                          str_c(missing, collapse = ", "), "\n")
      }
    }
  }
  
  # we'll add results from the various operations to this skeleton structure
  out <- unique(dt[, key_cols, with = F])
  
  if (!is.null(concat_cols) && length(concat_cols) > 0) {
    concatOutcome <- dt[, lapply(.SD, function(x) {
      values <- na.omit(unique(x))
      if (length(values) > concat_max_values) {
        str_c("< ", length(values), " Values >")  # return simplified string
      } else {
        str_c(values, collapse = sep) # return all values concatenated
      }}),
      by = key_cols, .SDcols = concat_cols]
    
    out <- merge(out, concatOutcome, by = key_cols, all.x = T)
    
    # if we've concattenated numeric/logical columns for which there were never more than 1 value,
    # set column back to original column class (i.e. not character)
    for (cn in concat_cols) {
      if (col_classes[[cn]] %in% c("numeric", "logical")
          && ! TRUE %in% str_detect(out[[cn]], sep)   # concatenated with &
          && ! TRUE %in% str_detect(out[[cn]], "Values >")  # collapsed because too long
      ) {
        if (col_classes[[cn]] == "numeric") set(out, j = cn, value = as.numeric(out[[cn]]))
        if (col_classes[[cn]] == "logical") set(out, j = cn, value = as.logical(out[[cn]]))
      }
    }
  }
  
  if (!is.null(sum_cols)) {
    out <- merge(out, dt[, lapply(.SD, sum, na.rm = T), by = key_cols, .SDcols = sum_cols],
                 by = key_cols, all.x = T)
  }
  
  if (!is.null(mean_cols)) {
    out <- merge(out, dt[, lapply(.SD, mean, na.rm = T), by = key_cols, .SDcols = mean_cols],
                 by = key_cols, all.x = T)
  }
  
  if (!is.null(max_cols)) {
    out <- merge(out, dt[, lapply(.SD, max, na.rm = T), by = key_cols, .SDcols = max_cols],
                 by = key_cols, all.x = T)
  }
  
  if (!is.null(min_cols)) {
    out <- merge(out, dt[, lapply(.SD, min, na.rm = T), by = key_cols, .SDcols = min_cols],
                 by = key_cols, all.x = T)
  }
  
  if (!is.null(weighted_average)) {
    for (cn in weighted_average) dt[, (cn) := get(cn) * get(weighted_average_by)]  
    out <- merge(out, dt[, lapply(.SD, sum, na.rm = T), by = key_cols, .SDcols = weighted_average],
                 by = key_cols, all.x = T)
    for (cn in weighted_average) out[, (cn) := ifelse(get(weighted_average_by) > 0, get(cn) / get(weighted_average_by), 0)]  
  }
  dt <- out
  if ("___MERGE_BY___" %in% names(dt)) dt[, `___MERGE_BY___` := NULL]
  
  if (verbose) cat("Merged", (n_rows - dt[, .N]), "duplicates\n")
  setcolorder(dt, intersect(col_order, names(dt)))
  
  return(dt)
}


# Get the class (e.g., "numeric", "character") of the indicated columns (name or vector of names)
# in the indicated table
# Returns a named vector with the class of each of the columns in `columns`,
# or of all columns if `columns` is NULL
get_column_classes <- function(dt, columns = NULL) {
  if (is.null(columns)) columns <- names(dt)
  return(sapply(dt, class)[names(dt) %in% columns])
}