library(data.table)

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
merge_duplicated_rows <- function(dt, key_cols = NULL, concat_cols = NULL, sum_cols = NULL, mean_cols = NULL, 
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
    if (verbose) cat("Merging all rows.\n")
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
    concat_outcome <- dt[, lapply(.SD, function(x) {
      values <- na.omit(unique(x))
      if (length(values) > concat_max_values) {
        str_c("< ", length(values), " Values >")  # return simplified string
      } else {
        str_c(values, collapse = sep) # return all values concatenated
      }}),
      by = key_cols, .SDcols = concat_cols]
    
    out <- merge(out, concat_outcome, by = key_cols, all.x = T)
    
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

# Check if any rows have duplicated values on the indicated by columns
# Output: a stop indicating nb of duplicates, or a message that you have none
check_duplicates <- function(dt, key_cols = NULL, stop = T, verbose = T) {
  
  if (is.null(key_cols)) {
    message("Please provide key_cols to check duplicates.")
    return()
  }
  n_duplicates <- sum(duplicated(dt[, mget(key_cols)]))
  if (n_duplicates > 0) {
    msg <- paste("You have ", n_duplicates, " duplicates (not counting the original rows).\n")
    if (stop) {
      stop(msg)
    } else {
      message(msg)
    }
  } else {
    if (verbose) cat("You have no duplicates.\n")
  }
}

# check if you have duplicates, a square dataset, or any NA values in your data
#
# Args:
#       - dt: data.table
#       - check_duplicates: check for duplicates on these columns
#       - check_square: variables to check squareness on. This means that the data has an equal amount of entries
#                       for the combination of these variables.
check_dt <- function(dt, check_duplicates = NULL, check_na = NULL, check_square = NULL) {
  if (is.null(check_duplicates) && is.null(check_na) && is.null(check_square)) {
    message("Please give me something to check.")
    stop()
  }
  
  # Check if any NA values exist in the provided columns (or all)
  if (!is.null(check_na)) {
    if (check_na == T) {
      check_na <- names(dt)
    }
    na_cols <- sapply(dt[, check_na, with = F], function(x) any(which(is.na(x))))
    na_cols <- names(na_cols)[na_cols == TRUE]
    if (length(na_cols) > 0) {
      message(paste0(c("Some columns contain NA values. Namely: ", na_cols), collapse = " "))
    }
  }
  
  # check for duplicate key_cols combinations
  if (!is.null(check_duplicates)) {
    check_duplicates(dt, check_duplicates, stop = T, verbose = T)
  }

  if (check_square) {
    if (length(unique(dt[, .N, by = check_square]$N)) > 1) {
      stop(paste0(c("Your data is not square in the variables: ", check_square), collapse = " "))
    }
  }
}

# Find duplicates on the indicated columns (i.e. any row with identical values on these columns
# will be flagged as a duplicate)
# Output is dt with a 'duplicated' column added (with T/F values)
# Note: the original row is also marked as duplicated, i.e. not like base::duplicated()
flag_duplicates <- function(dt, cols = NULL) {
  if (is.null(cols)) {
    cat("Please provide cols to check duplicates on.")
    return()
  }
  dt <- copy(dt)
  dt[, `:=` (ID = .I)]
  duplicate_ids <- unique(dt[duplicated(dt[, .SD, .SDcols = cols])
                            | duplicated(dt[, .SD, .SDcols = cols], fromLast = TRUE), ID])
  dt[, duplicated := ifelse(ID %in% duplicate_ids, TRUE, FALSE)]
  return(dt[, !"ID"])
}

# Small function that returns all rows of a data table which have more than a certain number of occurences in
# those combinations
# Args: - key_cols: column names of which you want to check duplicate combinations
#       - n_duplicates: number of row occurences higher than which you consider something duplicate
view_duplicates <- function(dt, key_cols = NULL, n_duplicates = 1) {
  out <- copy(dt)
  out[, count := .N, by = key_cols]
  out <- out[count > n_duplicates]
  return(out)
}

# ------------------------------- String / Missing Manipulation ---------------------------------------
# Set all NAs in character columns to an empty character.
# This function is mainly used for creating input for promo tools, as empty characters take up
# less memory in Excel than NA does.
set_na_to_empty_char <- function(dt) {
  return(set_na_to_char(dt, ""))
}

set_na_to_char <- function(dt, character) {
  dt <- copy(dt)
  non_num_col_indices <- which(sapply(dt, is.character))
  for (j in non_num_col_indices) set(dt, i = which(is.na(dt[[j]])), j = j, value = character)
  return(dt)
}



# Set all NA, NaN, and Inf values in any numeric columns in the indicated data.table to 0
set_non_numeric_values_to_zero <- function(dt) {
  return(set_non_numeric_values_to(dt, 0))
}



# Set all NA, NaN, and Inf values in any numeric columns in the indicated data.table to the indicated value
set_non_numeric_values_to <- function(dt, value) {
  dt <- copy(dt)
  num_col_indices <- which(sapply(dt, is.numeric))
  for (j in num_col_indices) set(dt, i = which(!is.finite(dt[[j]])), j = j, value = value)
  return(dt)
}



# round all numeric cols to this many decimals
round_num_cols <- function(dt, n_decimals) {
  dt <- copy(dt)
  num_cols <- names(dt)[sapply(dt, is.numeric)]
  for (cn in num_cols) dt[, (cn) := round(get(cn), n_decimals)]
  return(dt)
}



# Uppercase all character columns and trim whitespace
trim_and_uppercase <- function(dt) {
  char_cols <- which(sapply(dt, is.character))
  dt <- dt[, lapply(.SD, function(x) str_trim(str_to_upper(x))), .SDcols = char_cols]
  return(dt)
}



# Convert all numeric columns to character, using the indicated decimal separator
convert_num_cols_to_str <- function(dt, dec = ",") {
  dt <- copy(dt)
  num_col_indices <- which(sapply(dt, is.numeric))
  for (j in num_col_indices) set(dt, j = j, value = str_replace(as.character(dt[[j]]), "\\.", dec))
  return(dt)
}



# Replace certain character string in the column names of a table by another character.
# Mostly used to get rid of all spaces in column names, as they make working with a table more cumbersome.
replace_str_in_colnames <- function(dt, str_to_replace = " ", replace_by = "_") {
  colnames_to_fix <- names(dt)[str_detect(names(dt), str_to_replace)]
  for (col in colnames_to_fix) setnames(dt, col, str_replace_all(col, str_to_replace, replace_by))
}



# Remove 'bad' symbols from all character columns, replacing them with a space
# See code for included forbidden symbols (the first is an arrow I think)
# You will be notified if any changes that are made.
remove_bad_symbols <- function(dt, bad_symbols = c("_x001A_", "\032", "~", "â", "\t", "\n", "\r"),
                               replace_by = " ") {
  detection <- rbindlist(lapply(colnames(dt), function(x) {
    if (!is.numeric(dt[[x]])) {
      detect <- which(str_detect(dt[[x]], paste(bad_symbols, collapse = "|")))
      if (length(detect) > 0) {
        data.table(Variable = x, CHANGED = dt[[x]][detect])
      }
    }
  }))
  
  if (dim(detection)[1] > 0) {
    print(detection)
  }
  dt <- dt[, lapply(.SD, function(x) {
    if (!is.numeric(x)) {
      mgsub(bad_symbols, c(rep("", length(bad_symbols) - 1), replace_by), x)
    } else {
      x
    }}), .SDcols = colnames(dt)]
  return(dt)
}

# ------------------------ Stuff to do with factors ----------------------------

# Factorize the indicated columns. For large dts with character columns, this can reduce the object's size considerably
factorize <- function(dt, cols) {
  for (cn in cols) dt[, (cn) := as.factor(get(cn))]
  return(dt)
}



# Get rid of factor columns. Either convert all to character/numeric or work by naming the columns you want to change. 
# No need to specify convert_to if you're using num_cols and char_cols, can just leave it as default.
# This function mainly exists because working with factor columns can be annoying, and unless you're working with
# big datasets, there's no reason to use factors.
unfactor <- function(dt, num_cols = NULL, char_cols = NULL, convert_to = "character") {
  ### If everything needs to be converted to the same: either numeric or character.
  if (is.null(num_cols) && is.null(char_cols)) {
    factorCols <- names(dt)[which(sapply(dt, is.factor))]
    if (convert_to == "character") {
      for (fac in factorCols) dt[, (fac) := as.character(get(fac))]
    } else if (convert_to == "numeric") {
      for (fac in factorCols) dt[, (fac) := as.numeric(get(fac))]
    } else {
      message("Please tell me if you want to convert your factor columns to character or numeric. The current variable you provided was neither.")
      stop()
    }
    return(dt)
  } else { ### Change given columns to given class
    for (col in num_cols) dt[, (col) := as.numeric(get(col))]
    for (col in char_cols) dt[, (col) := as.character(get(col))]
    return(dt)
  }
}

# --------------------------------- Columns ---------------------------------
# Set the column order of dt, starting with columns in first_cols, then any columns not in first_cols
# or last_cols, then columns in last_cols
set_column_order <- function(dt, first_cols = NULL, last_cols = NULL) {
  if (is.null(first_cols) && is.null(last_cols)) {
    message("only NULL arguments passed, return dt as is")
    return(dt)
  }
  
  if (length(setdiff(c(first_cols, last_cols), names(dt))) > 0) {
    stop("Following columns are not in dt: ", str_c(setdiff(c(first_cols, last_cols), names(dt)), collapse = ", "))
  }
  
  setcolorder(dt, c(first_cols, setdiff(names(dt), c(first_cols, last_cols)), last_cols))
}

# Re-order the columns of dt such that the columns in cols_to_move (name or vector of names) are
# right after col_to_place_after (name)
place_columns_after <- function(dt, cols_to_move, col_to_place_after){
  if (length(setdiff(c(cols_to_move, col_to_place_after), names(dt))) > 0) {
    stop("Following columns are not in dt: ",
         str_c(setdiff(c(cols_to_move, col_to_place_after), names(dt)), collapse = ", "))
  }
  
  cols_before <- names(dt)[1 : which(names(dt) == col_to_place_after)]
  cols_before <- setdiff(cols_before, cols_to_move)
  
  if (which(names(dt) == col_to_place_after) < length(names(dt))) {
    cols_after <- names(dt)[(which(names(dt) == col_to_place_after) + 1) : length(names(dt))]
    cols_after <- setdiff(cols_after, cols_to_move)
  } else {
    cols_after <- NULL # place after the last column
  }
  
  setcolorder(dt, c(cols_before, cols_to_move, cols_after))
}

# Get the class (e.g., "numeric", "character") of the indicated columns (name or vector of names)
# in the indicated table
# Returns a named vector with the class of each of the columns in `columns`,
# or of all columns if `columns` is NULL
get_column_class <- function(dt, columns = NULL) {
  if (is.null(columns)) columns <- names(dt)
  return(sapply(dt, class)[names(dt) %in% columns])
}

# Set the class (e.g., "numeric", "character") of the variables in dt based on a named vector
# containing the class as value and the variable as name
#
# Returns dt with altered column classes.
#
# Any columns in dt that are not defined in cols_classes are left untouched.
#
# Args:
#     - dt
#     - cols_classes: named vector, where names reflect column names and values reflect
#         the corresponding column class
set_column_classes <- function(dt, cols_classes, verbose = T) {
  not_in_classes <- setdiff(names(dt), names(cols_classes))
  if (length(not_in_classes) > 0) {
    if (verbose) cat("Columns", str_c(not_in_classes, sep = ", "),
                     "are not in cols_classes. We will leave the class like it was.\n")
  }
  not_in_dt <- setdiff(names(cols_classes), names(dt))
  if (length(not_in_dt) > 0) {
    if (verbose) cat("Columns", str_c(not_in_dt, sep = ", "),
                     "are not in dt. We will not change these classes since they are not in dt.\n")
  }
  for (col in names(cols_classes)) {
    col_class <- cols_classes[names(cols_classes) == col]
    if (col_class == "numeric") {
      set(dt, j = col, value = as.numeric(dt[[col]]))
    } else if (col_class == "character") {
      set(dt, j = col, value = as.character(dt[[col]]))
    } else if (col_class == "integer") {
      set(dt, j = col, value = as.integer(dt[[col]]))
    } else if (col_class == "logical") {
      set(dt, j = col, value = as.logical(dt[[col]]))
    } else {
      stop(cat(col, "has class of", col_class, "which is not yet defined. Please add to function.\n"))
    }
  }
  return(dt)
}

