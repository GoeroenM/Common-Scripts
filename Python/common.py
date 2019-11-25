import pandas as pd
import numpy as np
import os
import sys

# Change wd to current directory
os.chdir('C:\\Users\\Goeroen\\Documents\\Python Scripts')

test = pd.read_csv('test.csv')
test = pd.read_csv('test2.csv')

# Function that flattens a list of lists
def flatten_list(l):
    # First remove all None elements from the list
    l = [i for i in l if i]
    flat_list = [item for sublist in l for item in sublist]
    return flat_list

# Function that merges rows with duplicated values in key_cols plus performs
# several optional operations.
#
# args:
#   - df: a pandas dataframe
#   - key_cols: key columns in which you want to check for duplicates
#   - concat_cols: columns for which the values will be concatenated using
#                  the separator sep
#   - sep: separator to use for concatenating
#   - sum_col: columns to sum over duplicates
#   - mean_cols: colums for which to take the mean over duplicates
#   - max_cols: columns for which to take the max
#   - min_cols: columns for which to take the min
#   - verbose: whether or not to print certain warnigns/info
#   - ignore_unique_columns: if any columns that are not in your key_cols only
#                            a single unique value, you can use this to ignore
#                            the concat operation for these columns.
#                            Default = True
#   - concat_unique: whether or not to only concat unique values or all values
#                    Default = True
#   - set_nan_to_zero: whether or not to set NaN values in numeric columns to
#                      zero or leave them be.
def merge_duplicated_rows(df, key_cols = None, concat_cols = None,
                          sum_cols = None, mean_cols = None, max_cols = None,
                          min_cols = None, sep = ";", verbose = True,
                          ignore_unique_columns = True, concat_unique = True,
                          set_nan_to_zero = False):
    
    if key_cols == None or key_cols== "":
        if verbose:
            print("No key columns given. \
                  Will check for duplicates in all variables.")
    
    # Check if any duplicates actually exist
    if (df[key_cols].duplicated(keep = False).sum() == 0):
        if verbose:
            print("No duplicates found.")
        return df
    
    # Set nan to zero if specified
    if set_nan_to_zero:
        num_cols = df.select_dtypes(include = np.number).columns.tolist()
        df[num_cols] = df[num_cols].fillna(0)
    
    # Keep some variables
    col_order = list(df)
    # col_classes = df.dtypes
    n_rows = len(df.index)
    
    if concat_cols == None:
        flat_args = flatten_list([key_cols, sum_cols, mean_cols, 
                                  max_cols, min_cols])
        s = set(flat_args)
        concat_cols = [x for x in col_order if x not in s]
        if verbose:
            print("No concat_cols specified. Using all columns not listed in",
                  end = ' ')
            print("any other argument: "+str(concat_cols)+".\n")
    
    # Create dummy variable on which we'll add the rest.
    # Keep unique instances of key variables.
    out = df[key_cols].drop_duplicates()
    
    # Ignore unique columns if specified
    if ignore_unique_columns:
        unique_values = pd.DataFrame(df[concat_cols].nunique())
        unique_cols = unique_values.index[unique_values[0] == 1].tolist()
        # Remove unique_cols from concat_cols
        if len(unique_cols) > 0:
            concat_cols = [x for x in concat_cols if x not in set(unique_cols)]
            merge_cols_ignore = flatten_list([key_cols, unique_cols])
            out = pd.merge(out, df[merge_cols_ignore].drop_duplicates(),
                        on = key_cols, how = 'left')
    
    # Perform operations.
    if concat_cols != None:
        # First check if any of the concat_cols are numeric, if so, convert to
        # string.
        if len(df[concat_cols].select_dtypes(include = np.number).columns) > 0:
            num_cols = df[concat_cols].select_dtypes(include = np.number).\
                      columns.tolist()
            if verbose:
                print("Some concat_cols were numeric. They will be converted",
                      end = ' ')
                print("to strings.\n")
                print("The columns converted are: "+str(num_cols))
            
            df[num_cols] = df[num_cols].astype(str)
        if concat_unique:
            merge_cols_concat = flatten_list([key_cols, concat_cols])
            out = pd.merge(out, df[merge_cols_concat].groupby(key_cols).
                           agg(lambda col: sep.join(col.unique())),
                           on = key_cols, how = 'left')
        else:
            merge_cols_concat = flatten_list([key_cols, concat_cols])
            out = pd.merge(out, df[merge_cols_concat].groupby(key_cols).
                           agg(lambda col: sep.join(col)),
                           on = key_cols, how = 'left')
        
    if sum_cols != None:
        merge_cols_sum = flatten_list([key_cols, sum_cols])
        out = pd.merge(out, df[merge_cols_sum].groupby(key_cols).sum(),
                       on = key_cols, how = 'left')
    
    if mean_cols != None:
        merge_cols_mean = flatten_list([key_cols, mean_cols])
        out = pd.merge(out, df[merge_cols_mean].groupby(key_cols).mean(),
                       on = key_cols, how = 'left')
    
    if min_cols != None:
        merge_cols_min = flatten_list([key_cols, min_cols])
        out = pd.merge(out, df[merge_cols_min].groupby(key_cols).min(),
                       on = key_cols, how = 'left')
    if max_cols != None:
        merge_cols_max = flatten_list([key_cols, max_cols])
        out = pd.merge(out, df[merge_cols_max].groupby(key_cols).max(),
                       on = key_cols, how = 'left')
    
    if verbose:
        print("Merged "+str(n_rows - len(out.index))+" duplicates.\n")
    
    # Reorder the columns to the original order.
    out = out.reindex(columns = col_order)
    
    # Reset index
    out = out.reset_index(drop = True)
    
    return out

# Function that checks whether or not your dataframe has duplicates and stops
# if specified.
def check_duplicates(df, key_cols = None, verbose = True, stop = True):
    if key_cols == None:
        print("Please provide key_cols to check duplicates.")
        return
    n_duplicates = df[key_cols].duplicated(keep = False).sum()
    if n_duplicates > 0:
        print("You have "+str(n_duplicates)+" duplicates.\n")
        if stop:
            sys.exit()
    else:
        if verbose:
            print("You have no duplicates.\n")

# Function that checks some basic properties of a dataframe
# args:
#   - df: dataframe to check
#   - key_col: key columns to check for duplicates
#   - check_nan: whether or not any columns contain NaN values.
#                Either give a list of columns to check or if you want all
#                all columns checked, just provide True.
#   - check_square: check if the dataset is 'square' in the columns you give.
#                   This means that the dataset has an equal amount of entries
#                   for all combinations of the variables you give here.
def check_df(df, check_duplicates = None, check_nan = None, check_square = None,
             verbose = True):
    if check_duplicates != None:
        check_duplicates(df, check_duplicates, verbose, stop = False)
    
    if check_nan != None:
        if check_nan == True:
            check_nan = list(df)

        if df[check_nan].isna().values.any():
            nan_cols = \
            df[check_nan].columns[df[check_nan].isna().any()].tolist()
            if verbose:
                print(str("Some numeric columns contain NaN values. Namely: "+
                          str(nan_cols)))
                
    if check_square != None:
        # Safest way to check is to work with a dummy table and column
        df_dummy = df[check_square]
        df_dummy['dummy'] = 1
        if len(df_dummy.groupby(check_duplicates).count()['dummy'].unique()) > 1:
            if verbose:
                print(str("Your data is not square in the variables: "+\
                          str(check_square)+".\n"))

# Function that flags any duplicated values in a new column called 'duplicated'
# Use keep = True if you only consider repeat values to be duplicates and not
# the original.
def flag_duplicates(df, key_cols = None, keep = False):
    if key_cols == None:
        print("Please provide key_cols to check duplicates.")
        return
    df['duplicated'] = df[key_cols].duplicated(keep = keep)
    return df

# Function that splits any cell in columns 'cols' on a given separator and
# creates new rows for each split element.
# E.g.: df =  colname  x  y     ->     output =  colname  x  y
#              a;b     1  2                         a     1  2
#               c      3  4                         b     1  2 
#                                                   c     3  4
def unlist_columns(df, cols = None, sep = ";"):
    if cols == None:
        print("Please provide a list of colums to split on.")
        return
    # Save original column order
    col_order = list(df)
    # Loop over all columns given
    for col in cols:
        # Create a dummy column to merge back on after splitting
        df['id'] = df.reset_index().index
        # Create dummy dataframe by splitting the column and then using stack
        # to create a single dataframe
        df_dummy = pd.DataFrame(df[col].str.split(sep).tolist(), 
                                index=df['id']).stack()
        # This recreates the id column with the id numbers that were originally
        # in df
        df_dummy = df_dummy.reset_index([0, 'id'])
        # Rename the column to its original name
        df_dummy = df_dummy.rename(columns = {0:col})
        # Remove the original column as we'll be adding it back
        del df[col]
        # Use a right merge to duplicate values of the other rows
        df = pd.merge(df, df_dummy, on = 'id', how = 'right')
        # Delete the dummy
        del df['id']
    
    # Reorder the columns to the original order.
    df = df.reindex(columns = col_order)
    
    # Reset index.
    df = df.reset_index(drop = True)
    
    return df

# Function that squares a dataset. This means that it will make sure that there
# are an equal amount of entries for the variable square_by on the combination
# of key_cols.
def square_dataset(df, set_nan_to_zero = True, square_by = None,
                   key_cols = None):
    if square_by == None or key_cols == None:
        print("Please supply the necessary arguments to run this function.")
        return
    
    # Check if data is already square, if so, no need to do anything.
    df['dummy'] = 1
    if len(df.groupby(key_cols).count()['dummy'].unique()) == 1:
        print("Data is already square.")
        del df['dummy']
        return df
    
    # Add the dummy column to square_by and key_cols in order to merge on
    square_by.append('dummy')
    key_cols.append('dummy')
    # Create a dummy dataframe that has all possible combinations of both
    # key_cols and square_by
    square_dummy = df[square_by].drop_duplicates()  
    df_dummy = df[key_cols].drop_duplicates()
    df_dummy = pd.merge(df_dummy, square_dummy, on = ['dummy'])
    
    del df['dummy']
    del df_dummy['dummy'] 
    
    # Now do a right join on the original dataframe to create square dataset
    merge_on = key_cols + square_by
    # Remove dummy from the list
    merge_on = [x for x in merge_on if x != 'dummy']
 
    df = pd.merge(df, df_dummy, on = merge_on, how = 'right')
    df.sort_values(merge_on)
    
    # Set numerical nans to zero if specified
    if set_nan_to_zero:
        num_cols = df.select_dtypes(include = np.number).columns.tolist()
        df[num_cols] = df[num_cols].fillna(0)
    
    
    return df