# Testing addition and overwrite to csv file using pandas

import pandas as pd

dataset = {
    'city': ['alpha','beta','charlie'],
    'population': ['1','2','3'],
    'age': ['1000', '1000', '200'],
    'harbour': ['1', '0', '1'],
}

main = pd.DataFrame(data=dataset)

updates = {
    'city': ['alpha', 'gamma'],
    'population': ['1', '4'],
    'age':['1000', '100'],
    'harbour': ['0', '1'],
}

def add_update_entry(dataframe, entryarr, colset):
    # Dataframe - main pandas dataframe
    # Entryarr  - array of row-like items for this entry
    # colset    - pairs of column name to index for the matchmaking
    #             i.e overwrite when matching 1st 2nd and 4th columns
    queries = []
    for column, index in colset:
        value = entryarr[index]
        queries.append(f"{column} == '{value}'")

    query = ' and '.join(queries)
    result = dataframe.query(query)
    if not result.empty:
        dataframe = dataframe.drop(result.index)
        add_ind = result.index[0]
    else:
        add_ind = len(dataframe)

    # Add entry
    dataframe.loc[add] = entryarr
    return dataframe

print(main)
main = add_update_entry(main, ['alpha', '2', '1000', '12'], [('city',0),('population', 1), ('age', 2)])
print(main)


# Update rules: for each new entry:
# - check first n entries
#   - if there is no match just add this entry to the 'add_entries' dict
#     - Assuming there are no duplicates in the add_entries dict
#     - (add kerchunk file to add_kerchunks list)
#   - if there is a match, check all entries
#     - if there is a full match, ignore this entry
#     - if no full match, update the entry
#     - (if clobber is true, add kerchunk file to add_kerchunks list)