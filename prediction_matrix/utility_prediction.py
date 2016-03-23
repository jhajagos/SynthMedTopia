"""
Utility function
"""


def get_all_paths(h5py_group):
    """Recurse and get all non-groups"""
    non_groups = []
    for group_name in h5py_group:
        if not h5py_group[group_name].__class__ == h5py_group.__class__:
            non_groups += [h5py_group[group_name].name]
        else:
            non_groups.extend(get_all_paths(h5py_group[group_name]))

    if len(non_groups):
        return non_groups


def copy_data_set(h5p1, h5p2, path, compression="gzip"):

    ds1 = h5p1[path]
    source_shape = ds1.shape
    source_dtype = ds1.dtype

    ds2 = h5p2.create_dataset(path, shape=source_shape, dtype=source_dtype, compression=compression)
    ds2[...] = ds1[...]


def create_dataset_with_new_number_of_rows(h5p1, h5p2, path, new_number_rows, compression="gzip"):
    ds1 = h5p1[path]
    source_dtype = ds1.dtype

    updated_shape = (new_number_rows, ds1.shape[1])
    ds2 = h5p2.create_dataset(path, shape=updated_shape, dtype=source_dtype, compression=compression)
    return ds2


def copy_into_dataset_starting_at(ds1, h5p2, path, starting_position):
    ds2 = h5p2[path]
    ds2_shape = ds2.shape
    ds2_rows = ds2_shape[0]
    ending_position = starting_position + ds2_rows

    ds1[starting_position : ending_position] = ds2[...]
    return ending_position