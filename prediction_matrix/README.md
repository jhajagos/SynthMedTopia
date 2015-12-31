PredictionMatrix
=============

This code transforms rows from a relational database into to a rich document structure and then to a matrix format for
machine learning applications. The rich document structure is formatted in a near
readable JSON format. The self describing matrix format is HDF5 which can be read by a wide range of scientific
programming environments including Matlab, scikits via h5py, Mathematica and R.

Code for mapping relation databases to JSON documents and for mapping JSON documents into HDF5 matrices for
so to be used in machine learning applications.

This code started out as a mapper for relational data into a format that could be used to easily train machine learning 
algorithms for hospital readmission. The examples in the tests are formatted around this use case. The two 
transforming programs are "build_document_mapping_from_db.py" and "build_hdf5_matrix_from_document.py" are not limited to
the readmission use case and have been designed to generate very large matrices. 
