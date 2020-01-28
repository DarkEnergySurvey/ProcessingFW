# $Id: errors.py 41004 2015-12-11 15:49:41Z mgower $
# $Rev:: 41004                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2015-12-11 09:49:41 #$:  # Date of last commit.

# pylint: disable=print-statement

"""
    Define some exceptions.

    Developed at:
    The National Center for Supercomputing Applications(NCSA).

    Copyright(C) 2012 Board of Trustees of the University of Illinois.
    All rights reserved.
"""

__version__ = "$Rev: 41004 $"

class MetadataConfigError(Exception):
    """ Represent an error in the METADATA and/or FILETYPE tables.
    """

    def __init__(self, msg):
        super().__init__(msg)

class DuplicateDBFiletypeError(MetadataConfigError):
    """ Duplicate filetype in the FILETYPE table in the database.
    """

    def __init__(self, msg=None):
        if not msg:
            msg = "Filetypes differing only by case exist in filetype table."
        super().__init__(msg)

class DuplicateDBHeaderError(MetadataConfigError):
    """ Duplicate header in the METADATA table in the database.
    """

    def __init__(self, msg=None):
        if not msg:
            msg = "Header names differing only by case exist in metadata table."
        super().__init__(msg)

class IdMetadataHeaderError(MetadataConfigError):
    """ Disallowed id header found in METADATA table in the database.
    """

    def __init__(self, msg=None):
        if not msg:
            msg = 'Id header found in metadata table.'
        super().__init__(msg)

class FileMetadataIngestError(Exception):
    """ Represent an error in the file metadata ingest routines.
    """

    def __init__(self, msg):
        super().__init__(msg)

class RequiredMetadataMissingError(FileMetadataIngestError):
    """ A required file metadata element was not found in the dataset to ingest.
    """

    def __init__(self, msg=None):
        if not msg:
            msg = "Required metadata element was not found."
        super().__init__(msg)

class DBMetadataNotFoundError(FileMetadataIngestError):
    "There is an unknown filetype in the file submitted for ingest."

    def __init__(self, msg=None):
        if not msg:
            msg = "The file's filetype was not found in the database."
        super().__init__(msg)
