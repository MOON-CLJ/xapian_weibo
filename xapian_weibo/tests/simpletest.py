#!/usr/bin/env python

import sys
import xapian
import string

if len(sys.argv) != 2:
    print >> sys.stderr, "Usage: %s PATH_TO_DATABASE" % sys.argv[0]
    sys.exit(1)

try:
    # Open the database for update, creating a new database if necessary.
    database = xapian.WritableDatabase(sys.argv[1], xapian.DB_CREATE_OR_OPEN)
    print database,'build success'
    for i in range(100000000):
        doc = xapian.Document()
        doc.set_data(str(i))

        indexer = xapian.TermGenerator()
        indexer.set_document(doc)
        indexer.index_text(str(i))
        database.add_document(doc)
        if i % 100000 == 0:
            print i

except Exception, e:
    print >> sys.stderr, "Exception: %s" % str(e)
    sys.exit(1)
