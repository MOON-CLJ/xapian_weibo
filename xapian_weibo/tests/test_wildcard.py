#!/usr/bin/env python
# -*- coding: utf-8 -*-

import xapian

stem = xapian.Stem('english')

db = xapian.inmemory_open()
doc = xapian.Document()
doc.add_posting(stem("is"), 1)
doc.add_posting(stem("there"), 2)
doc.add_posting(stem("anybody"), 3)
doc.add_posting(stem("out"), 4)
doc.add_posting(stem("there"), 5)
db.add_document(doc)

qp = xapian.QueryParser()
qp.set_database(db)

new_query = qp.parse_query('h*', qp.FLAG_WILDCARD)
print [str(new_query)]
