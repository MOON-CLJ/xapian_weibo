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

doc1 = xapian.Document()
doc1.add_posting(stem("is"), 1)
doc1.add_posting(stem("there"), 2)
doc1.add_posting(stem("anybody"), 3)
doc1.add_posting(stem("out"), 4)
doc1.add_posting(stem("there"), 5)
db.add_document(doc1)
db.commit()

for term in db.allterms():
    print term.term, term.termfreq
"""
    anybodi 2
    is 2
    out 2
    there 2
    可见stem的作用
"""

print "** " * 10
db = xapian.inmemory_open()
doc1 = xapian.Document()
doc1.add_posting(stem("新浪 微博"), 1)
db.add_document(doc1)
db.commit()

for term in db.allterms():
    print term.term, term.termfreq
"""
    新浪 微博 1
    可以直接指定带空格的term
"""


print "** " * 10
for term in doc.termlist():
    print term.term, term.wdf, list(term.positer)
"""
    anybodi 1 [3L]
    is 1 [1L]
    out 1 [4L]
    there 2 [2L, 5L]
"""

print "** " * 10
termgen = xapian.TermGenerator()
doc = xapian.Document()
termgen.set_document(doc)
termgen.index_text('hello world helllooo woooord hello word')
print [(item.term, item.wdf, [pos for pos in item.positer]) for item in doc.termlist()]
"""
    [('helllooo', 1L, [3L]), ('hello', 2L, [1L, 5L]), ('woooord', 1L, [4L]), ('word', 1L, [6L]), ('world', 1L, [2L])]
"""

print "** " * 10
termgen = xapian.TermGenerator()
doc = xapian.Document()
termgen.set_document(doc)
termgen.index_text_without_positions('hello world helllooo woooord hello word')
print [(item.term, item.wdf, [pos for pos in item.positer]) for item in doc.termlist()]
"""
    [('helllooo', 1L, []), ('hello', 2L, []), ('woooord', 1L, []), ('word', 1L, []), ('world', 1L, [])]
"""

print "** " * 10
termgen = xapian.TermGenerator()
doc = xapian.Document()
termgen.set_document(doc)
termgen.index_text_without_positions('hello world helllooo woooord hello word', 1, 'T')
print [(item.term, item.wdf, [pos for pos in item.positer]) for item in doc.termlist()]
"""
    [('Thelllooo', 1L, []), ('Thello', 2L, []), ('Twoooord', 1L, []), ('Tword', 1L, []), ('Tworld', 1L, [])]
"""


print "** " * 10
termgen = xapian.TermGenerator()
doc = xapian.Document()
termgen.set_document(doc)
stop = xapian.SimpleStopper()
termgen.set_stopper(stop)
stop.add('hello')
termgen.index_text_without_positions('hello world helllooo woooord hello word')
print [(item.term, item.wdf, [pos for pos in item.positer]) for item in doc.termlist()]
"""
    ? not work
    [('helllooo', 1L, []), ('hello', 2L, []), ('woooord', 1L, []), ('word', 1L, []), ('world', 1L, [])]
"""

print "** " * 10
termgen = xapian.TermGenerator()
doc = xapian.Document()
termgen.set_document(doc)
termgen.set_max_word_length(5)
termgen.index_text_without_positions('9' * 6)
print [(item.term, item.wdf, [pos for pos in item.positer]) for item in doc.termlist()]
"""
    []
"""
