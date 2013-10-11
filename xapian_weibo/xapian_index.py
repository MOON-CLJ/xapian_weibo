# -*- coding: utf-8 -*-

from xapian_backend import _database, Schema, DOCUMENT_ID_TERM_PREFIX, \
    InvalidIndexError, _index_field
from utils import load_scws, log_to_stub
from consts import XAPIAN_DATA_DIR, XAPIAN_STUB_FILE_DIR
from datetime import datetime
import xapian
import msgpack
import os


class XapianIndex(object):
    def __init__(self, dbpath, schema_version, pid, remote_stub):
        self.dbpath = dbpath
        self.remote_stub = remote_stub
        self.schema_version = schema_version
        self.schema = getattr(Schema, 'v%s' % schema_version)
        today_date_str = datetime.now().date().strftime("%Y%m%d")
        self.db_folder = os.path.join(XAPIAN_DATA_DIR, '%s/_%s_%s' % (today_date_str, dbpath, pid))
        self.s = load_scws()
        self.db = _database(self.db_folder, writable=True)

        self.termgen = xapian.TermGenerator()
        self.iter_keys = self.schema['origin_data_iter_keys']
        self.pre_func = self.schema.get('pre_func', {})

    def document_count(self):
        try:
            return _database(self.db_folder).get_doccount()
        except InvalidIndexError:
            return 0

    def add(self, item):
        document = xapian.Document()
        document_id = DOCUMENT_ID_TERM_PREFIX + str(item[self.schema['obj_id']])
        for field in self.schema['idx_fields']:
            self.index_field(field, document, item, self.schema_version)

        # origin_data跟term和value的处理方式不一样
        item = dict([(k, self.pre_func[k](item.get(k)) if k in self.pre_func and item.get(k) else item.get(k))
                     for k in self.iter_keys])
        document.set_data(msgpack.packb(item))
        document.add_term(document_id)
        # self.db.replace_document(document_id, document)
        self.db.add_document(document)

    def index_field(self, field, document, item, schema_version):
        _index_field(field, document, item, schema_version, self.schema, self.termgen)

    def _log_to_stub(self):
        log_to_stub(XAPIAN_STUB_FILE_DIR, self.dbpath, self.db_folder, remote_stub=self.remote_stub)

    def close(self):
        self.db.close()
        self._log_to_stub()
        print 'total index', self.document_count()
