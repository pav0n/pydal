# -*- coding: utf-8 -*-
__author__ = "pav0n"

import re
import time
from .._globals import IDENTITY
from ..helpers.methods import varquote_aux
from .base import NoSQLAdapter, BaseAdapter
from ..drivers import PlainTextAuthProvider
from ..drivers import ConsistencyLevel
from ..drivers import SimpleStatement
from ..helpers.methods import uuid2int
from uuid import uuid4

class CassandraAdapter(BaseAdapter):
    drivers = ('cassandra',)

    commit_on_alter_table = True
    support_distributed_transaction = True
    types = {
        'boolean': 'boolean',
        'string': 'VARCHAR',
        'text': 'TEXT',
        'json': 'TEXT',
        'password': 'VARCHAR',
        'blob': 'BLOB',
        'upload': 'VARCHAR',
        'integer': 'INT',
        'bigint': 'BIGINT',
        'float': 'FLOAT',
        'double': 'DOUBLE',
        'decimal': 'DECIMAL',
        'date': 'TIMESTAMP',
        'time': 'TIMESTAMP',
        'datetime': 'TIMESTAMP',
        'id': 'UUID PRIMARY KEY',
        'uuid': 'UUID',
        'reference': 'BIGINT',
        'list:integer': 'TEXT',
        'list:string': 'TEXT',
        'list:reference': 'TEXT',
        'big-reference': 'BIGINT'
    }

    QUOTE_TEMPLATE = "`%s`"


    REGEX_URI = re.compile('^(?P<user>[^:@]+)(\:(?P<password>[^@]*))?@(?P<host>[^\:/]+)(\:(?P<port>[0-9]+))?/(?P<db>[^?]+)(\?set_encoding=(?P<charset>\w+))?$')

    def __init__(self,db,uri,pool_size=0,folder=None,db_codec ='UTF-8',
                 credential_decoder=IDENTITY, driver_args={},
                 adapter_args={}, do_connect=True, after_connection=None):
        self.db = db
        self.dbengine = "cassandra"
        self.uri = uri
        if do_connect: self.find_driver(adapter_args,uri)
        self.pool_size = pool_size
        self.folder = folder
        self.db_codec = db_codec
        self._after_connection = after_connection
        self.find_or_make_work_folder()
        import random
        self.random = random
        self.TRUE_exp = 'TRUE'
        self.FALSE_exp = 'FALSE'
        self._last_insert = None
        ruri = uri.split('://',1)[1]
        m = self.REGEX_URI.match(ruri)
        if not m:
            raise SyntaxError(
                "Invalid URI string in DAL: %s" % self.uri)
        user = credential_decoder(m.group('user'))
        if not user:
            raise SyntaxError('User required')
        password = credential_decoder(m.group('password'))
        if not password:
            password = ''
        host = m.group('host')
        if not host:
            raise SyntaxError('Host name required')
        db = m.group('db')
        if not db:
            raise SyntaxError('Database name required')
        port = int(m.group('port') or '9042')
        charset = m.group('charset') or 'utf8'
        auth_provider = PlainTextAuthProvider(username=credential_decoder(user), password=credential_decoder(password))
        driver_args.update(auth_provider=auth_provider,
                           protocol_version=2,
                           contact_points=[host],
                           port=port,
                           compression=False,
                           max_schema_agreement_wait=0)

        def connector(driver_args=driver_args,keyspace=db):
            cluster = self.driver(**driver_args)
            session = cluster.connect(keyspace)
            session.rollback = lambda : ''
            session.commit = lambda : ''
            session.cursor = lambda : session
            print 'conectar'
            #query = SimpleStatement("INSERT INTO users (name, age) VALUES (%s, %s)",consistency_level=ConsistencyLevel.QUORUM)
            #session.execute('create table emp_test (empid int primary key, emp_first varchar, emp_last varchar, emp_dept varchar)')
            return  session
        self.connector = connector
        if do_connect: self.reconnect()

    def lastrowid(self,table):
        self.execute('select last_insert_id();')
        return int(self.cursor.fetchone()[0])
    
    def insert(self, table, fields, safe=None):
        """Safe determines whether a asynchronous request is done or a
        synchronous action is done
        For safety, we use by default synchronous requests"""
        query = self._insert(table,fields)
        self.cursor.execute(SimpleStatement(query,consistency_level=ConsistencyLevel.ONE))
        return None
    def _insert(self, table, fields):
        table_rname = table.sqlsafe
        if fields:
            keys = ','.join(f.sqlsafe_name for f, v in fields)
            values = ','.join(self.expand(v, f.type) for f, v in fields)
            if getattr(table,'_id',None):
                if(table._id in keys.split(',')):
                    keys += ',%s'%table._id.name
                    values += ',%s'%uuid4()
            return 'INSERT INTO %s(%s) VALUES (%s);' % (table_rname, keys, values)
        else:
            return self._insert_empty(table)

    def represent_exceptions(self, obj, fieldtype):
        if fieldtype in ('string','text'):
            return "'%s'" % obj
        else:
            return obj
        
    def close_connection(self):
        if self.connection:
            self.connection.cluster.shutdown()
            r = self.connection.shutdown()
            self.connection = None
            return r
        log.info('Connection closed.')
            
    def execute(self,a): pass
