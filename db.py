#!/usr/bin/python
# encoding=utf8
def drop_table(engine, declare_class):
    table = declare_class.__table__
    table.metadata.bind = engine
    if table.exists():
        table.drop()
        "DROPPED TABLE: '%s'" %declare_class.__tablename__
    else:
        "TABLE '%s' NOT EXIST" %declare_class.__tablename__
 
def create_table(engine, declare_class, drop_before_create=False):
    if drop_before_create:
        drop_table(engine, declare_class)

    table = declare_class.__table__
    table.metadata.bind = engine
    if not table.exists():
        table.create()
        print "CREATED TABLE: '%s'" %declare_class.__tablename__
    else:
        print "TABLE '%s' EXISTS" %declare_class.__tablename__
