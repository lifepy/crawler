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
 
def create_table(engine, declare_class, checkfirst=True):
    table = declare_class.__table__
    table.metadata.bind = engine
    if table.exists():
        choice = raw_input('The table you want to create already exists!\nIf you want to recreate database schema, everything in `%s` will be lost.\nAre you sure you want to recreate (type yes for confirmation, enter to cancel) [yes/NO]?'%declare_class.__tablename__)
        if choice == 'yes':
            drop_table(engine, declare_class)
    table.create()
    print "CREATED TABLE: '%s'" %declare_class.__tablename__

