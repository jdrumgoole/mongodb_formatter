#!/bin/env python

import pymongo
import sys
import contextlib
from datetime import datetime
from pprint import pprint
import csv

from mongodb_formatter.nested_dict import Nested_Dict

class Transform(object):

    def __init__(self):
        pass

    def __call__(self, doc):
        return doc

class Date_Transform(object):

    def __init__(self, tformat=None):
        super().__init__()
        if tformat is None:
            self._tformat = "%d-%b-%Y %H:%M"
        else:
            self._tformat = tformat

    def __call__(self, doc):
        d = Nested_Dict(doc)
        if d.has_key(field):
            value = d.get_value(field)
            if isinstance(value, datetime):
                d.set_value(field, value.strftime(time_format))
            else:
                d.set_value(field, datetime.fromtimestamp(value / 1000))

        return d.dict_value()
class Cursor_Processor(object):

    def __init__(self, cursor):
        self._cursor = cursor
        self._xform_list = []

    def add_transform(self, xform:Transform):
        self._xform_list.append(xform)

    def transform(self, doc):
        for i in self._xform_list:
            doc = i(doc)
        return doc

    def process(self):
        for i in self._cursor:
            yield self.transform(i)


class Doc_Formatter(object):

    @staticmethod
    def date_map_field(doc, field, time_format=None):
        '''
        Given a field that contains a datetime we want it to be output as a string otherwise
        pprint and other functions will abandon ship when they meet BSON time objects
        '''

        if time_format is None:
            time_format = "%d-%b-%Y %H:%M"
        d = Nested_Dict(doc)
        if d.has_key(field):
            value = d.get_value(field)
            if isinstance(value, datetime):
                d.set_value(field, value.strftime(time_format))
            else:
                d.set_value(field, datetime.fromtimestamp(value / 1000))

        return d.dict_value()

    @staticmethod
    def select_fields(doc, field_list):
        '''
        Take 'doc' and create a new doc using only keys from the 'fields' list.
        Supports referencing fields using dotted notation "a.b.c" so we can parse
        nested fields the way MongoDB does. The nested field class is a hack. It should
        be a sub-class of dict.
        '''

        if field_list is None or len(field_list) == 0:
            return doc

        newDoc = Nested_Dict({})
        oldDoc = Nested_Dict(doc)

        for i in field_list:
            if oldDoc.has_key(i):
                # print( "doc: %s" % doc )
                # print( "i: %s" %i )
                newDoc.set_value(i, oldDoc.get_value(i))
        return newDoc.dict_value()

    @staticmethod
    def date_map(doc, datemap_list, time_format=None):
        '''
        For all the datetime fields in "datemap" find that key in doc and map the datetime object to
        a strftime string. This pprint and others will print out readable datetimes.
        '''
        if datemap_list:
            for i in datemap_list:
                if isinstance(i, datetime):
                    doc=CursorFormatter.date_map_field(doc, i, time_format=time_format)
        return doc

    def format(self,doc):
        new_doc = Doc_Formatter.select_fields( doc, self._select_fields)
        return Doc_Formatter.date_map( new_doc, self._date_fields)

    def __init__(self, doc, select_fields, date_fields):

        self._select_fields = select_fields
        self._date_fields = date_fields
        self._doc = doc

    def __call__(self):
        return self.format( self._doc)

class CursorFormatter(object):
    '''
    If root is a file name output the content to that file.
    '''

    def __init__(self, cursor, filename="", formatter="json"):


        self._filename  = filename
        self._formatter = formatter
        self._cursor    = cursor


    def results(self):
        return self._results

    @contextlib.contextmanager
    def _smart_open(self, filename=None):
        if filename and filename != '-':
            fh = open(filename, 'w')
        else:
            fh = sys.stdout

        try:
            yield fh
        finally:
            if fh is not sys.stdout:
                fh.close()

    def mapper(self, doc, field_map, date_map, time_format=None):
        return CursorFormatter.fieldMapper( doc, field_map ).

    def printCSVCursor(self, fieldnames, datemap, time_format=None):
        '''
        Output CSV format. items are separated by commas. We only output the fields listed
        in the 'fieldnames'. We datemap fields listed in 'datemap'. If a datemap listed field
        is not a datetime object we will thow an exception.
        '''

        with self._smart_open(self._filename) as output:
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            count = 0
            for i in self._cursor:
                self._results.append(i)
                count = count + 1
                d = CursorFormatter.fieldMapper(i, fieldnames)
                d = CursorFormatter.dateMapper(d, datemap, time_format)

                # x = {}
                # for k, v in d.items():
                #
                #     if type(v) is unicode:
                #         x[k] = v
                #     else:
                #         x[k] = str(v).encode('utf8')

                # writer.writerow({k: v.encode('utf8') for k, v in x.items()})

                writer.writerow(d)

        return count

    def printJSONCursor(self, fieldnames, datemap, time_format=None):
        """

        Output plan json objects.

        :param c: collection
        :param fieldnames: fieldnames to include in output
        :param datemap: fieldnames to map dates to date strings
        :param time_format: field names to map to a specific time format
        :return:
        """

        count = 0

        with self._smart_open(self._filename) as output:
            for i in self._cursor:
                # print( "processing: %s" % i )
                # print( "fieldnames: %s" % fieldnames )
                self._results.append(i)
                d = CursorFormatter.fieldMapper(i, fieldnames)
                # print( "processing fieldmapper: %s" % d )
                d = CursorFormatter.dateMapper(d, datemap, time_format)
                pprint.pprint(d, output)
                count = count + 1

        return count

    def printCursor(self, fieldnames=None, datemap=None, time_format=None):
        '''
        Output a cursor to a filename or stdout if filename is "-".
        fmt defines whether we output CSV or JSON.
        '''

        if self._format == 'csv':
            count = self.printCSVCursor(fieldnames, datemap, time_format)
        else:
            count = self.printJSONCursor( fieldnames, datemap, time_format)

        return count

    def output(self, fieldNames=None, datemap=None, time_format=None):
        '''
        Output all fields using the fieldNames list. for fields in the list datemap indicates the field must
        be date
        '''

        count = self.printCursor(self._cursor, fieldNames, datemap, time_format)

#         print( "Wrote %i records" % count )

