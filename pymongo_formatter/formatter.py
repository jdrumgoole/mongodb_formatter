#!/bin/env python

import pymongo
import sys
import contextlib
from datetime import datetime
from pprint import pprint
import csv

from mongodb_formatter.nested_dict import Nested_Dict

class CursorFormatter(object):
    '''
    If root is a file name output the content to that file.
    '''

    def __init__(self, filename="", formatter="json"):


        self._filename = filename
        self._formatter = formatter



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

    @staticmethod
    def dateMapField(doc, field, time_format=None):
        '''
        Given a field that contains a datetime we want it to be output as a string otherwise
        pprint and other functions will abondon ship when they meet BSON time objects
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
    def fieldMapper(doc, fields):
        '''
        Take 'doc' and create a new doc using only keys from the 'fields' list.
        Supports referencing fields using dotted notation "a.b.c" so we can parse
        nested fields the way MongoDB does. The nested field class is a hack. It should
        be a sub-class of dict.
        '''

        if fields is None or len(fields) == 0:
            return doc

        newDoc = Nested_Dict({})
        oldDoc = Nested_Dict(doc)

        for i in fields:
            if oldDoc.has_key(i):
                # print( "doc: %s" % doc )
                # print( "i: %s" %i )
                newDoc.set_value(i, oldDoc.get_value(i))
        return newDoc.dict_value()

    @staticmethod
    def dateMapper(doc, datemap, time_format=None):
        '''
        For all the fields in "datemap" find that key in doc and map the datetime object to
        a strftime string. This pprint and others will print out readable datetimes.
        '''
        if datemap:
            for i in datemap:
                if isinstance(i, datetime):
                    CursorFormatter.dateMapField(doc, i, time_format=time_format)
        return doc

    def printCSVCursor(self, c, fieldnames, datemap, time_format=None):
        '''
        Output CSV format. items are separated by commas. We only output the fields listed
        in the 'fieldnames'. We datemap fields listed in 'datemap'. If a datemap listed field
        is not a datetime object we will thow an exception.
        '''

        with self._smart_open(self._filename) as output:
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            count = 0
            for i in c:
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

    def printJSONCursor(self, c, fieldnames, datemap, time_format=None):
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
            for i in c:
                # print( "processing: %s" % i )
                # print( "fieldnames: %s" % fieldnames )
                self._results.append(i)
                d = CursorFormatter.fieldMapper(i, fieldnames)
                # print( "processing fieldmapper: %s" % d )
                d = CursorFormatter.dateMapper(d, datemap, time_format)
                pprint.pprint(d, output)
                count = count + 1

        return count

    def printCursor(self, c, fieldnames=None, datemap=None, time_format=None):
        '''
        Output a cursor to a filename or stdout if filename is "-".
        fmt defines whether we output CSV or JSON.
        '''

        if self._format == 'csv':
            count = self.printCSVCursor(c, fieldnames, datemap, time_format)
        else:
            count = self.printJSONCursor(c, fieldnames, datemap, time_format)

        return count

    def output(self, fieldNames=None, datemap=None, time_format=None):
        '''
        Output all fields using the fieldNames list. for fields in the list datemap indicates the field must
        be date
        '''

        count = self.printCursor(self._cursor, fieldNames, datemap, time_format)

#         print( "Wrote %i records" % count )

