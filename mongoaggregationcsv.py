#!/usr/bin/env python
# -*- coding utf-8 -*-

"""
Mongoexport allows queries to be written to CSV files but does
not support the aggregation framework. This script can be used
as a filter for converting non-nested results to csv.
"""

__usage__ = "mongo --quiet db/col query.js | mongoaggregationcsv.py"
__author__ = "Chris Seymour"
__twitter__ = "iiSeymour"
__license__ = "MIT"


from StringIO import StringIO


def checkSchema(document):
    """
    Validate document is from a MongoDB aggregation query
    """
    if isinstance(document, dict):
        return bool("result" in document and "ok" in document)


def checkOk(document):
    """
    Validate the querys ok flag
    """
    return document["ok"] == 1


def checkEmpty(document):
    """
    Validate the result array contains at least one document
    """
    return len(document["result"]) > 0


def checkDepth(document):
    """
    Validate each sub-document in the results array has no nested documents
    Only scalar values and flat lists permitted
    """
    for subdoc in document["result"]:
        if any(isinstance(item, dict) for item in subdoc.values()):
            return False

        for subvalue in subdoc.values():
            if isinstance(subvalue, list):
                if any(isinstance(item, dict) for item in subvalue):
                    return False
                if any(isinstance(item, list) for item in subvalue):
                    return False

    return True


def uniqueKeys(document, sep=","):
    """
    Get all unique keys in the document for the CSV headers
    """
    columns = []

    for subdoc in document["result"]:
        for key in subdoc.keys():
            if key not in columns:
                if isinstance(key, str) and sep in key:
                    key = '"%s"' % key
                columns.append(key)
    return sorted(columns)


def csvHeader(document, columns, sep=","):
    """
    Returns the CSV header
    """
    return sep.join(map(str, columns)) + '\n'


def csvRow(document, columns, sep=","):
    """
    Returns a CSV rows
    """
    row = []
    for key in columns:
        if sep in key:
            val = document.get(key[1:-1], '')
        else:
            val = document.get(key, '')
        if isinstance(val, str) and sep in val:
            val = '"%s"' % val
        if isinstance(val, list):
            val = '"%s"' % sep.join(map(str, val))

        row.append(val)
    return sep.join(map(str, row)) + '\n'


def toCSV(document, sep=","):
    """
    Convert mongodb aggregation document to CSV file
    """
    csv = StringIO()
    columns = uniqueKeys(document)

    csv.write(csvHeader(document, columns, sep=sep))
    for subdoc in document["result"]:
        csv.write(csvRow(subdoc, columns, sep=sep))

    return csv

if __name__ == "__main__":

    import re
    import sys
    import json

    mongo_json = StringIO()

    for line in sys.stdin:
        if line.startswith("MongoDB shell") or line.startswith("connecting"):
            continue
        line = re.sub(r"NumberLong[(](\d+)[)]", r"\1", line)
        line = re.sub(r"ObjectId[(]([^)]+)[)]", r"\1", line)
        mongo_json.write(line)

    if mongo_json.getvalue().startswith("Error:"):
        print mongo_json.getvalue()
        sys.exit(1)

    try:
        data = json.loads(mongo_json.getvalue())
    except ValueError:
        sys.stderr.write("Input could not be encoded as JSON\n")
        sys.exit(1)

    if not checkSchema(data):
        sys.stderr.write("JSON doesn't match aggregation schema\n")
        sys.exit(1)

    if not checkOk(data):
        sys.stderr.write("Aggregation ok flag was %s\n" % data["ok"])
        sys.exit(1)

    if not checkEmpty(data):
        sys.stderr.write("Aggregation return no results\n")
        sys.exit(1)

    if not checkDepth(data):
        sys.stderr.write("Aggregation results are nested\n")
        sys.exit(1)

    print toCSV(data).getvalue()
