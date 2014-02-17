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
    Validate document is from a Mongo Aggregation
    """
    if isinstance(document, dict):
        return bool("result" in document and "ok" in document)


def checkOk(document):
    """
    Check aggregation ok flag
    """
    return document["ok"] == 1


def checkEmpty(document):
    """
    Check results array contains at least one document
    """
    return len(document["result"]) > 0


def checkDepth(document):
    """
    Check each sub-document in the results array has no nested documents
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


def csvHeader(document, sep=","):
    """
    Return CSV column names
    """
    return ','.join(map(str, data["result"][0].keys())) + '\n'


def csvRow(document, sep=","):
    """
    Return CSV rows
    """
    return ','.join(map(str, document.values())) + '\n'


def toCSV(document, sep=","):
    """
    Convert mongodb aggregation document to CSV file
    """
    csv = StringIO()

    csv.write(csvHeader(document, sep=sep))
    for subdoc in data["result"]:
        csv.write(csvRow(subdoc, sep=sep))

    return csv

if __name__ == "__main__":

    import re
    import sys
    import json

    mongo_json = StringIO()

    for line in sys.stdin:
        line = re.sub(r"NumberLong[(](\d+)[)]", r"\1", line)
        line = re.sub(r"ObjectId[(]([^)]+)[)]", r"\1", line)
        mongo_json.write(line)

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
