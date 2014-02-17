#!/usr/bin/env python
# -*- coding utf-8 -*-

"""
Mongoexport allows queries to be written to CSV files but does
not support the aggregation framework. This script can be used
as a filter for converting non-nested results to csv.
"""

__usage__ = "mongo db/col aggregation.js | mongoaggregationcsv.py > output.csv"
__author__ = "Chris Seymour"
__twitter__ = "iiSeymour"
__license__ = "MIT"


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


if __name__ == "__main__":

    import sys
    import json

    try:
        data = json.load(sys.stdin)
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

    print ','.join(data["result"][0].keys())

    for subdoc in data["result"]:
        print ','.join(map(str, subdoc.values()))
