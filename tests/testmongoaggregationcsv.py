#!/usr/bin/env python
# -*- coding utf-8 -*-

import unittest
import mongoaggregationcsv as toCSV


class TestMongoAggregationCSV(unittest.TestCase):

    def test_invalid_schema_1(self):
        testdata = 1
        self.assertFalse(toCSV.checkSchema(testdata))

    def test_invalid_schema_2(self):
        testdata = {"result": [{'a': 1}, {'b': 2}, {'c': 3}]}
        self.assertFalse(toCSV.checkSchema(testdata))

    def test_invalid_schema_3(self):
        testdata = {"result": [{'a': 1}, {'b': 2}, {'c': 3}], "ok": 0}
        self.assertTrue(toCSV.checkSchema(testdata))

    def test_ok_flag_1(self):
        testdata = {"result": [{'a': 1}, {'b': 2}, {'c': 3}], "ok": 0}
        self.assertFalse(toCSV.checkOk(testdata))

    def test_ok_flag_2(self):
        testdata = {"result": [{'a': 1}, {'b': 2}, {'c': 3}], "ok": 1}
        self.assertTrue(toCSV.checkOk(testdata))

    def test_empty_1(self):
        testdata = {"result": [], "ok": 1}
        self.assertFalse(toCSV.checkEmpty(testdata))

    def test_empty_2(self):
        testdata = {"result": [{'a': 1}, {'b': 2}, {'c': 3}], "ok": 1}
        self.assertTrue(toCSV.checkEmpty(testdata))

    def test_depth_1(self):
        testdata = {"result": [{'a': 1}, {'b': 2}, {'c': 3}], "ok": 1}
        self.assertTrue(toCSV.checkDepth(testdata))

    def test_depth_2(self):
        testdata = {"result": [{'a': 1}, {'b': 2}, {'c': [3, 4, 5]}], "ok": 1}
        self.assertTrue(toCSV.checkDepth(testdata))

    def test_depth_3(self):
        testdata = {"result": [{'a': 1}, {'b': [3, 4, [5, 6, 7]]}], "ok": 1}
        self.assertFalse(toCSV.checkDepth(testdata))

    def test_depth_4(self):
        testdata = {"result": [{'a': 1}, {'b': 2}, {'c': {'d': 3}}], "ok": 1}
        self.assertFalse(toCSV.checkDepth(testdata))

if __name__ == "__main__":
    unittest.main()
