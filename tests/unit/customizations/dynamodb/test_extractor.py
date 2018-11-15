# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
from awscli.customizations.dynamodb.extractor import AttributeExtractor
from awscli.testutils import unittest


class FakeLexer(object):
    def __init__(self, tokens):
        self.tokens = tokens

    def tokenize(self, expression):
        return self.tokens


class TestExtractor(unittest.TestCase):
    def test_extract_identifier(self):
        tokens = [
            {'type': 'unquoted_identifier', 'value': 'spam', 'start': 0,
             'end': 3},
            {'type': 'eof', 'value': '', 'start': 3, 'end': 3},
        ]
        lexer = FakeLexer(tokens)
        extractor = AttributeExtractor(lexer)
        result = extractor.extract('spam')
        expected = {
            'expression': '#n0',
            'identifiers': {'#n0': 'spam'},
            'values': {},
            'substitution_count': 1,
        }
        self.assertEqual(result, expected)

    def test_extract_quoted_identifier(self):
        tokens = [
            {'type': 'identifier', 'value': 'spam', 'start': 0,
             'end': 5},
            {'type': 'eof', 'value': '', 'start': 5, 'end': 5},
        ]
        lexer = FakeLexer(tokens)
        extractor = AttributeExtractor(lexer)
        result = extractor.extract("'spam'")
        expected = {
            'expression': '#n0',
            'identifiers': {'#n0': 'spam'},
            'values': {},
            'substitution_count': 1,
        }
        self.assertEqual(result, expected)

    def test_extract_string(self):
        tokens = [
            {'type': 'literal', 'value': 'spam', 'start': 0, 'end': 5},
            {'type': 'eof', 'value': '', 'start': 5, 'end': 5},
        ]
        lexer = FakeLexer(tokens)
        extractor = AttributeExtractor(lexer)
        result = extractor.extract('"spam"')
        expected = {
            'expression': ':n0',
            'identifiers': {},
            'values': {':n0': 'spam'},
            'substitution_count': 1,
        }
        self.assertEqual(result, expected)

    def test_extract_bytes(self):
        tokens = [
            {'type': 'literal', 'value': 'spam', 'start': 0, 'end': 5},
            {'type': 'eof', 'value': '', 'start': 5, 'end': 5},
        ]
        lexer = FakeLexer(tokens)
        extractor = AttributeExtractor(lexer)
        result = extractor.extract('"spam"')
        expected = {
            'expression': ':n0',
            'identifiers': {},
            'values': {':n0': 'spam'},
            'substitution_count': 1,
        }
        self.assertEqual(result, expected)

    def test_extract_number(self):
        tokens = [
            {'type': 'literal', 'value': 7, 'start': 0, 'end': 1},
            {'type': 'eof', 'value': '', 'start': 1, 'end': 1},
        ]
        lexer = FakeLexer(tokens)
        extractor = AttributeExtractor(lexer)
        result = extractor.extract("'spam'")
        expected = {
            'expression': ':n0',
            'identifiers': {},
            'values': {':n0': 7},
            'substitution_count': 1,
        }
        self.assertEqual(result, expected)

    def test_set_index_start(self):
        tokens = [
            {'type': 'unquoted_identifier', 'value': 'spam', 'start': 0,
             'end': 3},
            {'type': 'eof', 'value': '', 'start': 3, 'end': 3},
        ]
        lexer = FakeLexer(tokens)
        extractor = AttributeExtractor(lexer)
        result = extractor.extract('spam', index_start=5)
        expected = {
            'expression': '#n5',
            'identifiers': {'#n5': 'spam'},
            'values': {},
            'substitution_count': 1,
        }
        self.assertEqual(result, expected)

    def test_extract_multiple_attributes(self):
        tokens = [
            {'type': 'unquoted_identifier', 'value': 'spam', 'start': 0,
             'end': 3},
            {'type': 'whitespace', 'value': ' ', 'start': 3, 'end': 4},
            {'type': 'eq', 'value': '=', 'start': 4, 'end': 5},
            {'type': 'whitespace', 'value': ' ', 'start': 5, 'end': 6},
            {'type': 'literal', 'value': 7, 'start': 6, 'end': 7},
            {'type': 'eof', 'value': '', 'start': 7, 'end': 7},
        ]
        lexer = FakeLexer(tokens)
        extractor = AttributeExtractor(lexer)
        result = extractor.extract('spam = 7')
        expected = {
            'expression': '#n0 = :n1',
            'identifiers': {'#n0': 'spam'},
            'values': {':n1': 7},
            'substitution_count': 2,
        }
        self.assertEqual(result, expected)
