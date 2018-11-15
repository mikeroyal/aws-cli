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
from nose.tools import assert_equal, assert_raises

from awscli.customizations.dynamodb.lexer import Lexer, LexerError


def test_lexer():
    cases = [
        ('foo', [{'type': 'unquoted_identifier', 'value': 'foo'}]),
        ("'foo'", [{'type': 'identifier', 'value': 'foo'}]),
        ("'f\\'oo'", [{'type': 'identifier', 'value': "f'oo"}]),
        ('"spam"', [{'type': 'literal', 'value': 'spam'}]),
        ('"s\\"pam"', [{'type': 'literal', 'value': 's"pam'}]),
        ('100', [{'type': 'literal', 'value': 100}]),
        ('-100', [{'type': 'literal', 'value': -100}]),
        ('1.01', [{'type': 'literal', 'value': 1.01}]),
        ('1.01e6', [{'type': 'literal', 'value': 1010000}]),
        ('1.01E6', [{'type': 'literal', 'value': 1010000}]),
        ('1.01e+6', [{'type': 'literal', 'value': 1010000}]),
        ('1.01e-6', [{'type': 'literal', 'value': 0.00000101}]),
        ('\t', [{'type': 'whitespace', 'value': '\t'}]),
        ('\n', [{'type': 'whitespace', 'value': '\n'}]),
        ('\r', [{'type': 'whitespace', 'value': '\r'}]),
        (' ', [{'type': 'whitespace', 'value': ' '}]),
        (' \t \n', [{'type': 'whitespace', 'value': ' \t \n'}]),
        ("foo, 'bar', \"baz\"", [
            {'type': 'unquoted_identifier', 'value': 'foo'},
            {'type': 'comma', 'value': ','},
            {'type': 'whitespace', 'value': ' '},
            {'type': 'identifier', 'value': 'bar'},
            {'type': 'comma', 'value': ','},
            {'type': 'whitespace', 'value': ' '},
            {'type': 'literal', 'value': 'baz'},
        ]),
        ('b"4pyT"', [{'type': 'literal', 'value': b'\xe2\x9c\x93'}]),
        ('boo', [{'type': 'unquoted_identifier', 'value': 'boo'}]),
    ]

    tester = LexTester()
    for case in cases:
        yield tester.assert_tokens, case[0], case[1]

    simple_tokens = {
        '.': 'dot',
        ',': 'comma',
        ':': 'colon',
        '(': 'lparen',
        ')': 'rparen',
        '{': 'lbrace',
        '}': 'rbrace',
        '[': 'lbracket',
        ']': 'rbracket',
        '=': 'eq',
        '>': 'gt',
        '>=': 'gte',
        '<': 'lt',
        '<=': 'lte',
        '<>': 'ne',
    }
    for token, token_type  in simple_tokens.items():
        expected = [{'type': token_type, 'value': token}]
        yield tester.assert_tokens, token, expected

    string_tokens = ['and', 'between', 'in', 'or', 'not']
    for token in string_tokens:
        expected = [{'type': token, 'value': token}]
        yield tester.assert_tokens, token, expected

        expected = [{'type': token, 'value': token.upper()}]
        yield tester.assert_tokens, token.upper(), expected

        expected = [{'type': token, 'value': token.capitalize()}]
        yield tester.assert_tokens, token.capitalize(), expected


def test_lexer_error():
    cases = [
        '',
        "'",
        '"',
        '-',
        '1e-',
        '1ex',
        '1.',
        '1.x',
        '&',
        '|',
        'b"',
        'b"&"',
        # Invalid padding
        'b"898989;;"',
    ]

    tester = LexTester()
    for case in cases:
        yield tester.assert_lex_error, case


class LexTester(object):
    def __init__(self):
        self.lexer = Lexer()

    def assert_tokens(self, expression, expected):
        actual = self.lexer.tokenize(expression)
        simple_tokens = [
            {'type': t['type'], 'value': t['value']} for t in actual
        ]
        assert_equal(simple_tokens.pop()['type'], 'eof')
        assert_equal(simple_tokens, expected)

    def assert_lex_error(self, expression):
        with assert_raises(LexerError):
            list(self.lexer.tokenize(expression))
