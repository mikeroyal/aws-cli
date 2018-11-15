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
from base64 import b64decode
import binascii
import json
import re
import string


VALID_BASE64 = re.compile(r'[A-Za-z0-9+/=]+')


class LexerError(ValueError):
    pass


class Lexer(object):
    START_IDENTIFIER = set(string.ascii_letters + '_')
    VALID_IDENTIFIER = set(string.ascii_letters + string.digits + '_')
    WHITESPACE = set(' \t\n\r')
    SIMPLE_TOKENS = {
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
    }
    DIGITS = set(string.digits)
    INT_CHARS = set(string.digits + '-')
    OPERATION_TOKENS = {
        'and', 'between', 'in', 'or', 'not',
    }

    def tokenize(self, expression):
        self._init_expression(expression)
        while self._current is not None:
            if self._current in self.SIMPLE_TOKENS:
                yield {
                    'type': self.SIMPLE_TOKENS[self._current],
                    'value': self._current,
                    'start': self._position,
                    'end': self._position + 1,
                }
                self._next()
            elif self._current in self.START_IDENTIFIER:
                yield self._consume_unquoted_identifier()
            elif self._current == "'":
                yield self._consume_quoted_identifier()
            elif self._current == '"':
                yield self._consume_string_literal()
            elif self._current in self.WHITESPACE:
                yield self._consume_whitespace()
            elif self._current in self.INT_CHARS:
                yield self._consume_number()
            elif self._current == '<':
                if self._next() == '>':
                    self._next()
                    yield {
                        'type': 'ne', 'value': '<>',
                        'start': self._position - 2, 'end': self._position,
                    }
                elif self._current == '=':
                    self._next()
                    yield {
                        'type': 'lte', 'value': '<=',
                        'start': self._position - 2, 'end': self._position,
                    }
                else:
                    yield {
                        'type': 'lt', 'value': '<',
                        'start': self._position - 1, 'end': self._position
                    }
            elif self._current == '>':
                if self._next() == '=':
                    self._next()
                    yield {
                        'type': 'gte', 'value': '>=',
                        'start': self._position - 2, 'end': self._position,
                    }
                else:
                    yield {
                        'type': 'gt', 'value': '>',
                        'start': self._position - 1, 'end': self._position,
                    }
            else:
                raise LexerError(
                    'Unrecognized character %s at position %s' % (
                        self._current, self._position
                    ))
        yield {
            'type': 'eof', 'value': '',
            'start': self._length, 'end': self._length
        }

    def _consume_whitespace(self):
        start = self._position
        buff = self._current
        while self._next() in self.WHITESPACE:
            buff += self._current
        return {
            'type': 'whitespace', 'value': buff,
            'start': start, 'end': start + len(buff)
        }

    def _consume_unquoted_identifier(self):
        start = self._position
        buff = self._current

        if self._current in {'b', "B"}:
            if self._next() == '"':
                return self._consume_base64_string()
            elif self._current in self.VALID_IDENTIFIER:
                buff += self._current

        while self._next() in self.VALID_IDENTIFIER:
            buff += self._current

        lower = buff.lower()
        if lower in self.OPERATION_TOKENS:
            return {
                'type': lower, 'value': buff,
                'start': start, 'end': start + len(buff)
            }
        return {
            'type': 'unquoted_identifier', 'value': buff,
            'start': start, 'end': start + len(buff)
        }

    def _consume_quoted_identifier(self):
        start = self._position
        lexeme = self._consume_until("'").replace("\\'", "'")
        token_len = self._position - start
        return {
            'type': 'identifier', 'value': lexeme,
            'start': start, 'end': token_len
        }

    def _consume_string_literal(self):
        start = self._position
        lexeme = self._consume_until('"').replace('\\"', '"')
        token_len = self._position - start
        return {
            'type': 'literal', 'value': lexeme,
            'start': start, 'end': token_len
        }

    def _consume_base64_string(self):
        raw_string = self._consume_string_literal()

        # Python will simply ignore invalid characters, so we have to
        # validate manually.
        if raw_string['value'] and not VALID_BASE64.match(raw_string['value']):
            raise LexerError(
                'Invalid base64 string: b"%s"' % raw_string['value']
            )

        try:
            decoded = b64decode(raw_string['value'])
        except (TypeError, binascii.Error):
            raise LexerError(
                'Invalid base64 string: b"%s"' % raw_string['value']
            )

        raw_string['value'] = decoded
        raw_string['start'] -= 1
        return raw_string

    def _consume_number(self):
        start = self._position
        buff = self._consume_int()

        if self._current == '.':
            buff += self._current
            if self._next() not in self.DIGITS:
                raise LexerError(
                    'Invalid fractional character %s' % self._current
                )
            buff += self._consume_int()

        if self._current and self._current.lower() == 'e':
            buff += self._current
            if self._next() not in self.INT_CHARS and self._current != '+':
                raise LexerError(
                    'Invalid exponential character %s' % self._current
                )
            buff += self._consume_int()

        return {
            'type': 'literal', 'value': json.loads(buff),
            'start': start, 'end': start + len(buff)
        }

    def _consume_int(self):
        buff = self._current
        is_positive = self._current != '-'
        while self._next() in self.DIGITS:
            buff += self._current
        if not is_positive and len(buff) < 2:
            raise LexerError('Unknown token %s' % buff)
        return buff

    def _init_expression(self, expression):
        if not expression:
            raise LexerError('Expression must not be empty.')
        self._position = 0
        self._expression = expression
        self._chars = list(expression)
        self._current = self._chars[0]
        self._length = len(expression)

    def _next(self):
        if self._position == self._length - 1:
            self._current = None
        else:
            self._position += 1
            self._current = self._chars[self._position]
        return self._current

    def _consume_until(self, delimiter):
        # Consume until the delimiter is reached,
        # allowing for the delimiter to be escaped with "\".
        buff = ''
        self._next()
        while self._current != delimiter:
            if self._current == '\\':
                buff += '\\'
                self._next()
            if self._current is None:
                # We're at the EOF.
                raise LexerError("Unclosed %s delimiter" % delimiter)
            buff += self._current
            self._next()
        # Skip the closing delimiter.
        self._next()
        return buff
