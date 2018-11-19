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
import awscli.customizations.dynamodb.ast as ast
from .lexer import Lexer


class ParserError(ValueError):
    pass


class EmptyExpressionError(ParserError):
    def __init__(self):
        super(EmptyExpressionError, self).__init__(
            "Expressions must not be empty"
        )


class InvalidTokenError(ParserError):
    def __init__(self, value, token_type, expected_type):
        message = (
            "Unexpected token '{value}' of type '{token_type}'. "
            "Expected type: {expected_type}"
        )
        message = message.format(
            value=value, token_type=token_type,
            expected_type=expected_type,
        )
        super(InvalidTokenError, self).__init__(message)


class Parser(object):
    COMPARATORS = ['eq', 'ne', 'lt', 'lte', 'gt', 'gte']

    def __init__(self, lexer=None):
        self._lexer = lexer
        if lexer is None:
            self._lexer = Lexer()
        self._position = 0
        self._tokens = []
        self._current = None

    def parse(self, expression):
        self._position = 0
        self._tokens = list(self._lexer.tokenize(expression))
        self._current = self._tokens[0]

        if self._match('eof'):
            raise EmptyExpressionError()

        parsed = self._parse_expression()

        if not self._match('eof'):
            raise ParserError('parse')
        return parsed

    def _parse_expression(self):
        if self._match_next('comma'):
            return self._parse_sequence()
        return self._parse_and_or()

    def _parse_and_or(self):
        expression = self._parse_simple_expression()

        while self._match(['and', 'or']):
            conjunction_type = self._current['type']
            self._advance()
            right = self._parse_simple_expression()
            if conjunction_type == 'and':
                expression = ast.and_expression(expression, right)
            else:
                expression = ast.or_expression(expression, right)

        return expression

    def _parse_simple_expression(self):
        if self._match('lparen'):
            return self._parse_subexpression()
        if self._match('not'):
            return self._parse_not_expression()
        return self._parse_condition_expression()

    def _parse_subexpression(self):
        self._advance_if_match('lparen')
        expression = self._parse_simple_expression()
        self._advance_if_match('rparen')
        return ast.subexpression(expression)

    def _parse_not_expression(self):
        self._advance_if_match('not')
        expression = self._parse_simple_expression()
        return ast.not_expression(expression)

    def _parse_condition_expression(self):
        if self._match_next('lparen'):
            return self._parse_function()
        elif self._match_next('in'):
            return self._parse_in_expression()
        elif self._match_next('between'):
            return self._parse_between_expression()
        elif self._match_next(self.COMPARATORS):
            return self._parse_comparison_expression()
        else:
            raise ParserError('condition')

    def _parse_function(self):
        function_name = self._current.get('value')
        self._advance_if_match(['identifier', 'unquoted_identifier'])
        self._advance_if_match('lparen')
        arguments = self._parse_sequence()["children"]
        self._advance_if_match('rparen')
        return ast.function_expression(function_name, arguments)

    def _parse_in_expression(self):
        left = self._parse_operand()
        self._advance_if_match('in')
        self._advance_if_match('lparen')
        right = self._parse_sequence()
        self._advance_if_match('rparen')
        return ast.in_expression(left, right)

    def _parse_between_expression(self):
        left = self._parse_operand()
        self._advance_if_match('between')
        middle = self._parse_operand()
        self._advance_if_match('and')
        right = self._parse_operand()
        return ast.between_expression(left, middle, right)

    def _parse_comparison_expression(self):
        left = self._parse_operand()
        comparator = self._current['type']
        self._advance_if_match(self.COMPARATORS)
        right = self._parse_operand()
        return ast.comparison_expression(comparator, left, right)

    def _parse_sequence(self):
        # parses a sequence of literals or identifiers. There must be at
        # least one.
        elements = []
        while True:
            elements.append(self._parse_operand())
            if not self._match('comma'):
                break
            self._advance()
        return ast.sequence(elements)

    def _parse_operand(self):
        if self._match('literal'):
            value = self._current['value']
            self._advance()
            return ast.literal(value)
        elif self._match(['identifier', 'unquoted_identifier']):
            value = self._current['value']
            self._advance()
            return ast.identifier(value)
        else:
            raise ParserError('operand')

    def _advance(self):
        if self._position == len(self._tokens) - 1:
            self._current = None
        else:
            self._position += 1
            self._current = self._tokens[self._position]

    def _peek(self):
        if self._position == len(self._tokens) - 1:
            return None
        return self._tokens[self._position + 1]

    def _match(self, expected_type):
        return self._do_match(self._current, expected_type)

    def _match_next(self, expected_type):
        return self._do_match(self._peek(), expected_type)

    def _do_match(self, token, expected_type):
        if token is None:
            return False
        if isinstance(expected_type, list):
            return any(token['type'] == t for t in expected_type)
        return token['type'] == expected_type

    def _advance_if_match(self, token_type):
        if self._match(token_type):
            self._advance()
        else:
            raise InvalidTokenError(
                value=self._current['value'],
                token_type=self._current['type'],
                expected_type=token_type,
            )
