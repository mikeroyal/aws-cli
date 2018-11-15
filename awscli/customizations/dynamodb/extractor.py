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
from .lexer import Lexer


class AttributeExtractor(object):
    def __init__(self, lexer=None):
        self._lexer = lexer
        if lexer is None:
            self._lexer = Lexer()

    def extract(self, expression, index_start=0):
        tokens = list(self._lexer.tokenize(expression))
        identifiers = {}
        values = {}
        new_expression = ''
        index = index_start
        for token in tokens:
            if token['type'] in ['unquoted_identifier', 'identifier']:
                substitution_token = '#n%s' % index
                identifiers[substitution_token] = token['value']
                new_expression += substitution_token
                index += 1
            elif token['type'] == 'literal':
                substitution_token = ':n%s' % index
                values[substitution_token] = token['value']
                new_expression += substitution_token
                index += 1
            else:
                new_expression += token['value']

        return {
            'expression': new_expression,
            'identifiers': identifiers,
            'values': values,
            'substitution_count': index - index_start,
        }
