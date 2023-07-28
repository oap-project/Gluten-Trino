/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.trino.spi.protocol;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static java.lang.Character.isLetterOrDigit;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class MockSubfieldTokenizer
        implements Iterator<MockSubfield.PathElement>
{
    private static final char QUOTE = '\"';
    private static final char BACKSLASH = '\\';
    private static final char DOT = '.';
    private static final char OPEN_BRACKET = '[';
    private static final char CLOSE_BRACKET = ']';
    private static final char UNICODE_CARET = '\u2038';
    private static final char WILDCARD = '*';

    private final String path;
    private State state = State.NOT_READY;
    private int index;
    private boolean firstSegment = true;
    private MockSubfield.PathElement next;

    public MockSubfieldTokenizer(String path)
    {
        this.path = requireNonNull(path, "path is null");

        if (path.isEmpty()) {
            throw invalidSubfieldPath();
        }
    }

    @Override
    public final boolean hasNext()
    {
        if (state == State.FAILED) {
            throw new IllegalStateException();
        }
        switch (state) {
            case DONE:
                return false;
            case READY:
                return true;
            default:
        }
        return tryToComputeNext();
    }

    private boolean tryToComputeNext()
    {
        state = State.FAILED; // temporary pessimism
        next = computeNext();
        if (state != State.DONE) {
            state = State.READY;
            return true;
        }
        return false;
    }

    @Override
    public final MockSubfield.PathElement next()
    {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        state = State.NOT_READY;
        MockSubfield.PathElement result = next;
        next = null;
        return result;
    }

    @Override
    public final void remove()
    {
        throw new UnsupportedOperationException();
    }

    private MockSubfield.PathElement computeNext()
    {
        if (!hasNextCharacter()) {
            state = State.DONE;
            return null;
        }

        if (tryMatch(DOT)) {
            MockSubfield.PathElement token = matchPathSegment();
            firstSegment = false;
            return token;
        }

        if (tryMatch(OPEN_BRACKET)) {
            MockSubfield.PathElement token = tryMatch(QUOTE) ? matchQuotedSubscript() : tryMatch(WILDCARD) ? matchWildcardSubscript() : matchUnquotedSubscript();

            match(CLOSE_BRACKET);
            firstSegment = false;
            return token;
        }

        if (firstSegment) {
            MockSubfield.PathElement token = matchPathSegment();
            firstSegment = false;
            return token;
        }

        throw invalidSubfieldPath();
    }

    private MockSubfield.PathElement matchPathSegment()
    {
        // seek until we see a special character or whitespace
        int start = index;
        while (hasNextCharacter() && isUnquotedPathCharacter(peekCharacter())) {
            nextCharacter();
        }
        int end = index;

        String token = path.substring(start, end);

        // an empty unquoted token is not allowed
        if (token.isEmpty()) {
            throw invalidSubfieldPath();
        }

        return new MockSubfield.NestedField(token);
    }

    private MockSubfield.PathElement matchWildcardSubscript()
    {
        return MockSubfield.allSubscripts();
    }

    private static boolean isUnquotedPathCharacter(char c)
    {
        return c == ':' || c == '$' || c == '-' || c == '/' || c == '@' || c == '|' || c == '#' || c == ' ' || isUnquotedSubscriptCharacter(c);
    }

    private MockSubfield.PathElement matchUnquotedSubscript()
    {
        // seek until we see a special character or whitespace
        int start = index;
        while (hasNextCharacter() && isUnquotedSubscriptCharacter(peekCharacter())) {
            nextCharacter();
        }
        int end = index;

        String token = path.substring(start, end);

        // an empty unquoted token is not allowed
        if (token.isEmpty()) {
            throw invalidSubfieldPath();
        }

        long index;
        try {
            index = Long.valueOf(token);
        }
        catch (NumberFormatException e) {
            throw invalidSubfieldPath();
        }

        return new MockSubfield.LongSubscript(index);
    }

    private static boolean isUnquotedSubscriptCharacter(char c)
    {
        return c == '-' || c == '_' || isLetterOrDigit(c);
    }

    private MockSubfield.PathElement matchQuotedSubscript()
    {
        // quote has already been matched

        // seek until we see the close quote
        StringBuilder token = new StringBuilder();
        boolean escaped = false;

        while (hasNextCharacter() && (escaped || peekCharacter() != QUOTE)) {
            if (escaped) {
                switch (peekCharacter()) {
                    case QUOTE:
                    case BACKSLASH:
                        token.append(peekCharacter());
                        break;
                    default:
                        throw invalidSubfieldPath();
                }
                escaped = false;
            }
            else {
                if (peekCharacter() == BACKSLASH) {
                    escaped = true;
                }
                else {
                    token.append(peekCharacter());
                }
            }
            nextCharacter();
        }
        if (escaped) {
            throw invalidSubfieldPath();
        }

        match(QUOTE);

        String index = token.toString();
        if (index.equals(String.valueOf(WILDCARD))) {
            return MockSubfield.allSubscripts();
        }
        return new MockSubfield.StringSubscript(index);
    }

    private boolean hasNextCharacter()
    {
        return index < path.length();
    }

    private void match(char expected)
    {
        if (!tryMatch(expected)) {
            throw invalidSubfieldPath();
        }
    }

    private boolean tryMatch(char expected)
    {
        if (!hasNextCharacter() || peekCharacter() != expected) {
            return false;
        }
        index++;
        return true;
    }

    private void nextCharacter()
    {
        index++;
    }

    private char peekCharacter()
    {
        return path.charAt(index);
    }

    private RuntimeException invalidSubfieldPath()
    {
        return new RuntimeException(format("Invalid subfield path: '%s'", this));
    }

    @Override
    public String toString()
    {
        return path.substring(0, index) + UNICODE_CARET + path.substring(index);
    }

    private enum State {
        /** We have computed the next element and haven't returned it yet. */
        READY,

        /** We haven't yet computed or have already returned the element. */
        NOT_READY,

        /** We have reached the end of the data and are finished. */
        DONE,

        /** We've suffered an exception and are kaput. */
        FAILED,
    }
}
