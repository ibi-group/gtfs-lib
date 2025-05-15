package com.conveyal.gtfs.validator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class MTCValidatorTest {
    @Test
    void canValidateFieldLength() {
        MTCValidator validator = new MTCValidator(null, null);
        assertThat(validator.validateFieldLength(null, "abcdefghijklmnopqrstwxyz1234567890", 20), is(false));
        assertThat(validator.validateFieldLength(null, "abcdef", 20), is(true));
    }

    @Test
    void canValidateFieldLength_usingObject() throws MalformedURLException {
        MTCValidator validator = new MTCValidator(null, null);

        // You can also pass objects, in that case it will use toString().
        URL url = new URL("http://www.gtfs.org");
        assertThat(validator.validateFieldLength(null, url, 10), is(false));
        assertThat(validator.validateFieldLength(null, url, 30), is(true));
    }

    @ParameterizedTest
    @MethodSource("createValidateStopCodePrefixCases")
    void validateStopCodePrefix(PrefixTestCase prefixTestCase) {
        MTCValidator validator = new MTCValidator(
            null,
            null,
            prefixTestCase.primaryPrefix,
            prefixTestCase.secondaryPrefixes
        );
        assertEquals(
            validator.validateStopCodePrefix(null, prefixTestCase.stopCode),
            prefixTestCase.valid
        );
    }

    private static Stream<PrefixTestCase> createValidateStopCodePrefixCases() {
        String primaryPrefix = "primary-1";
        String stopCodePrefix = "stop-code-1";
        String secondaryPrefixOne = "secondary-1";
        String secondaryPrefixTwo = "secondary-2";
        String secondaryPrefixThree = "secondary-3";
        return Stream.of(
            new PrefixTestCase()
                .withStopCode(null)
                .withValid(true)
                .withMessage("No stop code defined. Skip validation."),
            new PrefixTestCase()
                .withStopCode(primaryPrefix + stopCodePrefix)
                .withPrimaryPrefix(primaryPrefix)
                .withValid(true)
                .withMessage("Match on primary prefix."),
            new PrefixTestCase()
                .withStopCode(stopCodePrefix)
                .withPrimaryPrefix("primary")
                .withMessage("Missing primary prefix."),
            new PrefixTestCase()
                .withStopCode(stopCodePrefix)
                .withValid(true)
                .withMessage("No prefixes defined, will be valid."),
            new PrefixTestCase()
                .withStopCode(secondaryPrefixOne + stopCodePrefix)
                .withSecondaryPrefixes(secondaryPrefixOne)
                .withValid(true)
                .withMessage("Match on first and only secondary prefixes."),
            new PrefixTestCase()
                .withStopCode(secondaryPrefixTwo + stopCodePrefix)
                .withSecondaryPrefixes(secondaryPrefixOne, secondaryPrefixTwo)
                .withValid(true)
                .withMessage("Match on second prefix in secondary prefixes."),
            new PrefixTestCase()
                .withStopCode(stopCodePrefix)
                .withPrimaryPrefix(primaryPrefix)
                .withSecondaryPrefixes(secondaryPrefixOne, secondaryPrefixTwo)
                .withMessage("No match on any prefix."),
            new PrefixTestCase()
                .withStopCode(secondaryPrefixThree + stopCodePrefix)
                .withPrimaryPrefix(primaryPrefix)
                .withValid(true)
                .withSecondaryPrefixes(secondaryPrefixOne, secondaryPrefixTwo, secondaryPrefixThree)
                .withMessage("Match on third secondary prefix with primary prefix defined.")
        );
    }

    private static class PrefixTestCase {
        public String stopCode;
        public String primaryPrefix;
        public List<String> secondaryPrefixes;
        public boolean valid;
        public String message;

        public PrefixTestCase withStopCode(String stopCode) {
            this.stopCode = stopCode;
            return this;
        }

        public PrefixTestCase withPrimaryPrefix(String primaryPrefix) {
            this.primaryPrefix = primaryPrefix;
            return this;
        }

        public PrefixTestCase withSecondaryPrefixes(String... secondaryPrefixes) {
            this.secondaryPrefixes = Arrays.asList(secondaryPrefixes);
            return this;
        }

        public PrefixTestCase withValid(boolean valid) {
            this.valid = valid;
            return this;
        }

        public PrefixTestCase withMessage(String message) {
            this.message = message;
            return this;
        }

        @Override
        public String toString() {
            return message;
        }
    }
}