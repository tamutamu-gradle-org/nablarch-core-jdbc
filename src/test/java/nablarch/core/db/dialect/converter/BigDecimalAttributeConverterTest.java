package nablarch.core.db.dialect.converter;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import java.math.BigDecimal;
import java.sql.Timestamp;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

/**
 * {@link BigDecimalAttributeConverter}のテスト。
 */
@RunWith(Enclosed.class)
public class BigDecimalAttributeConverterTest {

    public static class ConvertToDatabaseTest {

        @Rule
        public ExpectedException expectedException = ExpectedException.none();

        private final BigDecimalAttributeConverter sut = new BigDecimalAttributeConverter();

        @Test
        public void convertToBigDecimal() throws Exception {
            assertThat(sut.convertToDatabase(BigDecimal.TEN, BigDecimal.class), is(BigDecimal.TEN));
        }

        @Test
        public void convertToInteger() throws Exception {
            assertThat(sut.convertToDatabase(new BigDecimal("12123"), Integer.class), is(12123));
        }

        @Test
        public void convertToToLong() throws Exception {
            assertThat(sut.convertToDatabase(new BigDecimal("321123"), Long.class), is(321123L));
        }

        @Test
        public void convertToShort() throws Exception {
            assertThat(sut.convertToDatabase(new BigDecimal("100"), Short.class), is(Short.valueOf("100")));

        }

        @Test
        public void convertToString() throws Exception {
            assertThat(sut.convertToDatabase(new BigDecimal("1.1"), String.class), is("1.1"));
        }

        @Test
        public void convertToTimestamp_shouldThrowException() throws Exception {
            expectedException.expect(IllegalArgumentException.class);
            expectedException.expectMessage("unsupported database type:java.sql.Timestamp");
            sut.convertToDatabase(BigDecimal.ONE, Timestamp.class);
        }
    }

    public static class ConvertFromDatabaseTest {

        @Rule
        public ExpectedException expectedException = ExpectedException.none();

        private final BigDecimalAttributeConverter sut = new BigDecimalAttributeConverter();
        
        @Test
        public void convertFromNull() throws Exception {
            assertThat(sut.convertFromDatabase(null), is(nullValue()));
        }

        @Test
        public void converterFromBigDecimal() throws Exception {
            assertThat(sut.convertFromDatabase(new BigDecimal("1.2345")), is(new BigDecimal("1.2345")));
        }

        @Test
        public void convertFromInteger() throws Exception {
            assertThat(sut.convertFromDatabase(100), is(new BigDecimal("100")));
        }

        @Test
        public void convertFromLong() throws Exception {
            assertThat(sut.convertFromDatabase(Long.MAX_VALUE), is(BigDecimal.valueOf(Long.MAX_VALUE)));
        }

        @Test
        public void convertFromShort() throws Exception {
            assertThat(sut.convertFromDatabase(Short.valueOf("12345")), is(new BigDecimal("12345")));
        }

        @Test
        public void convertFromString() throws Exception {
            assertThat(sut.convertFromDatabase("12345.54321"), is(new BigDecimal("12345.54321")));
        }
        
        @Test
        public void convertFromDouble() throws Exception {
            assertThat(sut.convertFromDatabase(Double.valueOf("1.1")), is(new BigDecimal("1.1")));
        }
        
        @Test
        public void convertFromFloat() throws Exception {
            assertThat(sut.convertFromDatabase(Float.valueOf("1.2")), is(new BigDecimal("1.2")));
        }
        
        @Test
        public void convertFromTimestamp_shouldThrowException() throws Exception {
            expectedException.expect(IllegalArgumentException.class);
            expectedException.expectMessage("unsupported data type:java.sql.Timestamp, value:2016-01-02 11:22:33.123");

            sut.convertFromDatabase(Timestamp.valueOf("2016-01-02 11:22:33.123"));
        }
    }
}