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
 * {@link ShortAttributeConverter}のテスト。
 */
@RunWith(Enclosed.class)
public class ShortAttributeConverterTest {

    public static class ConvertToDatabaseTest {

        @Rule
        public ExpectedException expectedException = ExpectedException.none();

        private final ShortAttributeConverter sut = new ShortAttributeConverter();

        private static final Short INPUT = (short) 100;

        @Test
        public void convertToShort() throws Exception {
            assertThat(sut.convertToDatabase(INPUT, Short.class), is(INPUT));
        }

        @Test
        public void convertToInteger() throws Exception {
            assertThat(sut.convertToDatabase(INPUT, Integer.class), is(100));
        }

        @Test
        public void convertToLong() throws Exception {
            assertThat(sut.convertToDatabase(INPUT, Long.class), is(100L));
        }

        @Test
        public void convertToBigDecimal() throws Exception {
            assertThat(sut.convertToDatabase(INPUT, BigDecimal.class), is(BigDecimal.valueOf(100)));
        }

        @Test
        public void convertToString() throws Exception {
            assertThat(sut.convertToDatabase(INPUT, String.class), is("100"));
        }

        @Test
        public void convertToSqlDate() throws Exception {
            expectedException.expect(IllegalArgumentException.class);
            expectedException.expectMessage("unsupported database type:java.sql.Date");

            sut.convertToDatabase(INPUT, java.sql.Date.class);
        }
    }

    public static class ConvertFromDatabaseTest {

        @Rule
        public ExpectedException expectedException = ExpectedException.none();

        private final ShortAttributeConverter sut = new ShortAttributeConverter();

        @Test
        public void convertFromNull() throws Exception {
            assertThat(sut.convertFromDatabase(null), is(nullValue()));
        }

        @Test
        public void convertFromShort() throws Exception {
            assertThat(sut.convertFromDatabase((short) 101), is((short) 101));
        }

        @Test
        public void convertFromBigDecimal() throws Exception {
            assertThat(sut.convertFromDatabase(BigDecimal.ONE), is((short) 1));
        }

        @Test
        public void convertFromString() throws Exception {
            assertThat(sut.convertFromDatabase("101"), is((short) 101));
        }

        @Test
        public void convertFormInteger() throws Exception {
            assertThat(sut.convertFromDatabase(100), is((short) 100));
        }

        @Test
        public void convertFromTimestamp_shouldThrowException() throws Exception {
            expectedException.expect(IllegalArgumentException.class);
            expectedException.expectMessage(
                    "unsupported data type:java.sql.Timestamp, value:2016-12-01 00:11:22.123321");

            sut.convertFromDatabase(Timestamp.valueOf("2016-12-01 00:11:22.123321"));
        }
    }

    public static class PrimitiveTest {

        private final ShortAttributeConverter.Primitive sut = new ShortAttributeConverter.Primitive();

        @Test
        public void convertToDatabase() throws Exception {
            assertThat(sut.convertToDatabase((short) 100, Short.class), is((short) 100));
            assertThat(sut.convertToDatabase((short) 100, String.class), is("100"));
        }

        @Test
        public void convertFromDatabase() throws Exception {
            assertThat(sut.convertFromDatabase(null), is((short) 0));
            assertThat(sut.convertFromDatabase(100), is((short) 100));
            assertThat(sut.convertFromDatabase("101"), is((short) 101));
        }

    }
}