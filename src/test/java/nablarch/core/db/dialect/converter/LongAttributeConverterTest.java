package nablarch.core.db.dialect.converter;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

/**
 * {@link LongAttributeConverter}のテスト。
 */
@RunWith(Enclosed.class)
public class LongAttributeConverterTest {

    public static class ConvertToDatabaseTest {

        @Rule
        public ExpectedException expectedException = ExpectedException.none();

        private final LongAttributeConverter sut = new LongAttributeConverter();

        private static final Long INPUT = 12345L;

        @Test
        public void convertToLong() throws Exception {
            assertThat(sut.convertToDatabase(INPUT, Long.class), is(12345L));
        }

        @Test
        public void convertToBigDecimal() throws Exception {
            assertThat(sut.convertToDatabase(INPUT, BigDecimal.class), is(BigDecimal.valueOf(12345L)));
        }

        @Test
        public void convertToString() throws Exception {
            assertThat(sut.convertToDatabase(INPUT, String.class), is("12345"));
        }

        @Test
        public void convertToByteArray() throws Exception {
            expectedException.expect(IllegalArgumentException.class);
            expectedException.expectMessage("unsupported database type:[B");
            sut.convertToDatabase(INPUT, byte[].class);
        }
    }

    public static class ConvertFromDatabaseTest {
        
        @Rule
        public ExpectedException expectedException = ExpectedException.none();

        private final LongAttributeConverter sut = new LongAttributeConverter();
        
        @Test
        public void convertFromNull() throws Exception {
            assertThat(sut.convertFromDatabase(null), is(nullValue()));
        }

        @Test
        public void convertFromLong() throws Exception {
            assertThat(sut.convertFromDatabase(54321L), is(54321L));
        }

        @Test
        public void convertFromInteger() throws Exception {
            assertThat(sut.convertFromDatabase(100), is(100L));
        }

        @Test
        public void convertFromBigDecimal() throws Exception {
            assertThat(sut.convertFromDatabase(BigDecimal.valueOf(543212345)), is(543212345L));
        }

        @Test
        public void convertFromString() throws Exception {
            assertThat(sut.convertFromDatabase("12345"), is(12345L));
        }

        @Test
        public void convertFromSqlDate_shouldThrowException() throws Exception {
            expectedException.expect(IllegalArgumentException.class);
            expectedException.expectMessage("unsupported data type:java.sql.Date, value:2016-12-01");

            sut.convertFromDatabase(Date.valueOf("2016-12-01"));
        }
        
        @Test
        public void convertFromOverflowValue_shouldThrowException() throws Exception {
            expectedException.expect(NumberFormatException.class);
            sut.convertFromDatabase(new BigInteger("9999999999999999999"));
        }
    }
    
    public static class PrimitiveTest {

        private final LongAttributeConverter.Primitive sut = new LongAttributeConverter.Primitive();

        @Test
        public void convertToDatabase() throws Exception {
            assertThat(sut.convertToDatabase(100L, Long.class), is(100L));
            assertThat(sut.convertToDatabase(100L, String.class), is("100"));
        }

        @Test
        public void convertFromDatabase() throws Exception {
            assertThat(sut.convertFromDatabase(null), is(0L));
            assertThat(sut.convertFromDatabase(100), is(100L));
            assertThat(sut.convertFromDatabase("101"), is(101L));
        }
    }
}