package nablarch.core.db.dialect.converter;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

/**
 * {@link BooleanAttributeConverter}のテスト。
 */
@RunWith(Enclosed.class)
public class BooleanAttributeConverterTest {

    public static class ConvertToDatabase {

        @Rule
        public ExpectedException expectedException = ExpectedException.none();

        private final BooleanAttributeConverter sut = new BooleanAttributeConverter();

        @Test
        public void convertToBoolean() throws Exception {
            assertThat(sut.convertToDatabase(true, Boolean.class), is(Boolean.TRUE));
        }

        @Test
        public void convertToBigDecimal() throws Exception {
            assertThat(sut.convertToDatabase(true, BigDecimal.class), is(BigDecimal.ONE));
            assertThat(sut.convertToDatabase(false, BigDecimal.class), is(BigDecimal.ZERO));
        }

        @Test
        public void convertToInteger() throws Exception {
            assertThat(sut.convertToDatabase(true, Integer.class), is(1));
            assertThat(sut.convertToDatabase(false, Integer.class), is(0));
        }

        @Test
        public void convertToLong() throws Exception {
            assertThat(sut.convertToDatabase(true, Long.class), is(1L));
            assertThat(sut.convertToDatabase(false, Long.class), is(0L));
        }

        @Test
        public void convertToShort() throws Exception {
            assertThat(sut.convertToDatabase(true, Short.class), is(Short.valueOf("1")));
            assertThat(sut.convertToDatabase(false, Short.class), is(Short.valueOf("0")));
        }

        @Test
        public void convertToTimestamp_shouldThrowException() throws Exception {
            expectedException.expect(IllegalArgumentException.class);
            expectedException.expectMessage("unsupported database type:java.sql.Timestamp");

            sut.convertToDatabase(true, Timestamp.class);
        }
    }

    public static class ConvertFromDatabase {

        @Rule
        public ExpectedException expectedException = ExpectedException.none();

        private final BooleanAttributeConverter sut = new BooleanAttributeConverter();

        @Test
        public void convertFromNull() throws Exception {
            assertThat(sut.convertFromDatabase(null), is(nullValue()));
        }

        @Test
        public void convertFromBoolean() throws Exception {
            assertThat(sut.convertFromDatabase(false), is(Boolean.FALSE));
            assertThat(sut.convertFromDatabase(true), is(Boolean.TRUE));
        }

        @Test
        public void convertFromString() throws Exception {
            assertThat(sut.convertFromDatabase("1"), is(Boolean.TRUE));
            assertThat(sut.convertFromDatabase("on"), is(Boolean.TRUE));
            assertThat(sut.convertFromDatabase("On"), is(Boolean.TRUE));
            assertThat(sut.convertFromDatabase("true"), is(Boolean.TRUE));
            assertThat(sut.convertFromDatabase("True"), is(Boolean.TRUE));

            assertThat(sut.convertFromDatabase("0"), is(Boolean.FALSE));
            assertThat(sut.convertFromDatabase("false"), is(Boolean.FALSE));
            assertThat(sut.convertFromDatabase("aabbcc"), is(Boolean.FALSE));

        }

        @Test
        public void convertFromNumber() throws Exception {
            assertThat(sut.convertFromDatabase(Long.MAX_VALUE), is(Boolean.TRUE));
            assertThat(sut.convertFromDatabase(new BigDecimal(("1.12345"))), is(Boolean.TRUE));
            assertThat(sut.convertFromDatabase(new BigInteger("99999999999999999999")), is(Boolean.TRUE));
            assertThat(sut.convertFromDatabase(0), is(Boolean.FALSE));
            assertThat(sut.convertFromDatabase(BigDecimal.ZERO), is(Boolean.FALSE));
        }

        @Test
        public void convertFromTimestamp() throws Exception {
            expectedException.expect(IllegalArgumentException.class);
            expectedException.expectMessage(
                    "unsupported data type:java.sql.Timestamp, value:2016-12-03 01:02:03.123123");
            sut.convertFromDatabase(Timestamp.valueOf("2016-12-03 01:02:03.123123"));
        }
    }

    public static class PrimitiveTet {

        private final BooleanAttributeConverter.Primitive sut = new BooleanAttributeConverter.Primitive();

        @Test
        public void convertToDatabase() throws Exception {
            assertThat(sut.convertToDatabase(true, Boolean.class), is(true));
            assertThat(sut.convertToDatabase(false, Boolean.class), is(false));
        }
        
        @Test
        public void covertFromDatabase() throws  Exception {
            assertThat(sut.convertFromDatabase(null), is(false));
            assertThat(sut.convertFromDatabase(sut.convertFromDatabase("on")), is(true));
        }
    }

}