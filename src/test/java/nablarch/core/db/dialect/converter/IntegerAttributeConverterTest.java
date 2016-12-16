package nablarch.core.db.dialect.converter;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import java.math.BigDecimal;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

/**
 * {@link IntegerAttributeConverter}のテスト。
 */
@RunWith(Enclosed.class)
public class IntegerAttributeConverterTest {

    public static class ConvertToDatabaseTest {

        @Rule
        public ExpectedException expectedException = ExpectedException.none();

        private final IntegerAttributeConverter sut = new IntegerAttributeConverter();

        @Test
        public void convertToInteger() throws Exception {
            assertThat(sut.convertToDatabase(12345, Integer.class), is(12345));
        }

        @Test
        public void convertToBigDecimal() throws Exception {
            assertThat(sut.convertToDatabase(54321, BigDecimal.class), is(BigDecimal.valueOf(54321)));
        }

        @Test
        public void convertToLong() throws Exception {
            assertThat(sut.convertToDatabase(100, Long.class), is(100L));
        }
        
        @Test
        public void convertToShort() throws Exception {
            assertThat(sut.convertToDatabase(100, Short.class), is((short) 100));
        }

        @Test
        public void convertToString() throws Exception {
            assertThat(sut.convertToDatabase(100, String.class), is("100"));
        }

        @Test
        public void convertToByteArray_shouldThrowException() throws Exception {
            expectedException.expect(IllegalArgumentException.class);
            expectedException.expectMessage("unsupported database type:[B");

            sut.convertToDatabase(100, byte[].class);
        }
    }

    public static class ConvertFromDatabaseTest {

        private final IntegerAttributeConverter sut = new IntegerAttributeConverter();

        @Rule
        public ExpectedException expectedException = ExpectedException.none();

        @Test
        public void convertFromInteger() throws Exception {
            assertThat(sut.convertFromDatabase(54321), is(54321));
        }

        @Test
        public void convertFromBigDecimal() throws Exception {
            assertThat(sut.convertFromDatabase(new BigDecimal("12345")), is(12345));
        }

        @Test
        public void convertFromBigDecimalWithDecimal_shouldThrowException() throws Exception {
            expectedException.expect(ArithmeticException.class);
            sut.convertFromDatabase(new BigDecimal("12345.1"));
        }

        @Test
        public void convertFromLong() throws Exception {
            assertThat(sut.convertFromDatabase(100L), is(100));
        }

        @Test
        public void convertFromShort() throws Exception {
            assertThat(sut.convertFromDatabase(Short.valueOf("10")), is(10));
        }

        @Test
        public void convertFromString() throws Exception {
            assertThat(sut.convertFromDatabase("123321"), is(123321));
        }

        @Test
        public void convertFromNull() throws Exception {
            assertThat(sut.convertFromDatabase(null), is(nullValue()));
        }

        @Test
        public void convertFromByteArray_shouldThrowException() throws Exception {
            expectedException.expect(IllegalArgumentException.class);
            expectedException.expectMessage("unsupported data type:[B, value:");
            sut.convertFromDatabase(new byte[0]);
        }
        
        @Test
        public void convertFromOverflowValue_shouldThrowException() throws Exception {
            expectedException.expect(NumberFormatException.class);
            sut.convertFromDatabase(Long.MAX_VALUE);
        }
    }

    public static class PrimitiveTest {

        private final IntegerAttributeConverter.Primitive sut = new IntegerAttributeConverter.Primitive();

        @Test
        public void convertToDatabase() throws Exception {
            assertThat(sut.convertToDatabase(100, Integer.class), is(100));
            assertThat(sut.convertToDatabase(100, String.class), is("100"));
        }
        
        @Test
        public void convertFromDatabase() throws Exception {
            assertThat(sut.convertFromDatabase(null), is(0));
            assertThat(sut.convertFromDatabase(100), is(100));
            assertThat(sut.convertFromDatabase("101"), is(101));
        }
    }
}
