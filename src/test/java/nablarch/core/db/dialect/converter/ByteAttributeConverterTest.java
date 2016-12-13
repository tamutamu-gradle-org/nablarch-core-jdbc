package nablarch.core.db.dialect.converter;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.sql.Timestamp;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * {@link ByteAttributeConverter}のテスト
 * @author ryo asato
 */
@RunWith(Enclosed.class)
public class ByteAttributeConverterTest {

    public static class ConvertToDatabase {

        @Rule
        public ExpectedException expectedException = ExpectedException.none();

        private final ByteAttributeConverter sut = new ByteAttributeConverter();

        @Test
        public void convertToByte() throws Exception {
            byte target= 01;
            assertThat(sut.convertToDatabase(target, Byte.class), is(target));
        }

        @Test
        public void convertToTimestamp_shouldThrowException() throws Exception {
            expectedException.expect(IllegalArgumentException.class);
            expectedException.expectMessage("unsupported database type:java.sql.Timestamp");

            byte target= 01;
            sut.convertToDatabase(target, Timestamp.class);
        }
    }

    public static class ConvertFromDatabase {

        @Rule
        public ExpectedException expectedException = ExpectedException.none();

        private final ByteAttributeConverter sut = new ByteAttributeConverter();

        @Test
        public void convertFromNull() throws Exception {
            assertThat(sut.convertFromDatabase(null), is(nullValue()));
        }

        @Test
        public void convertFromByte() throws Exception {
            byte target = 01;
            assertThat(sut.convertFromDatabase(target), is(target));
        }

        @Test
        public void convertFromTimestamp_shouldThrowException() throws Exception {
            Timestamp target = Timestamp.valueOf("2016-12-03 01:02:03.123123");
            expectedException.expect(IllegalArgumentException.class);
            expectedException.expectMessage("unsupported data type:java.sql.Timestamp, value:" + target.toString());

            sut.convertFromDatabase(target);
        }
    }
    public static class PrimitiveTest {

        private final ByteAttributeConverter.Primitive sut = new ByteAttributeConverter.Primitive();

        @Test
        public void convertToDatabase() throws Exception {
            byte target = 01;
            assertThat(sut.convertToDatabase(target, Byte.class), is(target));
        }

        @Test
        public void convertFromDatabase() throws Exception {
            assertThat(sut.convertFromDatabase(null), is((byte) 0));
            byte target = 01;
            assertThat(sut.convertFromDatabase(target), is(target));
        }
    }
}
