package nablarch.core.db.dialect.converter;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import java.sql.Blob;
import java.sql.SQLException;

import org.hamcrest.CoreMatchers;

import nablarch.core.db.DbAccessException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import mockit.Expectations;
import mockit.Mocked;

/**
 * {@link ByteArrayAttributeConverter}のテスト。
 */
@RunWith(Enclosed.class)
public class ByteArrayAttributeConverterTest {

    public static class ConvertToDatabaseTest {

        @Rule
        public ExpectedException expectedException = ExpectedException.none();

        private final ByteArrayAttributeConverter sut = new ByteArrayAttributeConverter();

        @Test
        public void convertToByteArray() throws Exception {
            final byte[] bytes = {0x30, 0x31};
            assertThat(sut.convertToDatabase(bytes, byte[].class), is(bytes));
        }

        @Test
        public void convertToString_shouldThrowException() throws Exception {
            expectedException.expect(IllegalArgumentException.class);
            expectedException.expectMessage("unsupported database type:java.lang.String");
            sut.convertToDatabase(new byte[0], String.class);
        }
    }

    public static class ConvertFromDatabaseTest {

        @Rule
        public ExpectedException expectedException = ExpectedException.none();

        private final ByteArrayAttributeConverter sut = new ByteArrayAttributeConverter();
        
        @Test
        public void convertFromNull() throws Exception {
            assertThat(sut.convertFromDatabase(null), is(nullValue()));
        }

        @Test
        public void convertFromByte() throws Exception {
            final byte[] bytes = {0x30, 0x31, 0x32};
            assertThat(sut.convertFromDatabase(bytes), is(bytes));
        }

        @Test
        public void convertFromBlob(@Mocked final Blob mockBlob) throws Exception {

            new Expectations() {{
                mockBlob.length();
                result = 5;
                mockBlob.getBytes(0, 5);
                result = new byte[] {0x30, 0x31, 0x32, 0x33, 0x34};
            }};
            assertThat(sut.convertFromDatabase(mockBlob), is(new byte[] {0x30, 0x31, 0x32, 0x33, 0x34}));
        }

        @Test
        public void blobAccessError_shouldThrowException(@Mocked final Blob mockBlob) throws Exception {
            expectedException.expect(DbAccessException.class);
            expectedException.expectMessage("BLOB access failed");
            expectedException.expectCause(CoreMatchers.<Throwable>instanceOf(SQLException.class));
            new Expectations() {{
                mockBlob.length();
                result = new SQLException("blob access error");
            }};

            sut.convertFromDatabase(mockBlob);
        }

        @Test
        public void convertFromString_shouldThrowException() throws Exception {
            expectedException.expect(IllegalArgumentException.class);
            expectedException.expectMessage("unsupported data type:java.lang.String, value:abcdefg");

            sut.convertFromDatabase("abcdefg");
        }
    }
}