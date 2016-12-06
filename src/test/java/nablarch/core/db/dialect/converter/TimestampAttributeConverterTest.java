package nablarch.core.db.dialect.converter;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import java.sql.Date;
import java.sql.Timestamp;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

/**
 * {@link TimestampAttributeConverter}のテスト。
 */
@RunWith(Enclosed.class)
public class TimestampAttributeConverterTest {

    private static final Timestamp INPUT = Timestamp.valueOf("2016-01-02 11:22:33.123");

    public static class ConvertToDatabaseTest {

        @Rule
        public ExpectedException expectedException = ExpectedException.none();

        private final TimestampAttributeConverter sut = new TimestampAttributeConverter();


        @Test
        public void convertToTimestamp() throws Exception {
            assertThat(sut.convertToDatabase(INPUT, Timestamp.class), is(INPUT));
        }

        @Test
        public void convertToSqlDate() throws Exception {
            assertThat(sut.convertToDatabase(INPUT, Date.class), is(Date.valueOf("2016-01-02")));
        }

        @Test
        public void convertToString() throws Exception {
            assertThat(sut.convertToDatabase(INPUT, String.class), is("2016-01-02 11:22:33.123"));
        }

        @Test
        public void convertFromInteger_shouldThrowException() throws Exception {
            expectedException.expect(IllegalArgumentException.class);
            expectedException.expectMessage("unsupported database type:java.lang.Integer");

            sut.convertToDatabase(INPUT, Integer.class);
        }
    }

    public static class ConvertFromDatabaseTest {

        @Rule
        public ExpectedException expectedException = ExpectedException.none();

        private final TimestampAttributeConverter sut = new TimestampAttributeConverter();
        
        @Test
        public void convertFromNull() throws Exception {
            assertThat(sut.convertFromDatabase(null), is(nullValue()));
        }

        @Test
        public void convertFromTimestamp() throws Exception {
            assertThat(sut.convertFromDatabase(INPUT), is(INPUT));
        }

        @Test
        public void convertFromUtilDate() throws Exception {
            final java.util.Date date = new java.util.Date(Date.valueOf("2016-11-30")
                                                               .getTime());
            assertThat(sut.convertFromDatabase(date), is(Timestamp.valueOf("2016-11-30 00:00:00.000000")));
        }

        @Test
        public void convertFromSqlDate() throws Exception {
            final Date input = Date.valueOf("2016-11-30");
            assertThat(sut.convertFromDatabase(input), is(Timestamp.valueOf("2016-11-30 00:00:00.000000")));
        }

        @Test
        public void convertFromString() throws Exception {
            assertThat(sut.convertFromDatabase("2016-11-30 17:04:00.123456"),
                    is(Timestamp.valueOf("2016-11-30 17:04:00.123456")));
        }
        
        @Test
        public void convertFromInteger() throws Exception {
            expectedException.expect(IllegalArgumentException.class);
            expectedException.expectMessage("unsupported data type:java.lang.Integer, value:100");
            
            sut.convertFromDatabase(100);
        }
    }
}