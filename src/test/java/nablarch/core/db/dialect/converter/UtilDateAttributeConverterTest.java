package nablarch.core.db.dialect.converter;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import java.sql.Timestamp;
import java.util.Date;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

/**
 * {@link UtilDateAttributeConverter}のテスト。
 */
@RunWith(Enclosed.class)
public class UtilDateAttributeConverterTest {

    public static class ConvertToDatabaseTest {
        
        @Rule
        public ExpectedException expectedException = ExpectedException.none();

        private final UtilDateAttributeConverter sut = new UtilDateAttributeConverter();

        private static final Date INPUT = Timestamp.valueOf("2016-12-01 01:02:03.123321");
        
        @Test
        public void convertToSqlDate() throws Exception {
            assertThat(sut.convertToDatabase(INPUT, java.sql.Date.class), is(java.sql.Date.valueOf("2016-12-01")));
        }
        
        @Test
        public void convertToTimestamp() throws Exception {
            assertThat(sut.convertToDatabase(INPUT, Timestamp.class),
                    is(Timestamp.valueOf("2016-12-01 01:02:03.123")));
        }
        
        @Test
        public void convertToLong_shouldThrowException() throws Exception {
            expectedException.expect(IllegalArgumentException.class);
            expectedException.expectMessage("unsupported database type:java.lang.Long");
            sut.convertToDatabase(INPUT, Long.class);
        }
    }
    
    public static class ConvertFromDatabaseTest {
        @Rule
        public ExpectedException expectedException = ExpectedException.none();

        private final UtilDateAttributeConverter sut = new UtilDateAttributeConverter();
        
        @Test
        public void convertFromNull() throws Exception {
            assertThat(sut.convertFromDatabase(null), is(nullValue()));
        }
        
        @Test
        public void convertFromSqlDate() throws Exception {
            final java.sql.Date input = java.sql.Date.valueOf("2016-12-13");
            assertThat(sut.convertFromDatabase(input), is(new Date(input.getTime())));
        }
        
        @Test
        public void convertFromTimestamp() throws Exception {
            final Timestamp input = Timestamp.valueOf("2016-12-03 01:02:03.123321");
            assertThat(sut.convertFromDatabase(input), is(new Date(input.getTime())));
        }
        
        @Test
        public void convertFromString_shouldThrowException() throws Exception {
            expectedException.expect(IllegalArgumentException.class);
            expectedException.expectMessage("unsupported data type:java.lang.String, value:20161203");

            sut.convertFromDatabase("20161203");
        }
    }

}