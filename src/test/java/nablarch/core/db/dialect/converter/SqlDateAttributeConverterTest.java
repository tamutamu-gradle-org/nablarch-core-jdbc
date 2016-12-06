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
 * {@link SqlDateAttributeConverter}のテスト。
 */
@RunWith(Enclosed.class)
public class SqlDateAttributeConverterTest {

    public static class ConvertToDatabaseTest {
        
        @Rule
        public ExpectedException expectedException = ExpectedException.none();

        private final SqlDateAttributeConverter sut = new SqlDateAttributeConverter();
        
        private static final Date INPUT = Date.valueOf("2016-12-01");

        @Test
        public void convertToSqlDate() throws Exception {
            assertThat(sut.convertToDatabase(INPUT, Date.class), is(INPUT));
        }
        
        @Test
        public void convertToTimestamp() throws Exception {
            assertThat(sut.convertToDatabase(INPUT, Timestamp.class),
                    is(Timestamp.valueOf("2016-12-01 00:00:00.000000")));
        }
        
        @Test
        public void convertToString() throws Exception {
            assertThat(sut.convertToDatabase(INPUT, String.class), is("2016-12-01"));
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

        private final SqlDateAttributeConverter sut = new SqlDateAttributeConverter();
        
        @Test
        public void convertFromNull() throws Exception {
            assertThat(sut.convertFromDatabase(null), is(nullValue()));
        }
        
        @Test
        public void convertFromSqlDate() throws Exception {
            assertThat(sut.convertFromDatabase(Date.valueOf("2016-12-02")), is(Date.valueOf("2016-12-02")));
        }

        @Test
        public void convertFromString() throws Exception {
            assertThat(sut.convertFromDatabase("2016-12-03"), is(Date.valueOf("2016-12-03")));
        }
        
        @Test
        public void convertFromTimestamp() throws Exception {
            final Timestamp input = Timestamp.valueOf("2016-12-03 01:02:03.123321");
            assertThat(sut.convertFromDatabase(input), is(Date.valueOf("2016-12-03")));
        }
        
        @Test
        public void convertFromUtilDate() throws Exception {
            final java.util.Date input = new java.util.Date(Timestamp.valueOf("2016-12-03 01:02:03.123321").getTime());
            assertThat(sut.convertFromDatabase(input), is(Date.valueOf("2016-12-03")));
        }
        
        @Test
        public void convertFromInteger_shouldThrowException() throws Exception {
            expectedException.expect(IllegalArgumentException.class);
            expectedException.expectMessage("unsupported data type:java.lang.Integer, value:100");
            
            sut.convertFromDatabase(100);
        }
    }

}