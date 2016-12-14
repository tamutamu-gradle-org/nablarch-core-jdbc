package nablarch.core.db.dialect.converter;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Ref;
import java.sql.Timestamp;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

/**
 * {@link StringAttributeConverter}のテスト。
 */
@RunWith(Enclosed.class)
public class StringAttributeConverterTest {

    /**
     * {@link StringAttributeConverter#convertToDatabase(String, Class)}のテスト。
     */
    public static class ConvertToDatabaseTest {

        private final StringAttributeConverter sut = new StringAttributeConverter();

        @Rule
        public ExpectedException expectedException = ExpectedException.none();

        @Test
        public void toStringType() throws Exception {
            assertThat(sut.convertToDatabase("abc", String.class), is("abc"));
        }

        @Test
        public void toBigDecimalType() throws Exception {
            assertThat(sut.convertToDatabase("1", BigDecimal.class), is(BigDecimal.ONE));
        }

        @Test
        public void toLongType() throws Exception {
            assertThat(sut.convertToDatabase("1", Long.class), is(1L));
        }

        @Test
        public void toIntegerType() throws Exception {
            assertThat(sut.convertToDatabase("1", Integer.class), is(1));
        }

        @Test
        public void toShortType() throws Exception {
            assertThat(sut.convertToDatabase("1", Short.class), is(Short.valueOf("1")));
        }

        @Test
        public void toDateType() throws Exception {
            assertThat(sut.convertToDatabase("2016-12-02", Date.class), is(Date.valueOf("2016-12-02")));
        }

        @Test
        public void toTimestampType() throws Exception {
            assertThat(sut.convertToDatabase("2016-12-02 12:34:56.789123", Timestamp.class), is(Timestamp.valueOf("2016-12-02 12:34:56.789123")));
        }

        @Test
        public void emptyStringToStringType() throws Exception {
            assertThat(sut.convertToDatabase("", String.class), is(""));
        }

        @Test
        public void toNotStringType_shouldThrowException() throws Exception {
            expectedException.expect(IllegalArgumentException.class);
            expectedException.expectMessage("unsupported database type:java.sql.Ref");

            sut.convertToDatabase("1234", Ref.class);
        }
    }

    /**
     * {@link StringAttributeConverter#convertFromDatabase(Object)}のテスト。
     */
    public static class ConvertFromDatabaseTest {

        private final StringAttributeConverter sut = new StringAttributeConverter();
        
        @Test
        public void fromString() throws Exception {
            assertThat(sut.convertFromDatabase("123"), is("123"));
        }
        
        @Test
        public void fromInteger() throws Exception {
            assertThat(sut.convertFromDatabase(321), is("321"));
        }
        
        @Test
        public void convertFromBigDecimal() throws Exception {
            assertThat(sut.convertFromDatabase(new BigDecimal("1.1")), is("1.1"));
        }

        @Test
        public void fromNull() throws Exception {
            assertThat(sut.convertFromDatabase(null), is(nullValue()));
        }
    }
}