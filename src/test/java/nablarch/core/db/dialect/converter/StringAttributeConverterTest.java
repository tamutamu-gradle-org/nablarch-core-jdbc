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
        public void emptyStringToStringType() throws Exception {
            assertThat(sut.convertToDatabase("", String.class), is(""));
        }

        @Test
        public void toNotStringType_shouldThrowException() throws Exception {
            expectedException.expect(IllegalArgumentException.class);
            expectedException.expectMessage("unsupported database type:java.lang.Integer");

            sut.convertToDatabase("1234", Integer.class);
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