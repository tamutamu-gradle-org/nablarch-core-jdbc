package nablarch.core.db.dialect;

import oracle.sql.BLOB;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;

import static org.junit.Assert.assertTrue;

/**
 * {@link DefaultSqlTypeConverter}のテスト
 * @author ryo asato
 */
public class DefaultSqlTypeConverterTest {

    private DefaultSqlTypeConverter sut = new DefaultSqlTypeConverter();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void convert() {
        assertTrue(sut.convertToJavaClass(Types.BIT).isAssignableFrom(Boolean.class));
        assertTrue(sut.convertToJavaClass(Types.TINYINT).isAssignableFrom(Byte.class));
        assertTrue(sut.convertToJavaClass(Types.SMALLINT).isAssignableFrom(Short.class));
        assertTrue(sut.convertToJavaClass(Types.INTEGER).isAssignableFrom(Integer.class));
        assertTrue(sut.convertToJavaClass(Types.BIGINT).isAssignableFrom(Long.class));
        assertTrue(sut.convertToJavaClass(Types.FLOAT).isAssignableFrom(Double.class));
        assertTrue(sut.convertToJavaClass(Types.REAL).isAssignableFrom(Float.class));
        assertTrue(sut.convertToJavaClass(Types.DOUBLE).isAssignableFrom(Double.class));
        assertTrue(sut.convertToJavaClass(Types.NUMERIC).isAssignableFrom(BigDecimal.class));
        assertTrue(sut.convertToJavaClass(Types.DECIMAL).isAssignableFrom(BigDecimal.class));
        assertTrue(sut.convertToJavaClass(Types.CHAR).isAssignableFrom(String.class));
        assertTrue(sut.convertToJavaClass(Types.VARCHAR).isAssignableFrom(String.class));
        assertTrue(sut.convertToJavaClass(Types.LONGVARCHAR).isAssignableFrom(String.class));
        assertTrue(sut.convertToJavaClass(Types.DATE).isAssignableFrom(Date.class));
        assertTrue(sut.convertToJavaClass(Types.TIME).isAssignableFrom(Time.class));
        assertTrue(sut.convertToJavaClass(Types.TIMESTAMP).isAssignableFrom(Timestamp.class));
        assertTrue(sut.convertToJavaClass(Types.BINARY).isAssignableFrom(byte[].class));
        assertTrue(sut.convertToJavaClass(Types.VARBINARY).isAssignableFrom(byte[].class));
        assertTrue(sut.convertToJavaClass(Types.LONGVARBINARY).isAssignableFrom(byte[].class));
        assertTrue(sut.convertToJavaClass(Types.BLOB).isAssignableFrom(byte[].class));
        assertTrue(sut.convertToJavaClass(Types.CLOB).isAssignableFrom(String.class));
        assertTrue(sut.convertToJavaClass(Types.BOOLEAN).isAssignableFrom(Boolean.class));
    }

    @Test
    public void convertAbnormalSqlType() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("unsupported sqlType: 12345");
        sut.convertToJavaClass(12345);
    }
}
