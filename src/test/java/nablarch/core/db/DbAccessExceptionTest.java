package nablarch.core.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.sql.SQLException;

import org.junit.Test;

/**
 * Javadoc
 *
 * @author Hisaaki Sioiri
 */
public class DbAccessExceptionTest {

    @Test
    public void testGetSQLState() throws Exception {
        SQLException e = new SQLException("test", "sqlstate", 12);
        DbAccessException target = new DbAccessException("target", e);
        assertNotNull("事前検証", e.getSQLState());
        assertEquals("発生したSQLExceptionと同じ結果が返却されること。", e.getSQLState(), target.getSQLState());
    }

    @Test
    public void testGetErrorCode() throws Exception {
        SQLException e = new SQLException("test", "sqlstate", 12);
        DbAccessException exception = new DbAccessException("", e);
        assertNotNull("事前検証", e.getErrorCode());
        assertEquals("発生したSQLExceptionと同じ結果が返却されること。", e.getErrorCode(), exception.getErrorCode());
    }
}
