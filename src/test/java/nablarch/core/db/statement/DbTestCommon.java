package nablarch.core.db.statement;


import java.sql.Connection;
import java.sql.Statement;

/**
 * Created with IntelliJ IDEA.
 * User: tie301686
 * Date: 2014/09/03
 * Time: 16:34
 */
class DbTestCommon {

    static void alterDateFormat(Connection conn) throws Exception {

        final Statement statement = conn.createStatement();
        statement.execute("ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'");
        statement.execute("ALTER SESSION SET NLS_TIME_FORMAT='HH24:MI:SSXFF'");
        statement.execute("ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'");
        statement.execute("ALTER SESSION SET NLS_DATE_LANGUAGE = 'JAPANESE'");
    }
}
