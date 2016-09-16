package nablarch.core.db.statement;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import nablarch.core.log.LogTestSupport;
import nablarch.core.log.Logger;
import nablarch.core.util.Builder;

import org.junit.Test;

public class SqlLogUtilTest extends LogTestSupport {

    /**
     * 設定不備の場合に例外がスローされること。
     */
    @Test
    public void testInitialize() {
        
        System.setProperty("nablarch.appLog.filePath", "classpath:nablarch/core/log/app/app-log-invalid.properties");
        
        try { 
            SqlLogUtil.initialize();
            fail("must throw exception.");
        } catch (Throwable e) {
            assertThat(e.getCause().getMessage(), is("invalid sqlLogFormatter.className"));
        }
    }

    /**
     * カスタムフォーマッタで正しく動作すること。
     */
    @Test
    public void testCustom() {
        System.setProperty("nablarch.appLog.filePath", "classpath:nablarch/core/log/app/app-log-settings.properties");
        System.setProperty("sqlLogFormatter.className", "nablarch.core.db.statement.SqlLogUtilTest$CustomSqlLogFormatter");

        String message = SqlLogUtil.endRetrieve("method_name", 1, 2, 3);
        assertThat(message, is("method_name:1:2:3"));
    }

    /**
     * デフォルト設定で正しく出力されること。
     */
    @Test
    public void testDefault() {
        
        System.setProperty("nablarch.appLog.filePath", "classpath:nablarch/core/log/app/app-log-default.properties");
        
        String message = SqlLogUtil.startRetrieve("methodName_startRetrieve", "sql_startRetrieve", 1, 2, 3, 4, "additionalInfo_startRetrieve");
        assertThat(message, is(Builder.concat("methodName_startRetrieve", Logger.LS,
                                                  "\tSQL = [sql_startRetrieve]", Logger.LS,
                                                  "\tstart_position = [1] size = [2]", Logger.LS,
                                                  "\tquery_timeout = [3] fetch_size = [4]", Logger.LS,
                                                  "\tadditional_info:", Logger.LS,
                                                  "\tadditionalInfo_startRetrieve")));
        
        message = SqlLogUtil.endRetrieve("methodName_endRetrieve", 1, 2, 3);
        assertThat(message, is(Builder.concat("methodName_endRetrieve", Logger.LS,
                                                  "\texecute_time(ms) = [1] retrieve_time(ms) = [2] count = [3]")));
        
        message = SqlLogUtil.startExecute("methodName_startExecute", "sql_startExecute", "additionalInfo_startExecute");
        assertThat(message, is(Builder.concat("methodName_startExecute", Logger.LS,
                                              "\tSQL = [sql_startExecute]", Logger.LS,
                                              "\tadditional_info:", Logger.LS,
                                              "\tadditionalInfo_startExecute")));
        
        message = SqlLogUtil.endExecute("methodName_endExecute", 1);
        assertThat(message, is(Builder.concat("methodName_endExecute", Logger.LS,
                                                  "\texecute_time(ms) = [1]")));
        
        message = SqlLogUtil.startExecuteQuery("methodName_startExecuteQuery", "sql_startExecuteQuery", "additionalInfo_startExecuteQuery");
        assertThat(message, is(Builder.concat("methodName_startExecuteQuery", Logger.LS,
                                              "\tSQL = [sql_startExecuteQuery]", Logger.LS,
                                              "\tadditional_info:", Logger.LS,
                                              "\tadditionalInfo_startExecuteQuery")));
        
        message = SqlLogUtil.endExecuteQuery("methodName_endExecuteQuery", 1);
        assertThat(message, is(Builder.concat("methodName_endExecuteQuery", Logger.LS,
                                                  "\texecute_time(ms) = [1]")));
        
        message = SqlLogUtil.startExecuteUpdate("methodName_startExecuteUpdate", "sql_startExecuteUpdate", "additionalInfo_startExecuteUpdate");
        assertThat(message, is(Builder.concat("methodName_startExecuteUpdate", Logger.LS,
                                              "\tSQL = [sql_startExecuteUpdate]", Logger.LS,
                                              "\tadditional_info:", Logger.LS,
                                              "\tadditionalInfo_startExecuteUpdate")));
        
        message = SqlLogUtil.endExecuteUpdate("methodName_endExecuteUpdate", 1, 2);
        assertThat(message, is(Builder.concat("methodName_endExecuteUpdate", Logger.LS,
                                                  "\texecute_time(ms) = [1] update_count = [2]")));
        
        message = SqlLogUtil.startExecuteBatch("methodName_startExecuteBatch", "sql_startExecuteBatch", "additionalInfo_startExecuteBatch");
        assertThat(message, is(Builder.concat("methodName_startExecuteBatch", Logger.LS,
                                              "\tSQL = [sql_startExecuteBatch]", Logger.LS,
                                              "\tadditional_info:", Logger.LS,
                                              "\tadditionalInfo_startExecuteBatch")));
        
        message = SqlLogUtil.endExecuteBatch("methodName_endExecuteBatch", 1, 2);
        assertThat(message, is(Builder.concat("methodName_endExecuteBatch", Logger.LS,
                                                  "\texecute_time(ms) = [1] batch_count = [2]")));
    }

    public static class CustomSqlLogFormatter extends SqlLogFormatter {

        @Override
        public String endRetrieve(String methodName, long executeTime, long retrieveTime, int count) {
            return methodName + ':' + executeTime + ':' + retrieveTime + ':' + count;
        }
    }
}
