package nablarch.core.db.statement;

import java.util.Map;

import nablarch.core.log.LogUtil;
import nablarch.core.log.LogUtil.ObjectCreator;
import nablarch.core.log.app.AppLogUtil;
import nablarch.core.util.ObjectUtil;

/**
 * SQLログの出力を助けるユーティリティ。
 * @author Kiyohito Itoh
 */
public final class SqlLogUtil {
    
    /** 隠蔽コンストラクタ */
    private SqlLogUtil() {
    }
    
    /** {@link SqlLogFormatter}のクラス名 */
    private static final String PROPS_CLASS_NAME = SqlLogFormatter.PROPS_PREFIX + "className";
    
    /** {@link SqlLogFormatter}を生成する{@link ObjectCreator} */
    private static final ObjectCreator<SqlLogFormatter> SQL_LOG_FORMATTER_CREATOR = new ObjectCreator<SqlLogFormatter>() {
        public SqlLogFormatter create() {
            SqlLogFormatter formatter = null;
            Map<String, String> props = AppLogUtil.getProps();
            if (props.containsKey(PROPS_CLASS_NAME)) {
                String className =  props.get(PROPS_CLASS_NAME);
                formatter = ObjectUtil.createInstance(className);
            } else {
                formatter = new SqlLogFormatter();
            }
            return formatter;
        }
    };
    
    /** クラスローダに紐付く{@link SqlLogFormatter}を生成する。 */
    public static void initialize() {
        getSqlLogWriter();
    }
    
    /**
     * クラスローダに紐付く{@link SqlLogFormatter}を取得する。
     * @return {@link SqlLogFormatter}
     */
    private static SqlLogFormatter getSqlLogWriter() {
        return LogUtil.getObjectBoundToClassLoader(SQL_LOG_FORMATTER_CREATOR);
    }
    
    /**
     * SqlPStatement#retrieveメソッドの検索開始時のSQLログをフォーマットする。
     * @param methodName メソッド名 メソッド名
     * @param sql SQL文 SQL文
     * @param startPosition 取得開始位置
     * @param size 取得最大件数
     * @param queryTimeout タイムアウト時間
     * @param fetchSize フェッチする行数
     * @param additionalInfo 付加情報 付加情報
     * @return フォーマット済みのメッセージ
     */
    public static String startRetrieve(String methodName, String sql, int startPosition, int size, int queryTimeout, int fetchSize, String additionalInfo) {
        return getSqlLogWriter().startRetrieve(methodName, sql, startPosition, size, queryTimeout, fetchSize, additionalInfo);
    }
    
    /**
     * SqlPStatement#retrieveメソッドの検索終了時のSQLログをフォーマットする。
     * @param methodName メソッド名 メソッド名
     * @param executeTime 実行時間
     * @param retrieveTime データ取得時間
     * @param count 検索件数
     * @return フォーマット済みのメッセージ
     */
    public static String endRetrieve(String methodName, long executeTime, long retrieveTime, int count) {
        return getSqlLogWriter().endRetrieve(methodName, executeTime, retrieveTime, count);
    }
    
    /**
     * SqlPStatement#executeQueryメソッドの検索開始時のSQLログをフォーマットする。
     * @param methodName メソッド名 メソッド名
     * @param sql SQL文
     * @param additionalInfo 付加情報
     * @return フォーマット済みメッセージ
     */
    public static String startExecuteQuery(String methodName, String sql, String additionalInfo) {
        return getSqlLogWriter().startExecuteQuery(methodName, sql, additionalInfo);
    }
    
    /**
     * SqlPStatement#executeQueryメソッドの検索終了時のSQLログをフォーマットする。
     * @param methodName メソッド名 メソッド名
     * @param executeTime 実行時間
     * @return フォーマット済みメッセージ
     */
    public static String endExecuteQuery(String methodName, long executeTime) {
        return getSqlLogWriter().endExecuteQuery(methodName, executeTime);
    }
    
    /**
     * SqlPStatement#executeUpdateメソッドの更新開始時のSQLログをフォーマットする。
     * @param methodName メソッド名 メソッド名
     * @param sql SQL文
     * @param additionalInfo 付加情報
     * @return フォーマット済みメッセージ
     */
    public static String startExecuteUpdate(String methodName, String sql, String additionalInfo) {
        return getSqlLogWriter().startExecuteUpdate(methodName, sql, additionalInfo);
    }
    
    /**
     * SqlPStatement#executeUpdateメソッドの更新終了時のSQLログをフォーマットする。
     * @param methodName メソッド名 メソッド名
     * @param executeTime 実行時間
     * @param updateCount 更新件数
     * @return フォーマット済みメッセージ
     */
    public static String endExecuteUpdate(String methodName, long executeTime, int updateCount) {
        return getSqlLogWriter().endExecuteUpdate(methodName, executeTime, updateCount);
    }
    
    /**
     * SqlPStatement#executeメソッドの実行開始時のSQLログをフォーマットする。
     * @param methodName メソッド名 メソッド名
     * @param sql SQL文
     * @param additionalInfo 付加情報
     * @return フォーマット済みメッセージ
     */
    public static String startExecute(String methodName, String sql, String additionalInfo) {
        return getSqlLogWriter().startExecute(methodName, sql, additionalInfo);
    }
    
    /**
     * SqlPStatement#executeメソッドの実行終了時のSQLログをフォーマットする。
     * @param methodName メソッド名 メソッド名
     * @param executeTime 実行時間
     * @return フォーマット済みメッセージ
     */
    public static String endExecute(String methodName, long executeTime) {
        return getSqlLogWriter().endExecute(methodName, executeTime);
    }
    
    /**
     * SqlPStatement#executeBatchメソッドの更新開始時のSQLログをフォーマットする。
     * @param methodName メソッド名
     * @param sql SQL文
     * @param additionalInfo 付加情報
     * @return フォーマット済みメッセージ
     */
    public static String startExecuteBatch(String methodName, String sql, String additionalInfo) {
        return getSqlLogWriter().startExecuteBatch(methodName, sql, additionalInfo);
    }
    
    /**
     * SqlPStatement#executeBatchメソッドの更新終了時のSQLログをフォーマットする。
     * @param methodName メソッド名
     * @param executeTime 実行時間
     * @param batchCount バッチ件数
     * @return フォーマット済みメッセージ
     */
    public static String endExecuteBatch(String methodName, long executeTime, int batchCount) {
        return getSqlLogWriter().endExecuteBatch(methodName, executeTime, batchCount);
    }
}
