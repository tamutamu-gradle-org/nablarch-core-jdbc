package nablarch.core.db.statement;

import java.util.HashMap;
import java.util.Map;

import nablarch.core.log.LogItem;
import nablarch.core.log.LogUtil;
import nablarch.core.log.app.AppLogUtil;
import nablarch.core.util.StringUtil;
import nablarch.core.util.annotation.Published;

/**
 * SQLログを出力するクラス。
 * @author Kiyohito Itoh
 */
@Published(tag = "architect")
public class SqlLogFormatter {
    
    // PROPS
    
    /** プロパティ名のプレフィックス */
    public static final String PROPS_PREFIX = "sqlLogFormatter.";
    
    /** SqlPStatement#retrieveメソッドの検索開始時のフォーマットを取得する際に使用するプロパティ名 */
    private static final String PROPS_START_RETRIEVE_FORMAT = PROPS_PREFIX + "startRetrieveFormat";
    
    /** SqlPStatement#retrieveメソッドの検索終了時のフォーマットを取得する際に使用するプロパティ名 */
    private static final String PROPS_END_RETRIEVE_FORMAT = PROPS_PREFIX + "endRetrieveFormat";
    
    /** SqlPStatement#executeメソッドの実行開始時のフォーマットを取得する際に使用するプロパティ名 */
    private static final String PROPS_START_EXECUTE_FORMAT = PROPS_PREFIX + "startExecuteFormat";
    
    /** SqlPStatement#executeメソッドの実行終了時のフォーマットを取得する際に使用するプロパティ名 */
    private static final String PROPS_END_EXECUTE_FORMAT = PROPS_PREFIX + "endExecuteFormat";
    
    /** SqlPStatement#executeQueryメソッドの検索開始時のフォーマットを取得する際に使用するプロパティ名 */
    private static final String PROPS_START_EXECUTE_QUERY_FORMAT = PROPS_PREFIX + "startExecuteQueryFormat";
    
    /** SqlPStatement#executeQueryメソッドの検索終了時のフォーマットを取得する際に使用するプロパティ名 */
    private static final String PROPS_END_EXECUTE_QUERY_FORMAT = PROPS_PREFIX + "endExecuteQueryFormat";
    
    /** SqlPStatement#executeUpdateメソッドの更新開始時のフォーマットを取得する際に使用するプロパティ名 */
    private static final String PROPS_START_EXECUTE_UPDATE_FORMAT = PROPS_PREFIX + "startExecuteUpdateFormat";
    
    /** SqlPStatement#executeUpdateメソッドの更新終了時のフォーマットを取得する際に使用するプロパティ名 */
    private static final String PROPS_END_EXECUTE_UPDATE_FORMAT = PROPS_PREFIX + "endExecuteUpdateFormat";
    
    /** SqlPStatement#executeBatchメソッドの更新開始時のフォーマットを取得する際に使用するプロパティ名 */
    private static final String PROPS_START_EXECUTE_BATCH_FORMAT = PROPS_PREFIX + "startExecuteBatchFormat";
    
    /** SqlPStatement#executeBatchメソッドの更新終了時のフォーマットを取得する際に使用するプロパティ名 */
    private static final String PROPS_END_EXECUTE_BATCH_FORMAT = PROPS_PREFIX + "endExecuteBatchFormat";
    
    // DEFAULT FORMAT
    
    /** SqlPStatement#retrieveメソッドの検索開始時のデフォルトのフォーマット */
    private static final String DEFAULT_START_RETRIEVE_FORMAT
        = "$methodName$\n\tSQL = [$sql$]"
        + "\n\tstart_position = [$startPosition$] size = [$size$]\n\tquery_timeout = [$queryTimeout$] fetch_size = [$fetchSize$]"
        + "\n\tadditional_info:\n\t$additionalInfo$";
    
    /** SqlPStatement#retrieveメソッドの検索終了時のデフォルトのフォーマット */
    private static final String DEFAULT_END_RETRIEVE_FORMAT
        = "$methodName$\n\texecute_time(ms) = [$executeTime$] retrieve_time(ms) = [$retrieveTime$] count = [$count$]";
    
    /** SqlPStatement#executeメソッドの実行開始時のデフォルトのフォーマット */
    private static final String DEFAULT_START_EXECUTE_FORMAT = "$methodName$\n\tSQL = [$sql$]\n\tadditional_info:\n\t$additionalInfo$";
    
    /** SqlPStatement#executeメソッドの実行終了時のデフォルトのフォーマット */
    private static final String DEFAULT_END_EXECUTE_FORMAT = "$methodName$\n\texecute_time(ms) = [$executeTime$]";
    
    /** SqlPStatement#executeQueryメソッドの検索開始時のデフォルトのフォーマット */
    private static final String DEFAULT_START_EXECUTE_QUERY_FORMAT = "$methodName$\n\tSQL = [$sql$]\n\tadditional_info:\n\t$additionalInfo$";
    
    /** SqlPStatement#executeQueryメソッドの検索終了時のデフォルトのフォーマット */
    private static final String DEFAULT_END_EXECUTE_QUERY_FORMAT = "$methodName$\n\texecute_time(ms) = [$executeTime$]";
    
    /** SqlPStatement#executeUpdateメソッドの更新開始時のデフォルトのフォーマット */
    private static final String DEFAULT_START_EXECUTE_UPDATE_FORMAT = "$methodName$\n\tSQL = [$sql$]\n\tadditional_info:\n\t$additionalInfo$";
    
    /** SqlPStatement#executeUpdateメソッドの更新終了時のデフォルトのフォーマット */
    private static final String DEFAULT_END_EXECUTE_UPDATE_FORMAT = "$methodName$\n\texecute_time(ms) = [$executeTime$] update_count = [$updateCount$]";
    
    /** SqlPStatement#executeBatchメソッドの更新開始時のデフォルトのフォーマット */
    private static final String DEFAULT_START_EXECUTE_BATCH_FORMAT = "$methodName$\n\tSQL = [$sql$]\n\tadditional_info:\n\t$additionalInfo$";
    
    /** SqlPStatement#executeBatchメソッドの更新終了時のデフォルトのフォーマット */
    private static final String DEFAULT_END_EXECUTE_BATCH_FORMAT = "$methodName$\n\texecute_time(ms) = [$executeTime$] batch_count = [$batchCount$]";
    
    /** SqlPStatement#retrieveメソッドの検索開始時のログ出力項目 */
    private LogItem<SqlLogContext>[] startRetrieveLogItems;
    
    /** SqlPStatement#retrieveメソッドの検索終了時のログ出力項目 */
    private LogItem<SqlLogContext>[] endRetrieveLogItems;
    
    /** SqlPStatement#executeメソッドの実行開始時のログ出力項目 */
    private LogItem<SqlLogContext>[] startExecuteLogItems;
    
    /** SqlPStatement#executeメソッドの実行終了時のログ出力項目 */
    private LogItem<SqlLogContext>[] endExecuteLogItems;
    
    /** SqlPStatement#executeQueryメソッドの検索開始時のログ出力項目 */
    private LogItem<SqlLogContext>[] startExecuteQueryLogItems;
    
    /** SqlPStatement#executeQueryメソッドの検索終了時のログ出力項目 */
    private LogItem<SqlLogContext>[] endExecuteQueryLogItems;
    
    /** SqlPStatement#executeUpdateメソッドの更新開始時のログ出力項目 */
    private LogItem<SqlLogContext>[] startExecuteUpdateLogItems;
    
    /** SqlPStatement#executeUpdateメソッドの更新終了時のログ出力項目 */
    private LogItem<SqlLogContext>[] endExecuteUpdateLogItems;
    
    /** SqlPStatement#executeBatchメソッドの更新開始時のログ出力項目 */
    private LogItem<SqlLogContext>[] startExecuteBatchLogItems;
    
    /** SqlPStatement#executeBatchメソッドの更新終了時のログ出力項目 */
    private LogItem<SqlLogContext>[] endExecuteBatchLogItems;
    
    /**
     * フォーマット済みのログ出力項目を初期化する。
     */
    public SqlLogFormatter() {
        
        Map<String, String> props = AppLogUtil.getProps();
        Map<String, LogItem<SqlLogContext>> logItems = getLogItems();
        
        startRetrieveLogItems = getFormattedLogItems(logItems, props, PROPS_START_RETRIEVE_FORMAT, DEFAULT_START_RETRIEVE_FORMAT);
        
        endRetrieveLogItems = getFormattedLogItems(logItems, props, PROPS_END_RETRIEVE_FORMAT, DEFAULT_END_RETRIEVE_FORMAT);
        
        startExecuteLogItems = getFormattedLogItems(logItems, props, PROPS_START_EXECUTE_FORMAT, DEFAULT_START_EXECUTE_FORMAT);
        
        endExecuteLogItems = getFormattedLogItems(logItems, props, PROPS_END_EXECUTE_FORMAT, DEFAULT_END_EXECUTE_FORMAT);
        
        startExecuteQueryLogItems = getFormattedLogItems(logItems, props, PROPS_START_EXECUTE_QUERY_FORMAT, DEFAULT_START_EXECUTE_QUERY_FORMAT);
        
        endExecuteQueryLogItems = getFormattedLogItems(logItems, props, PROPS_END_EXECUTE_QUERY_FORMAT, DEFAULT_END_EXECUTE_QUERY_FORMAT);
        
        startExecuteUpdateLogItems = getFormattedLogItems(logItems, props, PROPS_START_EXECUTE_UPDATE_FORMAT, DEFAULT_START_EXECUTE_UPDATE_FORMAT);
        
        endExecuteUpdateLogItems = getFormattedLogItems(logItems, props, PROPS_END_EXECUTE_UPDATE_FORMAT, DEFAULT_END_EXECUTE_UPDATE_FORMAT);
        
        startExecuteBatchLogItems = getFormattedLogItems(logItems, props, PROPS_START_EXECUTE_BATCH_FORMAT, DEFAULT_START_EXECUTE_BATCH_FORMAT);
        
        endExecuteBatchLogItems = getFormattedLogItems(logItems, props, PROPS_END_EXECUTE_BATCH_FORMAT, DEFAULT_END_EXECUTE_BATCH_FORMAT);
    }
    
    /**
     * フォーマット済みのログ出力項目を取得する。
     * @param logItems フォーマット対象のログ出力項目
     * @param props 各種ログ出力の設定情報
     * @param formatPropName フォーマットのプロパティ名
     * @param defaultFormat デフォルトのフォーマット
     * @return フォーマット済みのログ出力項目
     */
    protected LogItem<SqlLogContext>[] getFormattedLogItems(Map<String, LogItem<SqlLogContext>> logItems, Map<String, String> props,
                                                             String formatPropName, String defaultFormat) {
        String format = defaultFormat;
        if (props.containsKey(formatPropName)) {
            format = props.get(formatPropName);
        }
        return LogUtil.createFormattedLogItems(logItems, format);
    }
    
    /**
     * フォーマット対象のログ出力項目を取得する。
     * @return フォーマット対象のログ出力項目
     */
    protected Map<String, LogItem<SqlLogContext>> getLogItems() {
        Map<String, LogItem<SqlLogContext>> logItems = new HashMap<String, LogItem<SqlLogContext>>();
        logItems.put("$methodName$", new MethodNameItem());
        logItems.put("$sql$", new SqlItem());
        logItems.put("$startPosition$", new StartPositionItem());
        logItems.put("$size$", new SizeItem());
        logItems.put("$queryTimeout$", new QueryTimeoutItem());
        logItems.put("$fetchSize$", new FetchSizeItem());
        logItems.put("$executeTime$", new ExecuteTimeItem());
        logItems.put("$retrieveTime$", new RetrieveTimeItem());
        logItems.put("$count$", new CountItem());
        logItems.put("$updateCount$", new UpdateCountItem());
        logItems.put("$batchCount$", new BatchCountItem());
        logItems.put("$additionalInfo$", new AdditionalInfoItem());
        return logItems;
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
    public String startRetrieve(String methodName, String sql, int startPosition, int size, int queryTimeout, int fetchSize, String additionalInfo) {
        SqlLogContext context = new SqlLogContext();
        context.setMethodName(methodName);
        context.setSql(sql);
        context.setStartPosition(startPosition);
        context.setSize(size);
        context.setQueryTimeout(queryTimeout);
        context.setFetchSize(fetchSize);
        context.setAdditionalInfo(additionalInfo);
        return LogUtil.formatMessage(startRetrieveLogItems, context);
    }
    
    /**
     * SqlPStatement#retrieveメソッドの検索終了時のSQLログをフォーマットする。
     * @param methodName メソッド名 メソッド名
     * @param executeTime 実行時間
     * @param retrieveTime データ取得時間
     * @param count 検索件数
     * @return フォーマット済みのメッセージ
     */
    public String endRetrieve(String methodName, long executeTime, long retrieveTime, int count) {
        SqlLogContext context = new SqlLogContext();
        context.setMethodName(methodName);
        context.setExecuteTime(executeTime);
        context.setRetrieveTime(retrieveTime);
        context.setCount(count);
        return LogUtil.formatMessage(endRetrieveLogItems, context);
    }
    
    /**
     * SqlPStatement#executeQueryメソッドの検索開始時のSQLログをフォーマットする。
     * @param methodName メソッド名 メソッド名
     * @param sql SQL文
     * @param additionalInfo 付加情報
     * @return フォーマット済みメッセージ
     */
    public String startExecuteQuery(String methodName, String sql, String additionalInfo) {
        SqlLogContext context = new SqlLogContext();
        context.setMethodName(methodName);
        context.setSql(sql);
        context.setAdditionalInfo(additionalInfo);
        return LogUtil.formatMessage(startExecuteQueryLogItems, context);
    }
    
    /**
     * SqlPStatement#executeQueryメソッドの検索終了時のSQLログをフォーマットする。
     * @param methodName メソッド名 メソッド名
     * @param executeTime 実行時間
     * @return フォーマット済みメッセージ
     */
    public String endExecuteQuery(String methodName, long executeTime) {
        SqlLogContext context = new SqlLogContext();
        context.setMethodName(methodName);
        context.setExecuteTime(executeTime);
        return LogUtil.formatMessage(endExecuteQueryLogItems, context);
    }
    
    /**
     * SqlPStatement#executeUpdateメソッドの更新開始時のSQLログをフォーマットする。
     * @param methodName メソッド名 メソッド名
     * @param sql SQL文
     * @param additionalInfo 付加情報
     * @return フォーマット済みメッセージ
     */
    public String startExecuteUpdate(String methodName, String sql, String additionalInfo) {
        SqlLogContext context = new SqlLogContext();
        context.setMethodName(methodName);
        context.setSql(sql);
        context.setAdditionalInfo(additionalInfo);
        return LogUtil.formatMessage(startExecuteUpdateLogItems, context);
    }
    
    /**
     * SqlPStatement#executeUpdateメソッドの更新終了時のSQLログをフォーマットする。
     * @param methodName メソッド名 メソッド名
     * @param executeTime 実行時間
     * @param updateCount 更新件数
     * @return フォーマット済みメッセージ
     */
    public String endExecuteUpdate(String methodName, long executeTime, int updateCount) {
        SqlLogContext context = new SqlLogContext();
        context.setMethodName(methodName);
        context.setExecuteTime(executeTime);
        context.setUpdateCount(updateCount);
        return LogUtil.formatMessage(endExecuteUpdateLogItems, context);
    }
    
    /**
     * SqlPStatement#executeメソッドの実行開始時のSQLログをフォーマットする。
     * @param methodName メソッド名 メソッド名
     * @param sql SQL文
     * @param additionalInfo 付加情報
     * @return フォーマット済みメッセージ
     */
    public String startExecute(String methodName, String sql, String additionalInfo) {
        SqlLogContext context = new SqlLogContext();
        context.setMethodName(methodName);
        context.setSql(sql);
        context.setAdditionalInfo(additionalInfo);
        return LogUtil.formatMessage(startExecuteLogItems, context);
    }
    
    /**
     * SqlPStatement#executeメソッドの実行終了時のSQLログをフォーマットする。
     * @param methodName メソッド名 メソッド名
     * @param executeTime 実行時間
     * @return フォーマット済みメッセージ
     */
    public String endExecute(String methodName, long executeTime) {
        SqlLogContext context = new SqlLogContext();
        context.setMethodName(methodName);
        context.setExecuteTime(executeTime);
        return LogUtil.formatMessage(endExecuteLogItems, context);
    }
    
    /**
     * SqlPStatement#executeBatchメソッドの更新開始時のSQLログをフォーマットする。
     * @param methodName メソッド名
     * @param sql SQL文
     * @param additionalInfo 付加情報
     * @return フォーマット済みメッセージ
     */
    public String startExecuteBatch(String methodName, String sql, String additionalInfo) {
        SqlLogContext context = new SqlLogContext();
        context.setMethodName(methodName);
        context.setSql(sql);
        context.setAdditionalInfo(additionalInfo);
        return LogUtil.formatMessage(startExecuteBatchLogItems, context);
    }
    
    /**
     * SqlPStatement#executeBatchメソッドの更新終了時のSQLログをフォーマットする。
     * @param methodName メソッド名
     * @param executeTime 実行時間
     * @param batchCount バッチ件数
     * @return フォーマット済みメッセージ
     */
    public String endExecuteBatch(String methodName, long executeTime, int batchCount) {
        SqlLogContext context = new SqlLogContext();
        context.setMethodName(methodName);
        context.setExecuteTime(executeTime);
        context.setBatchCount(batchCount);
        return LogUtil.formatMessage(endExecuteBatchLogItems, context);
    }
    
    /**
     * SQLログのコンテキスト情報を保持するクラス。
     * @author Kiyohito Itoh
     */
    @Published(tag = "architect")
    public static class SqlLogContext {
        /** メソッド名 */
        private String methodName;
        /** SQL文 */
        private String sql;
        /** 取得開始位置 */
        private int startPosition;
        /** 取得最大件数 */
        private int size;
        /** タイムアウト時間 */
        private int queryTimeout;
        /** フェッチ件数 */
        private int fetchSize;
        /** 実行時間 */
        private long executeTime;
        /** データ取得時間 */
        private long retrieveTime;
        /** 検索件数 */
        private int count;
        /** 更新件数 */
        private int updateCount;
        /** バッチ件数 */
        private int batchCount;
        /** 付加情報 */
        private String additionalInfo;
        /**
         * メソッド名を取得する。
         * @return メソッド名
         */
        public String getMethodName() {
            return methodName;
        }
        /**
         * メソッド名を設定する。
         * @param methodName メソッド名 
         */
        public void setMethodName(String methodName) {
            this.methodName = methodName;
        }
        /**
         * SQL文を取得する。
         * @return SQL文
         */
        public String getSql() {
            return sql;
        }
        /**
         * SQL文を設定する。
         * @param sql SQL文
         */
        public void setSql(String sql) {
            this.sql = sql;
        }
        /**
         * 取得開始位置を取得する。
         * @return 取得開始位置
         */
        public int getStartPosition() {
            return startPosition;
        }
        /**
         * 取得開始位置を設定する。
         * @param startPosition 取得開始位置
         */
        public void setStartPosition(int startPosition) {
            this.startPosition = startPosition;
        }
        /**
         * 取得最大件数を取得する。
         * @return 取得最大件数
         */
        public int getSize() {
            return size;
        }
        /**
         * 取得最大件数を設定する。
         * @param size 取得最大件数
         */
        public void setSize(int size) {
            this.size = size;
        }
        /**
         * タイムアウト時間を取得する。
         * @return タイムアウト時間
         */
        public int getQueryTimeout() {
            return queryTimeout;
        }
        /**
         * タイムアウト時間を設定する。
         * @param queryTimeout タイムアウト時間
         */
        public void setQueryTimeout(int queryTimeout) {
            this.queryTimeout = queryTimeout;
        }
        /**
         * フェッチ件数を取得する。
         * @return フェッチ件数
         */
        public int getFetchSize() {
            return fetchSize;
        }
        /**
         * フェッチ件数を設定する。
         * @param fetchSize フェッチ件数
         */
        public void setFetchSize(int fetchSize) {
            this.fetchSize = fetchSize;
        }
        /**
         * 実行時間を取得する。
         * @return 実行時間
         */
        public long getExecuteTime() {
            return executeTime;
        }
        /**
         * 実行時間を設定する。
         * @param executeTime 実行時間
         */
        public void setExecuteTime(long executeTime) {
            this.executeTime = executeTime;
        }
        /**
         * データ取得時間を取得する。
         * @return データ取得時間
         */
        public long getRetrieveTime() {
            return retrieveTime;
        }
        /**
         * データ取得時間を設定する。
         * @param retrieveTime データ取得時間
         */
        public void setRetrieveTime(long retrieveTime) {
            this.retrieveTime = retrieveTime;
        }
        /**
         * 検索件数を取得する。
         * @return 検索件数
         */
        public int getCount() {
            return count;
        }
        /**
         * 検索件数を設定する。
         * @param count 検索件数
         */
        public void setCount(int count) {
            this.count = count;
        }
        /**
         * 更新件数を取得する。
         * @return 更新件数
         */
        public int getUpdateCount() {
            return updateCount;
        }
        /**
         * 更新件数を設定する。
         * @param updateCount 更新件数
         */
        public void setUpdateCount(int updateCount) {
            this.updateCount = updateCount;
        }
        /**
         * バッチ件数を取得する。
         * @return バッチ件数
         */
        public int getBatchCount() {
            return batchCount;
        }
        /**
         * バッチ件数を設定する。
         * @param batchCount バッチ件数
         */
        public void setBatchCount(int batchCount) {
            this.batchCount = batchCount;
        }
        /**
         * 付加情報を取得する。
         * @return 付加情報
         */
        public String getAdditionalInfo() {
            return additionalInfo;
        }
        /**
         * 付加情報を設定する。
         * @param additionalInfo 付加情報 
         */
        public void setAdditionalInfo(String additionalInfo) {
            this.additionalInfo = additionalInfo;
        }
    }
    /**
     * メソッド名を取得するクラス。
     * @author Kiyohito Itoh
     */
    public static class MethodNameItem implements LogItem<SqlLogContext> {
        /**
         * メソッド名を取得する。
         * @param context {@link SqlLogContext}
         * @return メソッド名
         */
        public String get(SqlLogContext context) { return context.getMethodName(); }
    }
    /**
     * SQL文を取得するクラス。
     * @author Kiyohito Itoh
     */
    public static class SqlItem implements LogItem<SqlLogContext> {
        /**
         * SQL文を取得する。
         * @param context {@link SqlLogContext}
         * @return SQL文
         */
        public String get(SqlLogContext context) { return context.getSql(); }
    }
    /**
     * 取得開始位置を取得するクラス。
     * @author Kiyohito Itoh
     */
    public static class StartPositionItem implements LogItem<SqlLogContext> {
        /**
         * 取得開始位置を取得する。
         * @param context {@link SqlLogContext}
         * @return 取得開始位置
         */
        public String get(SqlLogContext context) { return String.valueOf(context.getStartPosition()); }
    }
    /**
     * 最大取得件数を取得するクラス。
     * @author Kiyohito Itoh
     */
    public static class SizeItem implements LogItem<SqlLogContext> {
        /**
         * 最大取得件数を取得する。
         * @param context {@link SqlLogContext}
         * @return 最大取得件数
         */
        public String get(SqlLogContext context) { return String.valueOf(context.getSize()); }
    }
    /**
     * タイムアウト時間を取得するクラス。
     * @author Kiyohito Itoh
     */
    public static class QueryTimeoutItem implements LogItem<SqlLogContext> {
        /**
         * タイムアウト時間を取得する。
         * @param context {@link SqlLogContext}
         * @return タイムアウト時間
         */
        public String get(SqlLogContext context) { return String.valueOf(context.getQueryTimeout()); }
    }
    /**
     * フェッチ件数を取得するクラス。
     * @author Kiyohito Itoh
     */
    public static class FetchSizeItem implements LogItem<SqlLogContext> {
        /**
         * フェッチ件数を取得する。
         * @param context {@link SqlLogContext}
         * @return フェッチ件数
         */
        public String get(SqlLogContext context) { return String.valueOf(context.getFetchSize()); }
    }
    /**
     * 実行時間を取得するクラス。
     * @author Kiyohito Itoh
     */
    public static class ExecuteTimeItem implements LogItem<SqlLogContext> {
        /**
         * 実行時間を取得する。
         * @param context {@link SqlLogContext}
         * @return 実行時間
         */
        public String get(SqlLogContext context) { return String.valueOf(context.getExecuteTime()); }
    }
    /**
     * データ取得時間を取得するクラス。
     * @author Kiyohito Itoh
     */
    public static class RetrieveTimeItem implements LogItem<SqlLogContext> {
        /**
         * データ取得時間を取得する。
         * @param context {@link SqlLogContext}
         * @return データ取得時間
         */
        public String get(SqlLogContext context) { return String.valueOf(context.getRetrieveTime()); }
    }
    /**
     * 検索件数を取得するクラス。
     * @author Kiyohito Itoh
     */
    public static class CountItem implements LogItem<SqlLogContext> {
        /**
         * 検索件数を取得する。
         * @param context {@link SqlLogContext}
         * @return 検索件数
         */
        public String get(SqlLogContext context) { return String.valueOf(context.getCount()); }
    }
    /**
     * 更新件数を取得するクラス。
     * @author Kiyohito Itoh
     */
    public static class UpdateCountItem implements LogItem<SqlLogContext> {
        /**
         * 更新件数を取得する。
         * @param context {@link SqlLogContext}
         * @return 更新件数
         */
        public String get(SqlLogContext context) { return String.valueOf(context.getUpdateCount()); }
    }
    /**
     * バッチ件数を取得するクラス。
     * @author Kiyohito Itoh
     */
    public static class BatchCountItem implements LogItem<SqlLogContext> {
        /**
         * バッチ件数を取得する。
         * @param context {@link SqlLogContext}
         * @return バッチ件数
         */
        public String get(SqlLogContext context) { return String.valueOf(context.getBatchCount()); }
    }
    /**
     * 付加情報を取得するクラス。
     * @author Kiyohito Itoh
     */
    public static class AdditionalInfoItem implements LogItem<SqlLogContext> {
        /**
         * 付加情報を取得する。
         * @param context {@link SqlLogContext}
         * @return 付加情報
         */
        public String get(SqlLogContext context) {
            String item = context.getAdditionalInfo();
            return StringUtil.isNullOrEmpty(item) ? "" : item;
        }
    }
}
