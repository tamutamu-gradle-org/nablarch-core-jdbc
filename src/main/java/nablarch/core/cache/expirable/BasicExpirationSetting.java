package nablarch.core.cache.expirable;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import nablarch.core.date.SystemTimeProvider;

import static java.util.Collections.unmodifiableMap;

/**
 * {@link ExpirationSetting}の基本実装クラス。
 * IDと有効期限の紐付けをMapで保持する。
 *
 * @author T.Kawasaki
 */
public class BasicExpirationSetting implements ExpirationSetting {

    /** 有効期限のパターン（数字＋アルファベット） */
    private static final Pattern PTN = Pattern.compile("(\\d+)(\\p{Alpha}+)");

    /** デフォルトの時間単位 */
    private static final Map<String, TimeUnit> DEFAULT_TIME_UNIT_MAPPING = unmodifiableMap(
            new HashMap<String, TimeUnit>() {
                {
                    // 文字列表現, 時間単位
                    put("ms", TimeUnit.MILLISECOND);
                    put("sec", TimeUnit.SECOND);
                    put("min", TimeUnit.MINUTE);
                    put("h", TimeUnit.HOUR);
                }
            });

    /** システム日時提供クラス */
    private SystemTimeProvider systemTimeProvider;

    /**
     * IDと有効期限のマッピング。
     * （例: "ID001", "100ms"）
     */
    private final Map<String, TimeoutExpression> expirationSetting = new HashMap<String, TimeoutExpression>();;


    /** {@inheritDoc} */
    @Override
    public boolean isCacheEnable(String id) {
        return expirationSetting.keySet().contains(id);
    }

    /** {@inheritDoc} */
    @Override
    public Date getExpiredDate(String id) {
        TimeoutExpression timeout = expirationSetting.get(id);
        if (timeout == null) {
            throw new IllegalArgumentException(
                    "specified id is not registered. [" + id + "]"
                            + " registerd items are " + expirationSetting
            );
        }
        return timeout.evaluate();
    }

    /**
     * システム日時提供クラスを設定する（必須）。
     * 本メソッドはDIコンテナから使用されることを想定している。
     *
     * @param systemTimeProvider システム日時提供クラス
     */
    public void setSystemTimeProvider(SystemTimeProvider systemTimeProvider) {
        this.systemTimeProvider = systemTimeProvider;
    }


    /**
     * 有効期限設定を設定する（必須）。
     *
     *
     * 本メソッドはDIコンテナから使用されることを想定している。
     *
     * @param expirationList 有効期限設定のリスト
     */
    public void setExpirationList(List<Map<String, String>> expirationList) {
        expirationSetting.clear();
        for (Map<String, String> setting : expirationList) {
            setExpiration(setting);
        }
    }

    /**
     * 有効期限設定を設定する（必須）。
     *
     *
     * 本メソッドはDIコンテナから使用されることを想定している。
     *
     * @param expiration 有効期限設定のリスト
     */
    public void setExpiration(Map<String, String> expiration) {
        for (Entry<String, String> entry : expiration.entrySet()) {
            String id = entry.getKey();
            TimeoutExpression to = new TimeoutExpression(entry.getValue());
            expirationSetting.put(id, to);
        }

    }

    /**
     * 時間単位のマッピングを取得する。
     * 本メソッドをオーバーライドすることで、
     * マッピングを変更することができる。
     *
     * @return マッピング
     */
    protected Map<String, TimeUnit> getTimeUnitMapping() {
        return DEFAULT_TIME_UNIT_MAPPING;
    }

    /**
     * 有効期限の評価を行う。
     * 文字列で表された有効期限を評価し、システム日時からの差分として返却する。
     * 例えば、"30s"という文字列表現が与えられた場合、システム日時に30秒付加した
     * 日時が返却される。
     *
     * @param timeoutExpression 有効期限の文字列表現
     * @return 有効期限
     */
    Date evaluate(String timeoutExpression) {
        TimeoutExpression timeout = new TimeoutExpression(timeoutExpression);
        return timeout.evaluate();
    }

    /**
     * 有効期限の文字列表現を表すクラス。
     */
    private final class TimeoutExpression {

        /** 量 (例:30, 1000等) */
        private final int amount;

        /** 単位 (例: ms, min, h等) */
        private final String unit;

        /**
         * 有効期限の文字列表現を評価する
         *
         * @return 有効期限
         */
        private Date evaluate() {
            Calendar timeout = getSystemTime();
            addTime(timeout);
            return timeout.getTime();
        }

        /**
         * 与えられた日時に、本インスタンスが評価した時間を付加する（破壊的メソッド）。
         * 例えば、本インスタンスの評価値が60秒である場合、
         * 引数で与えられた日時に60秒が付加される。
         *
         * @param target 付加される対象となる日時
         */
        private void addTime(Calendar target) {
            TimeUnit timeUnit = getTimeUnit(unit);
            timeUnit.add(target, amount);
        }

        /**
         * コンストラクタ
         *
         * @param expression 有効期限の文字列表現
         */
        private TimeoutExpression(String expression) {
            Matcher m = PTN.matcher(expression);
            try {
                if (!m.find()) {
                    throw new IllegalArgumentException(buildErrMsg(expression));
                }
                String amt = m.group(1);
                unit = m.group(2);
                amount = Integer.parseInt(amt);
            } catch (RuntimeException e) {
                throw new IllegalArgumentException(buildErrMsg(expression), e);
            }
        }

        /**
         * エラーメッセージを組み立てる。
         *
         * @param expression エラー発生時の文字列表現
         * @return エラーメッセージ
         */
        private String buildErrMsg(String expression) {
            return "invalid timeout expression. [" + expression + "]";
        }

        /**
         * システム日時を取得する。
         *
         * @return システム日時
         */
        private Calendar getSystemTime() {
            if (systemTimeProvider == null) {
                throw new IllegalStateException("systemTimeProvider must be set.");
            }
            Calendar now = Calendar.getInstance();
            now.setTime(systemTimeProvider.getDate());
            return now;
        }

        /**
         * 時間単位の文字列表現から時間の単位を取得する。
         * 例えば、"min"が与えられた場合、{@link TimeUnit#MINUTE}が返却される。
         *
         * @param expression 時間単位の文字列表現
         * @return 時間単位
         */
        private TimeUnit getTimeUnit(String expression) {
            TimeUnit unit = getTimeUnitMapping().get(expression);
            if (unit == null) {
                throw new IllegalArgumentException("unknown TimeUnit expression.[" + expression + "]");
            }
            return unit;
        }
    }

    /**
     * 時間の単位を表す列挙型。
     */
    static enum TimeUnit {
        /** ミリ秒 */
        MILLISECOND(Calendar.MILLISECOND),
        /** 秒 */
        SECOND(Calendar.SECOND),
        /** 分 */
        MINUTE(Calendar.MINUTE),
        /** 時間 */
        HOUR(Calendar.HOUR),
        /** 日 */
        DAY(Calendar.DATE);

        /** 列挙型の内部値 */
        private final int internal;

        /**
         * プライベートコンストラクタ。
         *
         * @param internal 内部値
         */
        private TimeUnit(int internal) {
            this.internal = internal;
        }

        /**
         * 与えられたカレンダーに対して、指定された量だけ時間を加算する（破壊的メソッド）。
         * 例えば、
         * <pre>
         * TimeUnit.MILLISECOND.add(calendar, 100);
         * </pre>
         * は与えられたカレンダーに100ミリ秒が加算される。
         *
         * @param calendar 元となるカレンダ（変更される）
         * @param amount   加算する量
         */
        private void add(Calendar calendar, int amount) {
            calendar.add(internal, amount);
        }
    }
}
