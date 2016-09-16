package nablarch.core.db.statement;

import nablarch.core.log.Logger;
import nablarch.core.util.StringUtil;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * ステートメントにバインドするパラメータ名とその値を保持するクラス。
 * 本クラスで保持された値は、ログ出力、クエリ結果のキャッシュに使用される。
 *
 * @author T.Kawasaki
 */
public class ParameterHolder {

    /** 実際のパラメータを保持するMap */
    private final Map<String, ParamValue> params
            = new TreeMap<String, ParamValue>();  // ログ出力時にソートするためTreeMap

    /**
     * {@link InputStream}のパラメータを追加する。
     *
     * @param name    パラメータ名
     * @param notUsed 使用しない
     */
    public void add(String name, InputStream notUsed) {
        add(name, new InputStreamValue());
    }

    /**
     * バイト配列のパラメータを追加する。
     *
     * @param name  パラメータ名
     * @param value パラメータ値
     */
    public void add(String name, byte[] value) {
        add(name, new BytesValue(value));
    }

    /**
     * パラメータ値を追加する。
     * {@link InputStream}、byte配列以外を付加する場合は本メソッドを使用する。
     *
     * @param name  パラメータ名
     * @param value パラメータ値
     */
    public void add(String name, Object value) {
        params.put(name, new ObjectValue(value));
    }

    /**
     * {@link InputStream}のパラメータを追加する。
     *
     * @param index パラメータインデックス
     * @param in    パラメータ値
     */
    public void add(int index, InputStream in) {
        add(indexToName(index), in);
    }

    /**
     * バイト配列のパラメータを追加する。
     *
     * @param index パラメータインデックス
     * @param value パラメータ値
     */
    public void add(int index, byte[] value) {
        add(indexToName(index), value);
    }

    /**
     * パラメータ値を追加する。
     * {@link InputStream}、byte配列以外を付加する場合は本メソッドを使用する。
     *
     * @param index パラメータインデックス
     * @param value パラメータ値
     */
    public void add(int index, Object value) {
        add(indexToName(index), value);
    }

    /**
     * パラメータインデックスをパラメータ名に変換する。
     *
     * @param index 変換元となるパラメータインデックス
     * @return パラメータ名
     */
    private String indexToName(int index) {
        return String.format("%02d", index);
    }

    /**
     * これまで本インスタンスに設定されたパラメータを取得する。
     *
     * @return パラメータ
     */
    public Map<String, ParamValue> getParameters() {
        return params;
    }

    /** 文字列変換時に使用するタブ文字 */
    private static final String TAB = "\t";

    /** 文字列変換時に使用するタブ（バッチ用：インデントが深い） */
    private static final String BATCH_TAB = "\t\t";

    /**
     * 与えられた{@link StringBuilder}に、全パラメータの文字列表現を付加する。（破壊的メソッド）
     * インデントにはタブ１文字が使用される。
     *
     * @param appended 文字列が付与される{@link StringBuilder}
     */
    void appendParameters(StringBuilder appended) {
        Formatter formatter = new Formatter(params, TAB);
        formatter.appendFormattedParameters(appended);
    }

    /**
     * 与えられた{@link StringBuilder}に、全パラメータの文字列表現を付加する（バッチ用）。（破壊的メソッド）
     * インデントにはタブ２文字が使用される。
     *
     * @param appended 文字列が付与される{@link StringBuilder}
     */
    void appendParametersForBatch(StringBuilder appended) {
        Formatter formatter = new Formatter(params, BATCH_TAB);
        formatter.appendFormattedParameters(appended);
    }


    /** {@inheritDoc} */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        appendParameters(sb);
        return sb.toString();
    }

    /**
     * バインドされるパラメータ値を表すインタフェース。
     * 本インタフェースはメソッドを定義しない。
     * 実装クラスを型安全に扱うためのインタフェースである。
     * （Mapのvalueの型として使用する）
     *
     * 本クラスのインスタンスは、以下の機能を実装する。
     * <ul>
     *     <li>格納したオブジェクトが等価であるかどうかの判定(equals, hashCode)</li>
     *     <li>格納したオブジェクトの文字列表現(toString)</li>
     * </ul>
     */
    public interface ParamValue {
    }

    /**
     * InputStreamのパラメータ値。
     * InputStreamの場合は、内容を読み取らないと等価判定ができないため
     * 等値である（同一インスタンスである）場合以外、等価と判定しない。
     * よって、本クラスでは{link #equals(Object)}メソッドをオーバライドしない。
     * （{@link Object#equals(Object)}の動作）
     */
    static class InputStreamValue implements ParamValue {

        /**
         * {@inheritDoc}
         * InputStreamは内容を出力できないので代替文字列を返却する。
         */
        @Override
        public String toString() {
            return "InputStream";
        }
    }

    /**
     * バイト配列のパラメータ値。
     */
    static final class BytesValue implements ParamValue {

        /** パラメータ値 */
        private final byte[] bytes;

        /**
         * コンストラクタ。
         *
         * @param bytes バインドされたバイト配列
         */
        BytesValue(byte[] bytes) {
            this.bytes = bytes;
        }

        /**
         * {@inheritDoc}
         * バイト配列として等価である場合、真を返却する。
         *
         * @see Arrays#equals(byte[], byte[])
         */
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof BytesValue)) {
                return false;
            }

            BytesValue other = (BytesValue) o;
            return Arrays.equals(bytes, other.bytes);
        }

        /** {@inheritDoc} */
        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }

        /**
         * {@inheritDoc}
         * byte配列は、文字列表現に適していないため、代替文字列を返却する。
         */
        @Override
        public String toString() {
            return "bytes";
        }

    }

    /**
     * byte[], InputStream以外のパラメータ値。
     */
    static final class ObjectValue implements ParamValue {

        /** パラメータ値 */
        private final Object obj;

        /**
         * コンストラクタ。
         * @param obj パラメータ値
         */
        ObjectValue(Object obj) {
            this.obj = obj;
        }

        /**
         * {@inheritDoc}
         * 保持しているパラメータ値同士が等価である場合、等価と判定する。
         */
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ObjectValue)) {
                return false;
            }
            ObjectValue that = (ObjectValue) o;
            if (obj == null) {
                return that.obj == null;
            }
            return obj.equals(that.obj);
        }

        /** {@inheritDoc} */
        @Override
        public int hashCode() {
            return obj != null ? obj.hashCode() : 0;
        }

        /** {@inheritDoc} */
        @Override
        public String toString() {
            return obj == null ? "null" : StringUtil.toString(obj);
        }
    }

    /**
     * パラメータを整形するクラス。
     * ここで整形された文字列はSQLログ出力に使用される。
     */
    private static final class Formatter {

        /** 出力対象となるパラメータ */
        private final Map<String, ParamValue> params;

        /** 整形に使用するタブ文字 */
        private final String tab;

        /**
         * コンストラクタ。
         *
         * @param params 出力対象となるパラメータ
         * @param tab    整形に使用するタブ文字
         */
        private Formatter(Map<String, ParamValue> params, String tab) {
            this.params = params;
            this.tab = tab;
        }

        /**
         * パラメータを整形し、その結果を引数で与えられた{@link StringBuilder}に付与する（破壊的メソッド）。
         *
         * @param appended 文字列が付与される{@link StringBuilder}
         */
        private void appendFormattedParameters(StringBuilder appended) {
            for (Entry<String, ParamValue> entry : params.entrySet()) {
                String name = entry.getKey();
                ParamValue value = entry.getValue();
                appended.append(Logger.LS)
                        .append(tab).append(name).append(" = [").append(value).append("]");
            }
        }
    }



    /**
     * 何も実行しない{@link ParameterHolder}実装クラス。
     */
    static final class NopParameterHolder extends ParameterHolder {

        /** インスタンス */
        private static final NopParameterHolder SOLO_INSTANCE = new NopParameterHolder();

        /**
         * インスタンスを取得する。
         * @return インスタンス
         */
        static NopParameterHolder getInstance() {
            return SOLO_INSTANCE;
        }

        /** プライベートコンストラクタ */
        private NopParameterHolder() {
        }

        /** {@inheritDoc} */
        @Override
        public void add(String name, Object value) {
        }

        /** {@inheritDoc} */
        @Override
        public void add(int index, Object value) {
        }
    }
}
