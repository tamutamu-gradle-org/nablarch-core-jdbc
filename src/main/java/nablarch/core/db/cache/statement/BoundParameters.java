package nablarch.core.db.cache.statement;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import nablarch.core.db.statement.ParameterHolder;
import nablarch.core.db.statement.ParameterHolder.ParamValue;

/**
 * ステートメント発行時にバインドされたパラメータを表すクラス。
 * {@link nablarch.core.db.cache.ResultSetCacheKey}の等価性判定に使用される。
 *
 * @author T.Kawasaki
 */
public class BoundParameters {

    /** パラメータ */
    private final Map<String, ParamValue> params;

    /** ハッシュコード */
    private final int hashCode;

    /**
     * コンストラクタ。
     *
     * @param holder パラメータ保持クラス
     */
    public BoundParameters(ParameterHolder holder) {
        this(holder.getParameters());
    }

    /**
     * コンストラクタ。
     *
     * @param original 元となるパラメータ
     */
    public BoundParameters(Map<String, ParamValue> original) {
        Map<String, ParamValue> copied = new HashMap<String, ParamValue>(original);
        this.params = Collections.unmodifiableMap(copied);
        this.hashCode = copied.hashCode();
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (!(o instanceof BoundParameters)) {
            return false;
        }
        BoundParameters other = (BoundParameters) o;
        return params.equals(other.params);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return hashCode;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return params.toString();
    }
}
