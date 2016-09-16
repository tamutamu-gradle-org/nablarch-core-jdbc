package nablarch.core.db.cache;

import nablarch.core.db.cache.statement.BoundParameters;
import nablarch.core.util.annotation.Published;

/**
 * 結果セットをキャッシュに格納する際のキーとなるクラス。
 * 以下の項目が等しい場合に等価と判定する。
 * <ul>
 * <li>SQL ID</li>
 * <li>バインドパラメータ</li>
 * <li>開始位置</li>
 * <li>最大件数</li>
 * </ul>
 * <p/>
 * 本クラスはイミュータブルである。
 *
 * @author T.Kawasaki
 */
@Published(tag = "architect")    // ResultSetCache#remove(Object)を呼ぶ際のキー
public final class ResultSetCacheKey {

    /** SQL ID */
    private final String sqlId;

    /** バインドパラメータ */
    private final BoundParameters params;

    /** 開始位置 （通常は1） */
    private final int startPos;

    /** 最大件数（通常は0） */
    private final int max;

    /**
     * ハッシュコード。
     * 本クラスはイミュータブルであるため、ハッシュコードは変更されない。
     */
    private final int hashCode;


    /**
     * フルコンストラクタ。
     *
     * @param sqlId    SQL ID
     * @param params   バインドパラメータ
     * @param startPos 開始位置
     * @param max      最大件数
     */
    public ResultSetCacheKey(String sqlId, BoundParameters params, int startPos, int max) {
        this.sqlId = sqlId;
        this.params = params;
        this.startPos = startPos;
        this.max = max;
        this.hashCode = calcHashCode();  // インスタンス生成時にハッシュコードを計算して保持しておく。
    }

    /**
     * {@inheritDoc}
     * 以下の項目が等しい場合に等価と判定する。
     * <ul>
     * <li>SQL ID</li>
     * <li>バインドパラメータ</li>
     * <li>開始位置</li>
     * <li>最大件数</li>
     * </ul>
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ResultSetCacheKey that = (ResultSetCacheKey) o;
        return sqlId.equals(that.sqlId)
                && params.equals(that.params)
                && startPos == that.startPos
                && max == that.max;
    }

    /**
     * 本インスタンスのハッシュコードを計算する。
     *
     * @return ハッシュコード
     */
    private int calcHashCode() {
        int result = sqlId.hashCode();
        result = 31 * result + params.hashCode();
        result = 31 * result + startPos;
        result = 31 * result + max;
        return result;
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return hashCode;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return "sqlId='" + sqlId + '\''
                + ", params=" + params
                + ", startPos=" + startPos
                + ", max=" + max
                + '}';
    }
}
