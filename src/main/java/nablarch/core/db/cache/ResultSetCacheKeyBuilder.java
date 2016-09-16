package nablarch.core.db.cache;

import nablarch.core.db.cache.statement.BoundParameters;
import nablarch.core.db.statement.ParameterHolder;
import nablarch.core.util.annotation.Published;

/**
 * {@link ResultSetCacheKey}を生成する際に使用するヘルパークラス。
 * {@link ResultSetCache#remove(Object)}を呼ぶ際のキー作成に使用する。
 *
 * @author T.Kawasaki
 */
@Published(tag = "architect")   // ResultSetCache#remove(Object)を呼ぶ際のキーを生成するため
public class ResultSetCacheKeyBuilder {

    /** SQL ID */
    private final String sqlId;

    /** バインドパラメータ保持クラス */
    private ParameterHolder paramBuilder = new ParameterHolder();

    /** 開始位置（デフォルト値: 1) */
    private int startPos = 1;

    /** 最大件数 (デフォルト値: 0) */
    private int max = 0;

    /**
     * コンストラクタ。
     * @param sqlId SQL ID（必須）
     */
    public ResultSetCacheKeyBuilder(String sqlId) {
        this.sqlId = sqlId;
    }

    /**
     * 設定された値を用いて{@link ResultSetCacheKey}インスタンスを生成する。
     *
     * @return {@link ResultSetCacheKey}
     */
    public ResultSetCacheKey build() {
        return new ResultSetCacheKey(
                sqlId,
                new BoundParameters(paramBuilder),
                startPos,
                max
        );
    }

    /**
     * 開始位置を設定する。
     * （省略時は1）
     *
     * @param startPos 開始位置
     * @return 本インスタンス
     */
    public ResultSetCacheKeyBuilder setStartPos(int startPos) {
        this.startPos = startPos;
        return this;
    }

    /**
     * 最大件数を設定する。
     * （省略時は0）
     *
     * @param max 最大件数
     * @return 本インスタンス
     */
    public ResultSetCacheKeyBuilder setMax(int max) {
        this.max = max;
        return this;
    }

    /**
     * パラメータを付加する。
     *
     * @param index インデックス
     * @param value 値
     * @return 本インスタンス
     */
    public ResultSetCacheKeyBuilder addParam(int index, byte[] value) {
        paramBuilder.add(index, value);
        return this;
    }

    /**
     * パラメータを付加する。
     *
     * @param index インデックス
     * @param value 値
     * @return 本インスタンス
     */
    public ResultSetCacheKeyBuilder addParam(int index, Object value) {
        paramBuilder.add(index, value);
        return this;
    }

    /**
     * パラメータを付加する。
     *
     * @param name パラメータ名
     * @param value 値
     * @return 本インスタンス
     */
    public ResultSetCacheKeyBuilder addParam(String name, Object value) {
        paramBuilder.add(name, value);
        return this;
    }

    /**
     * パラメータを付加する。
     *
     * @param name パラメータ名
     * @param value 値
     * @return 本インスタンス
     */
    public ResultSetCacheKeyBuilder addParam(String name, byte[] value) {
        paramBuilder.add(name, value);
        return this;
    }
}
