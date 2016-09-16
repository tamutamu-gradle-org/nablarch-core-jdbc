package nablarch.core.db.statement;

import nablarch.core.util.annotation.Published;

/**
 * 検索処理のオプションを保持するクラス。
 *
 * @author Hisaaki Sioiri
 */
@Published(tag = "architect")
public class SelectOption {

    /** 検索処理の取得開始レコード番号(0から開始) */
    private final int offset;

    /** 検索処理のリミット(オフセットから何レコード取得するか) */
    private final int limit;

    /** 検索処理の取得開始レコード番号(1から開始) */
    private final int startPosition;

    /**
     * 検索オプションを生成する。
     *
     * @param startPosition 検索開始レコード番号(1から開始)
     * @param limit 取得するレコード数
     */
    public SelectOption(int startPosition, int limit) {
        this.offset = startPosition - 1;
        this.limit = limit;
        this.startPosition = startPosition;
    }

    /**
     * 検索処理の取得開始レコード番号(0から開始)を返す。
     *
     * @return 取得開始レコード番号
     */
    public int getOffset() {
        return offset;
    }

    /**
     * 検索処理の取得するレコード数を返す。
     *
     * @return 取得するレコード数
     */
    public int getLimit() {
        return limit;
    }

    /**
     * 検索処理の取得開始位置(1から開始)を返す。
     *
     * @return 取得開始レコード番号
     */
    public int getStartPosition() {
        return startPosition;
    }

    @Override
    public String toString() {
        return new StringBuilder(getClass().getName())
                      .append(":{offset=").append(offset)
                      .append(", limit=").append(limit).append("}").toString();
    }
}
