package nablarch.core.db.support;

import nablarch.core.util.annotation.Published;

/**
 * 一覧検索において検索結果件数が検索結果の最大件数(上限)を超えた場合に発生する例外。
 * <p/>
 * 検索結果の最大件数(上限)と検索結果件数を保持する。<br/>
 * 検索結果の最大件数(上限)の設定については、{@link nablarch.core.db.support.ListSearchInfo#ListSearchInfo()}を参照。
 *
 * @author Kiyohito Itoh
 */
@Published
public class TooManyResultException extends RuntimeException {
    
    /** 検索結果の最大件数(上限) */
    private int maxResultCount;
    
    /** 検索結果件数 */
    private int resultCount;
    
    /**
     * 検索結果の最大件数(上限)及び検索結果件数を保持した{@code TooManyResultException}オブジェクトを生成する。
     * <p/>
     * 生成時に検索結果の最大件数(上限)及び検索結果件数を元に、メッセージを構築する。
     *
     * @param max 検索結果の最大件数(上限)
     * @param resultCount 検索結果件数
     */
    public TooManyResultException(int max, int resultCount) {
        super(String.format("exceeded the max result count. max result count = [%s], search results = [%s]",
                             max, resultCount));
        this.maxResultCount = max;
        this.resultCount = resultCount;
    }

    /**
     * 検索結果の最大件数(上限)を取得する。
     * @return 検索結果の最大件数(上限)
     */
    @Published
    public int getMaxResultCount() {
        return maxResultCount;
    }

    /**
     * 検索結果件数を取得する。
     * @return 検索結果件数
     */
    @Published
    public int getResultCount() {
        return resultCount;
    }
        
}
