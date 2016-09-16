package nablarch.core.db.support;

import java.io.Serializable;

import nablarch.core.repository.SystemRepository;
import nablarch.core.util.StringUtil;
import nablarch.core.util.annotation.Published;

/**
 * 一覧検索用の情報を保持する基底クラス。
 * 
 * @author Kiyohito Itoh
 */
@Published
public abstract class ListSearchInfo implements Serializable {

    /** serialVersionUID */
    private static final long serialVersionUID = 2899442338576789285L;

    /** 取得対象のページ番号 */
    private Integer pageNumber;
    
    /** 1ページあたりの最大取得件数 */
    private Integer max;
    
    /** 検索結果の総件数 */
    private int resultCount;
    
    /** 総ページ数 */
    private int pageCount;
    
    /** 検索結果の最大件数(上限) */
    private int maxResultCount;
    
    /** ソートID */
    private String sortId;
    
    /**
     * {@link SystemRepository}の設定値を元に{@code ListSearchInfo}を生成する。
     * <p/>
     * <pre>
     * 下記の初期化処理を行う。
     * 
     * 検索結果の最大件数(上限)：
     *     リポジトリの設定値(nablarch.listSearch.maxResultCount)を取得して設定する。
     *     リポジトリの設定値が存在しない場合は、200を設定する。
     * 
     * 検索結果のページ番号：
     *     1を設定する。
     * 
     * 1ページあたりの最大取得件数：
     *     リポジトリの設定値(nablarch.listSearch.max)を取得して設定する。
     *     リポジトリの設定値が存在しない場合は、20を設定する。
     * </pre>
     */
    protected ListSearchInfo() {
        
        Integer maxResultCountValue = getConfigValue("nablarch.listSearch.maxResultCount");
        maxResultCount = maxResultCountValue != null ? maxResultCountValue : 200;
        
        pageNumber = 1;
        
        Integer maxValue = getConfigValue("nablarch.listSearch.max");
        max = maxValue != null ? maxValue : 20;
    }

    /**
     * {@link SystemRepository}から設定値を取得する。
     * @param name 設定名
     * @return 設定値。存在しない場合はnull
     */
    protected final Integer getConfigValue(String name) {
        String value = SystemRepository.getString(name);
        return StringUtil.isNullOrEmpty(value) ? null : Integer.valueOf(value);
    }
    
    /**
     * 検索条件のプロパティ名を取得する。
     * @return 検索条件のプロパティ名
     */
    public abstract String[] getSearchConditionProps();
    
    /**
     * 取得対象のページ番号を取得する。
     * @return 取得対象のページ番号
     */
    public final Integer getPageNumber() {
        return pageNumber;
    }

    /**
     * 取得対象のページ番号を設定する。
     * @param pageNumber 取得対象のページ番号
     */
    public void setPageNumber(Integer pageNumber) {
        this.pageNumber = pageNumber;
    }
    
    /**
     * 検索結果の取得開始位置を取得する。
     * @return 検索結果の取得開始位置
     */
    public final int getStartPosition() {
        return getPageNumber() == 1 ? 1 : ((getPageNumber() - 1) * getMax()) + 1;
    }

    /**
     * 検索結果の取得終了位置を取得する。
     * <pre>
     * 検索結果の総件数が現在のページ番号に対する最大取得終了位置に満たない場合は、
     * 検索結果の総件数を返す。
     * 
     * 検索結果の総件数が現在のページ番号に対する最大取得終了位置以上の場合は、
     * 現在のページ番号に対する最大取得終了位置を返す。
     * </pre>
     * @return 検索結果の取得終了位置
     */
    public final int getEndPosition() {

        // 現在のページ番号に対する最大取得終了位置の算出
        int maxEndPosition = getStartPosition() + getMax() - 1;

        return getResultCount() < maxEndPosition
            ? getResultCount() // 検索結果の総件数が現在のページ番号に対する最大取得終了位置に満たない場合
            : maxEndPosition;  // 検索結果の総件数が現在のページ番号に対する最大取得終了位置以上の場合
    }

    /**
     * 1ページあたりの最大取得件数を取得する。
     * @return 1ページあたりの最大取得件数
     */
    public final Integer getMax() {
        return max;
    }

    /**
     * 1ページあたりの最大取得件数を設定する。
     * @param max 1ページあたりの最大取得件数
     */
    public void setMax(Integer max) {
        this.max = max;
    }

    /**
     * 検索結果の総件数を取得する。
     * @return 検索結果の総件数
     */
    public final int getResultCount() {
        return resultCount;
    }
    
    /**
     * 検索結果の総件数を設定する。
     * @param resultCount 検索結果の総件数
     */
    public final void setResultCount(int resultCount) {
        this.resultCount = resultCount;
    }
    
    /**
     * 検索結果の最大件数(上限)を取得する。
     * @return 検索結果の最大件数(上限)
     */
    public final int getMaxResultCount() {
        return maxResultCount;
    }

    /**
     * 検索結果の最大件数(上限)を設定する。
     * @param maxResultCount 検索結果の最大件数(上限)
     */
    public void setMaxResultCount(int maxResultCount) {
        this.maxResultCount = maxResultCount;
    }

    /**
     * ソートIDを取得する。
     * @return ソートID
     */
    public final String getSortId() {
        return sortId;
    }

    /**
     * ソートIDを設定する。
     * @param sortId ソートID
     */
    public void setSortId(String sortId) {
        this.sortId = sortId;
    }

    /**
     * 前のページが存在するか否かを取得する。
     * @return 前のページが存在する場合は{@code true}
     */
    public final boolean getHasPrevPage() {
        return getPageNumber() > 1;
    }
    
    /**
     * 次のページが存在するか否かを取得する。
     * @return 次のページが存在する場合は{@code true}
     */
    public final boolean getHasNextPage() {
        return getPageNumber() < getPageCount();
    }

    /**
     * 総ページ数を取得する。
     * @return 総ページ数
     */
    public final int getPageCount() {
        pageCount = getResultCount() / getMax();
        if (getResultCount() - (pageCount * getMax()) > 0) {
            pageCount++;
        }
        return pageCount;
    }
    
    /**
     * 最初のページ番号を取得する。
     * @return 最初のページ番号
     */
    public final int getFirstPageNumber() {
        return 1;
    }
    
    /**
     * 前のページ番号を取得する。
     * @return 前のページ番号
     */
    public final int getPrevPageNumber() {
        return getPageNumber() - 1;
    }

    /**
     * 次のページ番号を取得する。
     * @return 次のページ番号
     */
    public final int getNextPageNumber() {
        return getPageNumber() + 1;
    }
    
    /**
     * 最終ページの番号を取得する。
     * @return 最終ページの番号
     */
    public final int getLastPageNumber() {
        return pageCount;
    }
}
