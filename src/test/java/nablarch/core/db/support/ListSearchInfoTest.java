package nablarch.core.db.support;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import nablarch.core.repository.ConfigFileLoader;
import nablarch.core.repository.SystemRepository;

import org.junit.Test;

/**
 * {@link ListSearchInfo}のテストクラス。
 * 
 * @author Kiyohito Itoh
 */
public class ListSearchInfoTest {
    
    /**
     * 初期化処理が正しく動作すること。
     */
    @Test
    public void testInit() {
        
        // リポジトリの設定値が存在しない場合
        ListSearchInfo cond = new ListSearchInfo() {
            public String[] getSearchConditionProps() { return null; }
        };
        assertThat(cond.getMaxResultCount(), is(200));
        assertThat(cond.getPageNumber(), is(1));
        assertThat(cond.getMax(), is(20));
        
        // リポジトリの設定値が存在する場合
        ConfigFileLoader loader = new ConfigFileLoader("classpath:nablarch/core/db/support/paging.config");
        SystemRepository.load(loader);
        cond = new ListSearchInfo() {
            public String[] getSearchConditionProps() { return null; }
        };
        assertThat(cond.getMaxResultCount(), is(1000));
        assertThat(cond.getPageNumber(), is(1));
        assertThat(cond.getMax(), is(100));
        SystemRepository.clear();
    }
    
    /**
     * ゲッタとセッタが正しく動作すること。
     */
    @Test
    public void testGetterSetter() {
        
        ListSearchInfo cond = new ListSearchInfo() {
            public String[] getSearchConditionProps() { return null; }
        };
        
        // 検索結果の最大件数(上限)を設定した場合
        cond.setMaxResultCount(300);
        assertThat(cond.getMaxResultCount(), is(300));
        
        // 検索結果の取得最大件数を設定した場合
        cond.setMax(30);
        assertThat(cond.getMax(), is(30));
        
        // 検索結果の取得開始ページを設定した場合(途中のページ)
        cond.setMax(50);
        cond.setPageNumber(3);
        assertThat(cond.getPageNumber(), is(3));
        assertThat("現在のページ番号が正しく取得できること。", cond.getPageNumber(), is(3));
        assertThat("検索結果の取得開始位置が正しく取得できること。", cond.getStartPosition(), is(101));
        
        // 検索結果の取得開始ページを設定した場合(最初のページ)
        cond.setPageNumber(1);
        assertThat(cond.getPageNumber(), is(1));
        assertThat("現在のページ番号が正しく取得できること。", cond.getPageNumber(), is(1));
        assertThat("検索結果の取得開始位置が正しく取得できること。", cond.getStartPosition(), is(1));
        
        // 検索結果の総件数を設定した場合(maxより多い)
        cond.setMax(50);
        cond.setResultCount(222);
        assertThat(cond.getResultCount(), is(222));
        assertThat("総ページ数が正しく取得できること。", cond.getPageCount(), is(5));

        // 検索結果の総件数を設定した場合(maxより少ない)
        cond.setResultCount(222);
        cond.setMax(300);
        assertThat(cond.getResultCount(), is(222));
        assertThat("総ページ数が正しく取得できること。", cond.getPageCount(), is(1));
        
        // 検索結果の総件数を設定した場合(検索結果が0)
        cond.setMax(50);
        cond.setResultCount(0);
        assertThat(cond.getResultCount(), is(0));
        assertThat("総ページ数が正しく取得できること。", cond.getPageCount(), is(0));
        
        // 検索結果の総件数を設定した場合(検索結果が1)
        cond.setMax(50);
        cond.setResultCount(1);
        assertThat(cond.getResultCount(), is(1));
        assertThat("総ページ数が正しく取得できること。", cond.getPageCount(), is(1));

        // ソートID
        cond.setSortId("userId");
        assertThat(cond.getSortId(), is("userId"));
    }
    
    /**
     * ページングが正しく動作すること。
     */
    @Test
    public void testPaging() {

        // max=20, maxResultCount=200, startPageNumber=1
        ListSearchInfo cond = new ListSearchInfo() {
            public String[] getSearchConditionProps() { return null; }
        };
        
        // 検索結果が100件の場合
        cond.setResultCount(100);
        assertThat(cond.getResultCount(), is(100));
        assertThat(cond.getPageNumber(), is(1));
        assertThat(cond.getPageCount(), is(5));
        assertThat(cond.getHasPrevPage(), is(false));
        assertThat(cond.getHasNextPage(), is(true));
        assertThat(cond.getNextPageNumber(), is(2));
        assertThat(cond.getLastPageNumber(), is(5));
        assertThat(cond.getStartPosition(), is(1));
        assertThat(cond.getEndPosition(), is(20));

        // 検索結果の取得開始ページを5に変更した場合(検索結果が100件のまま)
        cond.setResultCount(100);
        cond.setPageNumber(5);
        assertThat(cond.getResultCount(), is(100));
        assertThat(cond.getPageNumber(), is(5));
        assertThat(cond.getPageCount(), is(5));
        assertThat(cond.getHasPrevPage(), is(true));
        assertThat(cond.getHasNextPage(), is(false));
        assertThat(cond.getNextPageNumber(), is(6));
        assertThat(cond.getLastPageNumber(), is(5));
        assertThat(cond.getStartPosition(), is(81));
        assertThat(cond.getEndPosition(), is(100));

        // 検索結果が99件の場合
        cond.setResultCount(99);
        cond.setPageNumber(1);
        assertThat(cond.getResultCount(), is(99));
        assertThat(cond.getPageNumber(), is(1));
        assertThat(cond.getPageCount(), is(5));
        assertThat(cond.getHasPrevPage(), is(false));
        assertThat(cond.getHasNextPage(), is(true));
        assertThat(cond.getNextPageNumber(), is(2));
        assertThat(cond.getLastPageNumber(), is(5));
        assertThat(cond.getStartPosition(), is(1));
        assertThat(cond.getEndPosition(), is(20));

        // 検索結果の取得開始ページを5に変更した場合(検索結果が99件のまま)
        cond.setResultCount(99);
        cond.setPageNumber(5);
        assertThat(cond.getResultCount(), is(99));
        assertThat(cond.getPageNumber(), is(5));
        assertThat(cond.getPageCount(), is(5));
        assertThat(cond.getHasPrevPage(), is(true));
        assertThat(cond.getHasNextPage(), is(false));
        assertThat(cond.getNextPageNumber(), is(6));
        assertThat(cond.getLastPageNumber(), is(5));
        assertThat(cond.getStartPosition(), is(81));
        assertThat(cond.getEndPosition(), is(99));

        // 検索結果が101件の場合
        cond.setResultCount(101);
        cond.setPageNumber(1);
        assertThat(cond.getResultCount(), is(101));
        assertThat(cond.getPageNumber(), is(1));
        assertThat(cond.getPageCount(), is(6));
        assertThat(cond.getHasPrevPage(), is(false));
        assertThat(cond.getHasNextPage(), is(true));
        assertThat(cond.getNextPageNumber(), is(2));
        assertThat(cond.getLastPageNumber(), is(6));
        assertThat(cond.getStartPosition(), is(1));
        assertThat(cond.getEndPosition(), is(20));
        
        // 検索結果の取得開始ページを3に変更した場合(検索結果が101件のまま)
        cond.setResultCount(101);
        cond.setPageNumber(3);
        assertThat(cond.getResultCount(), is(101));
        assertThat(cond.getPageNumber(), is(3));
        assertThat(cond.getPageCount(), is(6));
        assertThat(cond.getHasPrevPage(), is(true));
        assertThat(cond.getFirstPageNumber(), is(1));
        assertThat(cond.getPrevPageNumber(), is(2));
        assertThat(cond.getHasNextPage(), is(true));
        assertThat(cond.getNextPageNumber(), is(4));
        assertThat(cond.getLastPageNumber(), is(6));
        assertThat(cond.getStartPosition(), is(41));
        assertThat(cond.getEndPosition(), is(60));
        
        // 検索結果の取得開始ページを5に変更した場合(検索結果が101件のまま)
        cond.setResultCount(101);
        cond.setPageNumber(5);
        assertThat(cond.getResultCount(), is(101));
        assertThat(cond.getPageNumber(), is(5));
        assertThat(cond.getPageCount(), is(6));
        assertThat(cond.getHasPrevPage(), is(true));
        assertThat(cond.getFirstPageNumber(), is(1));
        assertThat(cond.getPrevPageNumber(), is(4));
        assertThat(cond.getHasNextPage(), is(true));
        assertThat(cond.getNextPageNumber(), is(6));
        assertThat(cond.getLastPageNumber(), is(6));
        assertThat(cond.getStartPosition(), is(81));
        assertThat(cond.getEndPosition(), is(100));

        // 検索結果の取得開始ページを6に変更した場合(検索結果が101件のまま)
        cond.setResultCount(101);
        cond.setPageNumber(6);
        assertThat(cond.getResultCount(), is(101));
        assertThat(cond.getPageNumber(), is(6));
        assertThat(cond.getPageCount(), is(6));
        assertThat(cond.getHasPrevPage(), is(true));
        assertThat(cond.getFirstPageNumber(), is(1));
        assertThat(cond.getPrevPageNumber(), is(5));
        assertThat(cond.getHasNextPage(), is(false));
        assertThat(cond.getStartPosition(), is(101));
        assertThat(cond.getEndPosition(), is(101));
        
        // 検索結果の取得開始ページを5に変更し、かつ検索結果が減少した場合(ページ5の件数以上)
        cond.setResultCount(81);
        cond.setPageNumber(5);
        assertThat(cond.getResultCount(), is(81));
        assertThat(cond.getPageNumber(), is(5));
        assertThat(cond.getPageCount(), is(5));
        assertThat(cond.getHasPrevPage(), is(true));
        assertThat(cond.getFirstPageNumber(), is(1));
        assertThat(cond.getPrevPageNumber(), is(4));
        assertThat(cond.getHasNextPage(), is(false));
        assertThat(cond.getStartPosition(), is(81));
        assertThat(cond.getEndPosition(), is(81));

        // 検索結果の取得開始ページを5に変更し、かつ検索結果が減少した場合(ページ5の件数未満)
        cond.setResultCount(80);
        cond.setPageNumber(5);
        assertThat(cond.getResultCount(), is(80));
        assertThat(cond.getPageNumber(), is(5));
        assertThat(cond.getPageCount(), is(4)); // 総ページ数が現在のページを下まわる。
        assertThat(cond.getHasPrevPage(), is(true));
        assertThat(cond.getFirstPageNumber(), is(1));
        assertThat(cond.getPrevPageNumber(), is(4)); // いつでも現在のページからマイナス1。
        assertThat(cond.getHasNextPage(), is(false));
        assertThat(cond.getStartPosition(), is(81));
        assertThat(cond.getEndPosition(), is(80)); // このケースは意味がないので表示しないはず。検索結果の総件数が出力される。
    }

}
