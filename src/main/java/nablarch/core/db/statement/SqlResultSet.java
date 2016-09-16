package nablarch.core.db.statement;

import nablarch.core.util.annotation.Published;

import java.util.ArrayList;

/**
 * 簡易検索結果を保持するクラス。
 *
 * @author Hisaaki Sioiri
 * @see nablarch.core.db.statement.BasicSqlPStatement#retrieve()
 */
@Published
public class SqlResultSet extends ArrayList<SqlRow> {

    /**
     * 検索結果({@link java.sql.ResultSet})から{@link SqlResultSet}のオブジェクトを構築する。
     *
     * @param rs 検索結果
     * @param startPos 検索結果の取得開始位置
     * @param max 取得最大件数
     */
    public SqlResultSet(ResultSetIterator rs, int startPos, int max) {
        super(max == 0 ? 10 : max);

        // 開始位置まで空回し
        for (int i = 0; (i < (startPos - 1)) && rs.next(); i++) ;

        while (rs.next()) {
            add(rs.getRow());
        }
    }

    /**
     * {@link SqlResultSet}を生成する。
     *
     * @param size 初期容量
     * @see ArrayList#ArrayList(int)
     */
    protected SqlResultSet(int size) {
        super(size);
    }


}

