package nablarch.core.db.statement;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nablarch.core.cache.StaticDataLoader;
import nablarch.core.util.FileUtil;

/**
 * SQL文をクラスパス上のリソース(SQLファイル)からロードするクラス。<br/>
 * <p/>
 * 本クラスでは、下記ルールに従いSQL文をロードする。<br/>
 * <p/>
 * a) SQL_IDとSQL文の1グループは、空行で区切られて記述されている。
 * <p/>
 * 1つのSQL文の中に空行はいれてはならない。
 * また、異なるSQL文との間には必ず空行を入れなくてはならない。
 * コメント行は、空行とはならない。
 * <p/>
 * b) 1つのSQL文の最初の「=」までがSQL_IDとなる。
 * <p/>
 * SQL_IDとは、SQLファイル内でSQL文を一意に特定するためのIDである。
 * SQL_IDには、任意の値を設定することが可能となっている。
 * <p/>
 * c) コメントは、「--」で開始されている必要がある。
 * <p/>
 * 「--」以降の値は、コメントとして扱い読み込まない。<br/>
 * ※コメントは、行コメントとして扱う。複数行に跨るブロックコメントはサポートしない。
 * <p/>
 * d) SQL文の途中で改行を行っても良い。また、可読性を考慮してスペースやtabなどで桁揃えを行っても良い。
 *
 * @author Hisaaki Sioiri
 */
public class BasicSqlLoader implements StaticDataLoader<Map<String, String>> {

    /** ファイルエンコーディング */
    private String fileEncoding;

    /** コメント開始文字 */
    private static final String COMMENT = "--";

    /** SQLファイルの拡張子(デフォルトは、.sql) */
    private String extension = "sql";

    /**
     * ファイルエンコーディングを設定する。<br/>
     * <p/>
     * ここで設定されたエンコーディングを使用してSQLファイルを読み込む。
     * 本設定を行わない場合は、JVMのデフォルトエンコーディングを使用してSQLファイルが読み込まれる。
     *
     * @param fileEncoding ファイルエンコーディング
     */
    public void setFileEncoding(String fileEncoding) {
        this.fileEncoding = fileEncoding;
    }

    /**
     * 拡張子を設定する。<br/>
     * ここで設定された拡張子を付加したファイルをSQLファイルとして読み込む。
     * 指定がない場合は、デフォルトで拡張子はsqlとなる。
     *
     * @param extension 拡張子
     */
    public void setExtension(String extension) {
        this.extension = extension;
    }

    /**
     * SQL文をロードする。
     *
     * @param id データのID(SQL文が書かれたファイルのリソース名)
     * @return ロードしたSQL文
     *         <br/>
     *         KEY->SQL_ID<br/>
     *         VALUE->SQL文
     */
    public Map<String, String> getValue(Object id) {
        Map<String, String> sqlHolder = new HashMap<String, String>();

        String sqlResource = String.format("classpath:%s.%s", id.toString().replace('.', '/'),
                extension);
        BufferedReader reader = null;
        InputStream resource = FileUtil.getResource(sqlResource);
        try {
            if (fileEncoding == null) {
                reader = new BufferedReader(new InputStreamReader(resource));
            } else {
                reader = new BufferedReader(new InputStreamReader(resource, fileEncoding));
            }

            String line;
            StringBuilder sb = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                if (line.trim().length() == 0 && sb.length() != 0) {
                    // SQLの終端の場合
                    formatSql(sqlResource, sb.toString(), sqlHolder);
                    sb.setLength(0);
                    continue;
                }

                // コメントを除去する。
                String trimLine = trimComment(line);

                if (trimLine.length() == 0) {
                    // 空行は処理しない。
                    continue;
                }

                // 行頭にスペースを挿入せずに複数行に渡ってSQL文を記述された場合、
                // 行と行の間にスペースが入らなくなってしまうため、明示的にスペースを挿入
                sb.append(" ").append(trimLine);
            }
            if (sb.length() != 0) {
                // 最後が空行で終っていなかった場合の対処
                formatSql(sqlResource, sb.toString(), sqlHolder);
            }
        } catch (IOException e) {
            throw new RuntimeException(String.format(
                    "can not read in SQL definition file. sql resource = [%s]", sqlResource), e);
        } finally {
            FileUtil.closeQuietly(reader);
        }
        return sqlHolder;
    }

    /**
     * 1SQLをSQL_IDとSQL文に分割し、保持する。
     *
     * @param sqlResource SQLリソース名
     * @param line 1SQL
     * @param holder SQL文を保持するMap
     */
    private void formatSql(String sqlResource, String line, Map<String, String> holder) {
        // SQL文とSQLIDは'='で区切られている。
        int index = line.indexOf('=');
        if (index == -1) {
            throw new RuntimeException(String.format(
                    "sql format is invalid. valid sql format is 'SQL_ID = SQL'. sql resource = [%s]",
                    sqlResource));
        }
        String sqlId = line.substring(0, index).trim();
        String sql = line.substring(index + 1).trim();

        if (holder.put(sqlId, trimWhiteSpaceAndUnEscape(sql)) != null) {
            throw new RuntimeException(String.format(
                    "SQL_ID is duplicated. SQL_ID = [%s], sql resource = [%s]", sqlId,
                    sqlResource));
        }
    }

    /**
     * {@inheritDoc}
     * <br/>
     * 本メソッドは、サポートしない。
     */
    public List<Map<String, String>> getValues(String indexName, Object key) {
        return null;
    }

    /**
     * {@inheritDoc}
     * <br/>
     * 本メソッドはサポートしない。
     */
    public List<Map<String, String>> loadAll() {
        return null;
    }

    /**
     * {@inheritDoc}
     * <br/>
     * 本メソッドはサポートしない。
     */
    public List<String> getIndexNames() {
        return null;
    }

    /**
     * {@inheritDoc}
     * <br/>
     * 本メソッドはサポートしない。
     */
    public Object getId(Map<String, String> value) {
        return null;
    }

    /**
     * {@inheritDoc}
     * <br/>
     * 本メソッドはサポートしない。
     */
    public Object generateIndexKey(String indexName, Map<String, String> value) {
        return null;
    }

    /**
     * SQL文から不要なスペース(スペースの定義は、{@link Character#isWhitespace(char)})を削除する。<br/>
     * <br/>
     * 前後のスペースだけではなく、見た目を整えるために挿入されているSQL文中のスペースも削除する。<br/>
     * また、アンエスケープ処理を行う。
     *
     * @param sql SQL文
     * @return スペースを除去したSQL文
     */
    private String trimWhiteSpaceAndUnEscape(String sql) {
        StringBuilder sb = new StringBuilder();
        boolean preWhiteSpace = false;
        boolean literal = false;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            literal = c == '\'' ? !literal : literal;
            if (literal) {
                sb.append(c);
                continue;
            }
            if (!Character.isWhitespace(c)) {
                // ホワイトスペース以外は追加する。
                sb.append(c);
                preWhiteSpace = false;
            } else {
                // ホワイトスペースの場合は、１文字前がホワイトスペース以外の場合に
                // 半角スペースを１文字追加する。
                if (!preWhiteSpace) {
                    sb.append(' ');
                }
                preWhiteSpace = true;
            }
        }
        return sb.toString();
    }

    /**
     * ファイルから読み込んだ１行データからコメントを削除する。
     *
     * @param line 1行データ
     * @return コメントを削除したデータ
     */
    private String trimComment(String line) {
        int pos = line.indexOf(COMMENT);
        if (pos == -1) {
            // コメントが存在しない場合は、そのまま返却
            return line;
        }
        // 以降コメントが存在する場合の処理
        int literalPos = line.indexOf("'");
        if (literalPos == -1 || pos < literalPos) {
            // リテラルが存在しない場合や、コメント以降のリテラルの場合
            // コメント以降を削除し返却
            return line.substring(0, pos);
        }
        // リテラルが存在する場合の処理
        // リテラル以降のコメント部分のみを削除する。
        StringBuilder sb = new StringBuilder();
        int literalEndPos = line.indexOf("'", literalPos + 1);
        if (literalEndPos != -1) {
            sb.append(line.substring(0, literalEndPos + 1));
            sb.append(trimComment(line.substring(literalEndPos + 1)));
        } else {
            sb.append(line);
        }
        return sb.toString();
    }

}

