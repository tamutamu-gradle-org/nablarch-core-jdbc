package nablarch.core.db.statement;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.Map;

import org.junit.Test;

/**
 * {@link BasicSqlLoader}のテストクラス。
 *
 * @author Hisaaki Sioiri
 */
public class BasicSqlLoaderTest {

    /** test target class */
    private BasicSqlLoader loader = new BasicSqlLoader();

    /**
     * {@link BasicSqlLoader#getValue(Object)}のテスト。
     *
     * @throws Exception
     */
    @Test
    public void testGetValue() throws Exception {
        Map<String, String> value = loader.getValue("nablarch.core.db.statement.sqlloader.BasicSqlLoaderTest");

        // SQL数は2つであること。
        assertThat(value.size(), is(3));

        // 各SQL文のassert
        assertThat(value.get("SQL001"), is("SELECT USER_NAME, TEL, FROM USER_MTR"));
        assertThat(value.get("SQL002"), is(
                "INSERT INTO USERS ( "
                        + "USER_ID, "
                        + "EXTENSION_NUMBER_BUILDING, "
                        + "EXTENSION_NUMBER_PERSONAL, "
                        + "INSERT_DATE, "
                        + "INSERT_USER_ID, "
                        + "KANA_NAME, "
                        + "KANJI_NAME, "
                        + "MAIL_ADDRESS, "
                        + "MOBILE_PHONE_NUMBER_AREA_CODE, "
                        + "MOBILE_PHONE_NUMBER_CITY_CODE, "
                        + "MOBILE_PHONE_NUMBER_SBSCR_CODE, "
                        + "UPDATED_DATE, "
                        + "UPDATED_USER_ID ) "
                        + "VALUES ( "
                        + ":USER_ID, "
                        + ":EXTENSION_NUMBER_BUILDING, "
                        + ":EXTENSION_NUMBER_PERSONAL, "
                        + ":INSERT_DATE, "
                        + ":INSERT_USER_ID, "
                        + ":KANA_NAME, "
                        + ":KANJI_NAME, "
                        + ":MAIL_ADDRESS, "
                        + ":MOBILE_PHONE_NUMBER_AREA_CODE, "
                        + ":MOBILE_PHONE_NUMBER_CITY_CODE, "
                        + ":MOBILE_PHONE_NUMBER_SBSCR_CODE, "
                        + ":UPDATED_DATE, "
                        + ":UPDATED_USER_ID )"));

        assertThat(value.get("SQL003"), is("BEGIN dbms_output.put_line('hoge'); END;"));
    }

    /**
     * SQL読み込み部分のテスト。
     * SQL文内の不要なスペースやタブが削除される。
     * ただし、リテラル内のスペースなどは削除されない。
     */
    @Test
    public void testSqlRead1() {
        Map<String, String> value = loader.getValue("nablarch.core.db.statement.sqlloader.SqlRead1");
        assertThat(value.size(), is(1));

        assertThat(value.get("SQL001"), is(
                "select * from table where col1 = ? and col2 = ? and col3 = '              '"));
    }

    /**
     * SQL読み込み部分のテスト。
     * SQL文ないのコメントは削除される。
     * ただし、リテラル内に書かれたコメント書式は削除されない。
     */
    @Test
    public void testSqlRead2() {
        Map<String, String> value = loader.getValue("nablarch.core.db.statement.sqlloader.SqlRead2");
        assertThat(value.size(), is(1));

        assertThat(value.get("SQL1"), is("update user set comment = '-- 削除されないコメント', b = '-- hogehoge', a = ?"));
    }

    /**
     * SQL読み込み部分のテスト。
     * 1ファイルに複数のSQL文が定義されている。
     */
    @Test
    public void testSqlRead3() {
        Map<String, String> value = loader.getValue("nablarch.core.db.statement.sqlloader.SqlRead3");
        assertThat(value.size(), is(3));

        for (int i = 1; i <= 3; i++) {
            assertThat(value.get(String.format("SQL%d", i)), is(String.format(
                    "select '%d' from dual", i)));
        }
    }

    /**
     * SQL読み込み部分のテスト。
     * エンコーディングを指定した場合
     */
    @Test
    public void testSqlRead4() {
        BasicSqlLoader loader = new BasicSqlLoader();
        loader.setFileEncoding("MS932");
        Map<String, String> value = loader.getValue("nablarch.core.db.statement.sqlloader.SqlRead4");

        assertThat(value.size(), is(1));
        assertThat(value.get("SQL1"), is("SELECT 'あいうえお' FROM DUAL"));
    }

    /**
     * SQL読み込み部分のテスト。
     * 拡張子を指定した場合
     */
    @Test
    public void testSqlRead5() {
        BasicSqlLoader loader = new BasicSqlLoader();
        loader.setExtension("txt");
        Map<String, String> value = loader.getValue("nablarch.core.db.statement.sqlloader.SqlRead5");

        assertThat(value.size(), is(1));
        assertThat(value.get("SQL1"), is("select '1' from dual"));
    }

    /**
     * SQL読み込み部分のテスト。
     * １行のなかでシングルクォート(')が閉じられていない場合(SQL文としては、不正な物)
     */
    @Test
    public void testSqlRead6() {
        BasicSqlLoader loader = new BasicSqlLoader();
        Map<String, String> value = loader.getValue("nablarch.core.db.statement.sqlloader.SqlRead6");

        assertThat(value.size(), is(1));
        assertThat(value.get("SQL1"), is("select '-- 不正な文   ' from dual"));
    }

    /**
     * SQL読み込み部分のテスト。
     * SQL_IDとSQL文が'='で区切られていない場合はロード時にエラーが発生すること。
     */
    @Test
    public void testSqlRead7() {
        BasicSqlLoader loader = new BasicSqlLoader();
        try {
            loader.getValue("nablarch.core.db.statement.sqlloader.SqlRead7");
            fail("do not run");
        } catch (Exception e) {
            assertThat(e.getMessage(), is(
                    "sql format is invalid. valid sql format is 'SQL_ID = SQL'."
                            + " sql resource = [classpath:nablarch/core/db/statement/sqlloader/SqlRead7.sql]"));
        }
    }

    /**
     * SQL読み込み部分のテスト。
     * 同一ファイル内に同一のSQLIDが存在した場合は、ロード時にエラーが発生すること。
     */
    @Test
    public void testSqlRead8() {
        BasicSqlLoader loader = new BasicSqlLoader();
        try {
            loader.getValue("nablarch.core.db.statement.sqlloader.SqlRead8");
            fail("do not run");
        } catch (Exception e) {
            assertThat(e.getMessage(), is(
                    "SQL_ID is duplicated. SQL_ID = [SQL1], sql resource = [classpath:nablarch/core/db/statement/sqlloader/SqlRead8.sql]"));
        }
    }

    /**
     * SQL読み込み部分のテスト。
     * 不正なエンコーディングを指定した場合
     */
    @Test
    public void testSqlRead9() {
        BasicSqlLoader loader = new BasicSqlLoader();
        loader.setFileEncoding("不正なエンコーディング");
        try {
            loader.getValue("nablarch.core.db.statement.sqlloader.SqlRead8");
            fail("do not run");
        } catch (Exception e) {
            assertThat(e.getMessage(), is(
                    "can not read in SQL definition file. sql resource = [classpath:nablarch/core/db/statement/sqlloader/SqlRead8.sql]"));
        }
    }

    /**
     * SQL読み込み部分のテスト。
     * 異なるSQL文の間にコメントを入れた場合(SQL文としては、不正な物)
     */
    @Test
    public void testSqlRead10() {
        BasicSqlLoader loader = new BasicSqlLoader();
        Map<String, String> value = loader.getValue("nablarch.core.db.statement.sqlloader.SqlRead10");

        assertThat(value.size(), is(2));
        assertThat(value.get("SQL2"), is("select '2' from dual SQL3 = select '3' from dual"));
    }
    
    /**
     * SQL読み込み部分のテスト。
     * 1つのSQL文の中に空行を入れた場合はロード時にエラーが発生すること。
     */
    @Test
    public void testSqlRead11() {
        BasicSqlLoader loader = new BasicSqlLoader();
        try {
            loader.getValue("nablarch.core.db.statement.sqlloader.SqlRead11");
            fail("do not run");
        } catch (Exception e) {
            assertThat(e.getMessage(), is(
                    "sql format is invalid. valid sql format is 'SQL_ID = SQL'."
                            + " sql resource = [classpath:nablarch/core/db/statement/sqlloader/SqlRead11.sql]"));
        }
    }
    
    /**
     * {@link BasicSqlLoader#getValues(String, Object)}のテスト
     * 本メソッドは、サポートしないため必ずnullが返却される。
     *
     * @throws Exception
     */
    @Test
    public void testGetValues() throws Exception {
        assertThat(loader.getValues("ID", new Object()), nullValue());
    }

    /**
     * {@link BasicSqlLoader#loadAll()}のテスト。
     * 本メソッドは、サポートしないため必ずnullが返却される。
     *
     * @throws Exception
     */
    @Test
    public void testLoadAll() throws Exception {
        assertThat(loader.loadAll(), nullValue());
    }

    /**
     * {@link BasicSqlLoader#getIndexNames()}のテスト
     * 本メソッドは、サポートしないため必ずnullが返却される。
     *
     * @throws Exception
     */
    @Test
    public void testGetIndexNames() throws Exception {
        assertThat(loader.getIndexNames(), nullValue());
    }

    /**
     * {@link BasicSqlLoader#getId(Map)}のテスト。
     * 本メソッドは、サポートしないため必ずnullが返却される。
     *
     * @throws Exception
     */
    @Test
    public void testGetId() throws Exception {

        Object o = loader.getId(null);
        assertThat(o, nullValue());
    }

    /**
     * {@link BasicSqlLoader#generateIndexKey(String, Map)}のテスト。
     * 本メソッドは、サポートしないため必ずnullが返却される。
     *
     * @throws Exception
     */
    @Test
    public void testGenerateIndexKey() throws Exception {
        Object o = loader.generateIndexKey("", null);
        assertThat(o, nullValue());
    }
}
