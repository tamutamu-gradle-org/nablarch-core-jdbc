package nablarch.core.db.statement;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nablarch.core.db.statement.sqlconvertor.VariableInSyntaxConvertor;

import org.junit.Test;


/**
 * BasicSqlParameterParserのテストクラス。
 *
 * @author Hisaaki Sioiri
 */
public class BasicSqlParameterParserTest {

    /** parseのテスト。 */
    @Test
    public void testParse() {
        BasicSqlParameterParser parser = new BasicSqlParameterParser();

        // 名前付きバインド変数あり
        parser.parse("select * from test where col1 = :col12345");
        assertEquals("select * from test where col1 = ?", parser.getSql());
        List<String> list = parser.getNameList();
        assertEquals(1, list.size());
        assertEquals("col12345", list.get(0));

        parser.parse("select * from test where col1 = :col12345 and col2 = :userId");
        assertEquals("select * from test where col1 = ? and col2 = ?", parser.getSql());
        List<String> nameList = parser.getNameList();
        assertEquals(2, nameList.size());
        assertEquals("col12345", nameList.get(0));
        assertEquals("userId", nameList.get(1));

        // リテラル内にコロンあり
        // リテラル内は置き換えられない
        parser.parse("select * from test where col1 = :col12345 and col2 = '12345:12345'");
        assertEquals("select * from test where col1 = ? and col2 = '12345:12345'", parser.getSql());
        List<String> parserNameList = parser.getNameList();
        assertEquals(1, parserNameList.size());
        assertEquals("col12345", parserNameList.get(0));

        // リテラル内にコロンあり
        // リテラル内は置き換えられない
        parser.parse(
                "select '1234', '2345', ''':a''', nvl(col1, :VAR) from test where col1 = :col12345 and col2 = '123''45:12345'");
        assertEquals(
                "select '1234', '2345', ''':a''', nvl(col1, ?) from test where col1 = ? and col2 = '123''45:12345'",
                parser.getSql());
        parserNameList = parser.getNameList();
        assertEquals(2, parserNameList.size());
        assertEquals("VAR", parserNameList.get(0));
        assertEquals("col12345", parserNameList.get(1));

        // リテラルのエスケープが不正な場合
        // SQL文の解析が行えないため、名前付き変数のリストはサイズ0となる。
        parser.parse("select 'abc'' from test where col1 = :col12345");
        parserNameList = parser.getNameList();
        assertEquals("select 'abc'' from test where col1 = :col12345", parser.getSql());
        assertEquals(0, parserNameList.size());

        // 名前付きバインド変数の存在しないSQLの場合
        parser.parse("select 'abc'' from test");
        parserNameList = parser.getNameList();
        assertEquals("select 'abc'' from test", parser.getSql());
        assertEquals(0, parserNameList.size());

        // 配列指定
        parser.parse("select * from test where col1 = :COL_1[0]");
        List<String> list1 = parser.getNameList();
        assertEquals(1, list1.size());
        assertEquals("COL_1[0]", list1.get(0));
        assertEquals("select * from test where col1 = ?", parser.getSql());

        // 配列複数指定
        parser.parse(
                "select * from test where col1 = :col1 and col2 in (:col2[0], :col2[1]) and col3 = :col2[2]");
        List<String> list2 = parser.getNameList();
        assertEquals(4, list2.size());
        assertEquals("col1", list2.get(0));
        assertEquals("col2[0]", list2.get(1));
        assertEquals("col2[1]", list2.get(2));
        assertEquals("col2[2]", list2.get(3));
        assertEquals("select * from test where col1 = ? and col2 in (?, ?) and col3 = ?",
                parser.getSql());

        // エラー系
        // 配列の添字に数字以外が使用されている場合
        try {
            parser.parse("select * from test where col1 = :col1[a]");
            fail("do not run.");
        } catch (Exception e) {
            assertEquals(
                    "parameter name of Array is invalid. sql = [select * from test where col1 = :col1[a]]",
                    e.getMessage());
        }

        // かっこが閉じられていない場合
        try {
            parser.parse("select * from test where col1 = :col1[100");
            fail("do not run.");
        } catch (Exception e) {
            assertEquals(
                    "parameter name of Array is invalid. sql = [select * from test where col1 = :col1[100]",
                    e.getMessage());
        }

        try {

            parser.parse("select * from test where col1 = :col[" + (char) ('0' - 1));
            fail("do not run.");
        } catch (Exception e) {
            assertEquals(
                    "parameter name of Array is invalid. sql = [select * from test where col1 = :col[" + (char) ('0' - 1) + "]",
                    e.getMessage());
        }

        // カバレッジをとおすためのケース
        parser.parse("select * from test where col1 = :col" + ((char) ('z' + 1)));
        List<String> list3 = parser.getNameList();
        assertEquals(1, list3.size());
        assertEquals("col", list3.get(0));
        assertEquals("select * from test where col1 = ?" + (char) ('z' + 1), parser.getSql());

    }

    /**
     * {@link SqlParameterParser#parse}のテスト。<br>
     * %つきのSQL文の解析確認を行う。
     */
    @Test
    public void testParse2() {
        BasicSqlParameterParser parser = new BasicSqlParameterParser();
        parser.setLikeEscapeChar('\\');

        // %が存在している場合(全てのパターンを一括で確認)
        parser.parse(
                "select * from test where col1 = :%col12345 and (col2 = :%userId% or col3 = :userName%)");
        assertEquals(
                "select * from test where col1 = ? escape '\\' and (col2 = ? escape '\\' or col3 = ? escape '\\')",
                parser.getSql());

        List<String> list = parser.getNameList();
        assertEquals(3, list.size());
        assertEquals("%col12345", list.get(0));
        assertEquals("%userId%", list.get(1));
        assertEquals("userName%", list.get(2));

        parser.setLikeEscapeChar('$');
        parser.parse(
                "select * from test where col1 = :%col12345 and (col2 = :userId% or col3 = :userName%)");
        assertEquals(
                "select * from test where col1 = ? escape '$' and (col2 = ? escape '$' or col3 = ? escape '$')",
                parser.getSql());

        List<String> nameList = parser.getNameList();
        assertEquals(3, nameList.size());
        assertEquals("%col12345", nameList.get(0));
        assertEquals("userId%", nameList.get(1));
        assertEquals("userName%", nameList.get(2));

        // 不正な位置に%がある場合
        parser.setLikeEscapeChar('\\');
        parser.parse("select * from test where col1 = :col12345 and col2 = :%us%erId%");
        assertEquals("select * from test where col1 = ? and col2 = ? escape '\\'", parser.getSql());
        List<String> parserNameList = parser.getNameList();
        assertEquals(2, parserNameList.size());
        assertEquals("col12345", parserNameList.get(0));
        assertEquals("%us%erId%", parserNameList.get(1));     // 不正な位置に%があるバインド変数名となること。
    }

    /**
     * {@link SqlParameterParser#parse}のテスト。<br>
     * 可変条件をもつSQL文のテスト(引数がMapのパターン)
     */
    @Test
    public void testParseVariableConditionForMap() {
        BasicSqlParameterParser parser = new BasicSqlParameterParser();
        parser.setLikeEscapeChar('\\');

        Map<String, Object> cond = new HashMap<String, Object>();
        cond.put("col1", null);
        cond.put("col3", "hoge");

        // col1は、入力無なので「0 = 0」が挿入される。
        // col3は、入力有なので「0 = 1」が挿入される。
        String actual = parser.convertToJdbcSql(
                "select * from test where $if(col1){col1 = :col1} and (col2 = :col2 or $if(col3){col3 like :col3%})",
                cond);
        assertEquals(
                "select * from test where (0 = 0 or (col1 = :col1)) and (col2 = :col2 or (0 = 1 or (col3 like :col3%)))",
                actual);

        Map<String, Object> map = new HashMap<String, Object>();
        map.put("col1", new Timestamp(System.currentTimeMillis()));
        map.put("col3", null);

        // col1は、入力有なので「0 = 1」が挿入される。
        // col3は、入力無なので「0 = 0」が挿入される。
        String s = parser.convertToJdbcSql(
                "select * from test where $if(col1){col1 = :col1} and (col2 = :col2 or $if(col3){col3 like :%col3%})",
                map);
        assertEquals(
                "select * from test where (0 = 1 or (col1 = :col1)) and (col2 = :col2 or (0 = 0 or (col3 like :%col3%)))",
                s);

        // 上記と同じ条件で、バインド変数が左辺にあるパターン
        String s1 = parser.convertToJdbcSql(
                "select * from test where $if(col1){:col1=col1} and (:col2 = col2 or $if(col3){col3  like  :%col3})",
                map);
        assertEquals(
                "select * from test where (0 = 1 or (:col1=col1)) and (:col2 = col2 or (0 = 0 or (col3  like  :%col3)))",
                s1);

        String s2 = parser.convertToJdbcSql(
                "select * from test where $if(col1){:col1=col1 and :col = col} and (:col2 = col2 or $if(col3){col3  like  :col3%})",
                map);
        assertEquals(
                "select * from test where (0 = 1 or (:col1=col1 and :col = col)) and (:col2 = col2 or (0 = 0 or (col3  like  :col3%)))",
                s2);

        // 条件が以下、以上の場合
        Map<String, Object> hashMap = new HashMap<String, Object>();
        hashMap.put("cond", new byte[]{});
        hashMap.put("col2", "");
        String s3 = parser.convertToJdbcSql(
                "select * from test where $if(cond){col1 <= :cond} and $if(cond){col1 >= :cond} and $if(col2){col2 != :col2}",
                hashMap);
        assertEquals(
                "select * from test where (0 = 0 or (col1 <= :cond)) and (0 = 0 or (col1 >= :cond)) and (0 = 0 or (col2 != :col2))",
                s3);

        Map<String, Object> objectHashMap = new HashMap<String, Object>();
        objectHashMap.put("col1", new Timestamp(System.currentTimeMillis()));
        objectHashMap.put("col3", "");
        // 例外パターン
        // バインド変数名がMapに存在しない場合
        try {
            parser.convertToJdbcSql(
                    "select * from test where $if(col11){:col11=col1} and (:col2 = col2 or $if(col3){col3  like  :col3%})",
                    objectHashMap);
            fail("エラーなので、ここは通らない。");
        } catch (IllegalArgumentException e) {
            assertEquals("there is not sql parameter 'col11' in the key of the Map.",
                    e.getMessage());
        }
        
        // SELECT句で、$if構文を使用しているため不正
        String sql = parser.convertToJdbcSql(
                        "SELECT $if (col1) {:col1} FROM USER_MTR",
                        objectHashMap);
        assertEquals("SELECT (0 = 1 or (:col1)) FROM USER_MTR", sql);
        
        // WHERE句で$if構文を使用しているが、ネストしているため不正
        sql = parser.convertToJdbcSql(
                    "SELECT "
                        + "USER_ID "
                    + "FROM "
                        + "USER_MTR "
                    + "WHERE $if (col1) {USER = :col1 $if(col3) {USER_ID = :col3}}",
                    objectHashMap);
        assertEquals("SELECT USER_ID FROM USER_MTR WHERE (0 = 1 or (USER = :col1 $if(col3) {USER_ID = :col3))}", sql);
    }

    /**
     * {@link SqlParameterParser#convertToJdbcSql(String, Object)}のテスト。<br>
     * 可変条件をもつSQL文のテスト(引数がObjectのパターン)
     */
    @Test
    public void testParseVariableConditionForObject() {
        BasicSqlParameterParser parser = new BasicSqlParameterParser();
        parser.setLikeEscapeChar('\\');

        Entity entity = new Entity();
        entity.userId = "";
        entity.userName = null;
        entity.date = new Date(System.currentTimeMillis());

        // dateのみ値がある場合
        String sql =
                "select * from table1 t1, table2 t2" + " where $if(userId){t1.user_id = :userId}"
                        + " and t1.user_id = t2.user_id"
                        + " and $if(userName){t2.user_name = :userName}"
                        + " and $if(date){t2.date = :date}";
        String actual = parser.convertToJdbcSql(sql, entity);
        assertEquals(
                "select * from table1 t1, table2 t2" + " where (0 = 0 or (t1.user_id = :userId))"
                        + " and t1.user_id = t2.user_id"
                        + " and (0 = 0 or (t2.user_name = :userName))"
                        + " and (0 = 1 or (t2.date = :date))", actual);


        // userNameに値を設定
        entity.userName = "name";
        actual = parser.convertToJdbcSql(sql, entity);
        assertEquals(
                "select * from table1 t1, table2 t2" + " where (0 = 0 or (t1.user_id = :userId))"
                        + " and t1.user_id = t2.user_id"
                        + " and (0 = 1 or (t2.user_name = :userName))"
                        + " and (0 = 1 or (t2.date = :date))", actual);

        // userIdに値を設定
        entity.userId = "userId";
        actual = parser.convertToJdbcSql(sql, entity);
        assertEquals(
                "select * from table1 t1, table2 t2" + " where (0 = 1 or (t1.user_id = :userId))"
                        + " and t1.user_id = t2.user_id"
                        + " and (0 = 1 or (t2.user_name = :userName))"
                        + " and (0 = 1 or (t2.date = :date))", actual);

        // 存在しないフィールド名
        try {
            parser.convertToJdbcSql("select * from user where $if(user){user_id = :user}",
                    entity);
            fail("ここはとおらない。");
        } catch (Exception e) {
            assertEquals("failed to get user property.", e.getMessage());
        }
    }

    /**
     * {@link SqlParameterParser#convertToJdbcSql(String, Object)}のIN句用テスト。<br>
     * 可変条件をもつSQL文のテスト(引数がObjectのパターン)
     */
    @Test
    public void testParseVariableConditionForObjectInStatementByObject() {
        BasicSqlParameterParser parser = new BasicSqlParameterParser();
        parser.setLikeEscapeChar('\\');

        final Map<String, Object> object = new HashMap<String, Object>();
        object.put("hoge", new String[1]);
        object.put("hoge1", Arrays.asList("1", "2"));
        object.put("hoge2", new String[3]);
        object.put("hoge3", new String[0]);
        object.put("hoge4", new ArrayList<String>());
        object.put("hoge5", "");

        // 配列サイズが1の場合
        String sql1 = "select * from hoge where hoge in (:hoge[])";
        String result1 = parser.convertToJdbcSql(sql1, object);
        assertThat(result1, is("select * from hoge where hoge in (:hoge[0])"));

        // 配列サイズが1、2、3の要素がある場合
        String sql2 = "select * from hoge where hoge in (:hoge[]) and hoge1 in (:hoge1[]) and hoge2 in(:hoge2[])";
        String result2 = parser.convertToJdbcSql(sql2, object);
        assertThat(result2,
                is("select * from hoge where hoge in (:hoge[0]) and hoge1 in (:hoge1[0],:hoge1[1]) and hoge2 in(:hoge2[0],:hoge2[1],:hoge2[2])"));

        // 配列サイズが0の場合（SQLは置換えされない）
        String sql3 = "select * from hoge where hoge3 in (:hoge3[])";
        String result3 = parser.convertToJdbcSql(sql3, object);
        assertThat(result3, is("select * from hoge where hoge3 in (:hoge3[])"));

        // 可変引数とIN句の混在
        String sql4 = "select * from hoge where $if (hoge) {hoge1 in (:hoge[])} and $if (hoge4) {hoge4 in (:hoge4[])}";
        String result4 = parser.convertToJdbcSql(sql4, object);
        // :hoge4[]は、置き換えられない。
        assertThat(result4,
                is("select * from hoge where (0 = 1 or (hoge1 in (:hoge[0]))) and (0 = 0 or (hoge4 in (:hoge4[])))"));

        // IN句以外の場所で使用した場合。
        String sql5 = "select * from hoge where hoge = :hoge[] and hoge1 = :hoge1[] and hoge2 = :hoge2[2]";
        String result5 = parser.convertToJdbcSql(sql5, object);
        // IN句以外でも、[]は、[0]や[1]などに置き換わること。
        // ただし、明示的に添字を指定している箇所は置き換わらないこと。
        assertThat(result5,
                is("select * from hoge where hoge = :hoge[0] and hoge1 = :hoge1[0],:hoge1[1] and hoge2 = :hoge2[2]"));

        try {
            String sql6 = "select * from hoge where hoge5 in (:hoge5[])";
            parser.convertToJdbcSql(sql6, object);
            fail("do not run.");
        } catch (Exception e) {
            assertThat(e.getMessage(), is("object type in field is invalid. valid object type is Collection or Array. field name = [hoge5]."));
        }


    }

    /**
     * {@link SqlParameterParser#convertToJdbcSql(String, Object)}のIN句用テスト。<br>
     * 可変条件をもつSQL文のテスト(引数がMapのパターン)
     */
    @Test
    public void testParseVariableConditionForObjectInStatementByMap() {
        BasicSqlParameterParser parser = new BasicSqlParameterParser();

        // Map指定
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("hoge", new String[1]);
        map.put("hoge1", new ArrayList<String>() {
            {
                add("a");
                add("b");
            }
        });
        map.put("hoge2", new String[3]);
        map.put("hoge3", new String[0]);
        map.put("hoge4", new ArrayList<String>());

        // 配列サイズが1の場合
        String sql1 = "select * from hoge where hoge in (:hoge[])";
        String result1 = parser.convertToJdbcSql(sql1, map);
        assertThat(result1, is("select * from hoge where hoge in (:hoge[0])"));

        // 配列サイズが1、2、3の要素がある場合
        String sql2 = "select * from hoge where hoge in (:hoge[]) and hoge1 in (:hoge1[]) and hoge2 in(:hoge2[])";
        String result2 = parser.convertToJdbcSql(sql2, map);
        assertThat(result2,
                is("select * from hoge where hoge in (:hoge[0]) and hoge1 in (:hoge1[0],:hoge1[1]) and hoge2 in(:hoge2[0],:hoge2[1],:hoge2[2])"));

        // 配列サイズが0の場合（SQLは置換えされない）
        String sql3 = "select * from hoge where hoge3 in (:hoge3[])";
        String result3 = parser.convertToJdbcSql(sql3, map);
        assertThat(result3, is("select * from hoge where hoge3 in (:hoge3[])"));

        // 可変引数とIN句の混在
        String sql4 = "select * from hoge where $if (hoge) {hoge1 in (:hoge[])} and $if (hoge4) {hoge4 in (:hoge4[])}";
        String result4 = parser.convertToJdbcSql(sql4, map);
        // :hoge4[]は、置き換えられない。
        assertThat(result4,
                is("select * from hoge where (0 = 1 or (hoge1 in (:hoge[0]))) and (0 = 0 or (hoge4 in (:hoge4[])))"));

        // IN句以外の場所で使用した場合。
        String sql5 = "select * from hoge where hoge = :hoge[] and hoge1 = :hoge1[] and hoge2 = :hoge2[2]";
        String result5 = parser.convertToJdbcSql(sql5, map);
        // IN句以外でも、[]は、[0]や[1]などに置き換わること。
        // ただし、明示的に添字を指定している箇所は置き換わらないこと。
        assertThat(result5,
                is("select * from hoge where hoge = :hoge[0] and hoge1 = :hoge1[0],:hoge1[1] and hoge2 = :hoge2[2]"));

    }

    /**
     * {@link SqlParameterParser#convertToJdbcSql(String, Object)}のORDER BY句用テスト。<br>
     * 可変条件をもつSQL文のテスト(引数がObjectのパターン)
     */
    @Test
    public void testParseVariableConditionForObjectOrderByStatementByObject() {
        BasicSqlParameterParser parser = new BasicSqlParameterParser();
        parser.setLikeEscapeChar('\\');

        Entity obj = new Entity();
        
        // 可変ORDER BY構文が含まれない場合
        String sql = "select * from user where userId = :userId order by userId";
        String replaceSql = parser.convertToJdbcSql(sql, obj);
        assertThat(replaceSql, is("select * from user where userId = :userId order by userId"));
        
        // 可変ORDER BY構文のケースにヒットする場合(1つ目)
        obj = new Entity();
        obj.sortId = 1;
        
        sql = "select * from user where userId = :userId $sort(sortId) {(1 userId asc) (2 userId desc) (3 userName asc) (4 userName desc) (default userId)}";
        replaceSql = parser.convertToJdbcSql(sql, obj);
        assertThat(replaceSql, is("select * from user where userId = :userId ORDER BY userId asc"));
        
        // 可変ORDER BY構文のケースにヒットする場合(2つ目)
        obj = new Entity();
        obj.sortId = 2;
        
        sql = "select * from user where userId = :userId $sort(sortId) {(1 userId asc) (2 userId desc) (3 userName asc) (4 userName desc) (default userId)}";
        replaceSql = parser.convertToJdbcSql(sql, obj);
        assertThat(replaceSql, is("select * from user where userId = :userId ORDER BY userId desc"));
        
        // 可変ORDER BY構文のケースにヒットする場合(4つ目)
        obj = new Entity();
        obj.sortId = 4;
        
        sql = "select * from user where userId = :userId $sort(sortId) {(1 userId asc) (2 userId desc) (3 userName asc) (4 userName desc) (default userId)}";
        replaceSql = parser.convertToJdbcSql(sql, obj);
        
        // 可変ORDER BY構文のケースにヒットしない場合(defaultあり)
        obj = new Entity();
        obj.sortId = 5;
        
        sql = "select * from user where userId = :userId $sort(sortId) {(1 userId asc) (2 userId desc) (3 userName asc) (4 userName desc) (default userId)}";
        replaceSql = parser.convertToJdbcSql(sql, obj);
        assertThat(replaceSql, is("select * from user where userId = :userId ORDER BY userId"));

        // 可変ORDER BY構文のケースにヒットしない場合(defaultあり、かつパラメータの値がnull)
        obj = new Entity();
        obj.sortId = null;
        
        sql = "select * from user where userId = :userId $sort(sortId) {(1 userId asc) (2 userId desc) (3 userName asc) (4 userName desc) (default userId)}";
        replaceSql = parser.convertToJdbcSql(sql, obj);
        assertThat(replaceSql, is("select * from user where userId = :userId ORDER BY userId"));

        // 可変ORDER BY構文のケースにヒットしない場合(defaultなし)
        obj = new Entity();
        obj.sortId = 5;
        
        sql = "select * from user where userId = :userId $sort(sortId) {(1 userId asc) (2 userId desc) (3 userName asc) (4 userName desc)}";
        replaceSql = parser.convertToJdbcSql(sql, obj);
        assertThat(replaceSql, is("select * from user where userId = :userId "));

        // 可変ORDER BY構文のケースにヒットしない場合(defaultなし、かつパラメータの値がnull)
        obj = new Entity();
        obj.sortId = null;
        
        sql = "select * from user where userId = :userId $sort(sortId) {(1 userId asc) (2 userId desc) (3 userName asc) (4 userName desc)}";
        replaceSql = parser.convertToJdbcSql(sql, obj);
        assertThat(replaceSql, is("select * from user where userId = :userId "));

        // 存在しないパラメータ名の場合
        obj = new Entity();
        obj.sortId = 5;
        
        sql = "select * from user where userId = :userId $sort(unknown) {(1 userId asc) (2 userId desc) (3 userName asc) (4 userName desc)}";
        try {
            parser.convertToJdbcSql(sql, obj);
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("failed to get unknown property."));
        }

        // SELECT句で$sort構文を使用しているため不正
        obj = new Entity();
        obj.sortId = 1;
        
        sql = "select $sort(sortId) {(1 userId asc) (2 userId desc) (3 userName asc) (4 userName desc) (default userId) from user where userId = :userId}";
        replaceSql = parser.convertToJdbcSql(sql, obj);
        assertThat(replaceSql, is("select ORDER BY userId asc"));

        // WHERE句で$sort構文を使用しているため不正
        obj = new Entity();
        obj.sortId = 1;
        
        sql = "select * from user where $sort(sortId) {(1 userId asc) (2 userId desc) (3 userName asc) (4 userName desc) (default userId) from user where userId = :userId}";
        replaceSql = parser.convertToJdbcSql(sql, obj);
        assertThat(replaceSql, is("select * from user where ORDER BY userId asc"));
        
        // 可変ORDER BY構文を複数使用している場合
        obj = new Entity();
        obj.sortId1 = 2;
        obj.sortId2 = 3;
        
        sql = "select * from ("
                + "select * from user $sort(sortId1) {(1 userId asc) (2 userId desc) (3 userName asc) (4 userName desc) (default userId)}"
            + ") where rownum <= :number $sort(sortId2) {(1 userId asc) (2 userId desc) (3 userName asc) (4 userName desc) (default userId)}";
        replaceSql = parser.convertToJdbcSql(sql, obj);
        assertThat(replaceSql, is("select * from (select * from user ORDER BY userId desc) where rownum <= :number ORDER BY userName asc"));
    }

    /**
     * {@link SqlParameterParser#convertToJdbcSql(String, Object)}のORDER BY句用テスト。<br>
     * 可変条件をもつSQL文のテスト(引数がMapのパターン)
     */
    @Test
    public void testParseVariableConditionForMapOrderByStatementByObject() {
        BasicSqlParameterParser parser = new BasicSqlParameterParser();
        parser.setLikeEscapeChar('\\');

        Map<String, Object> obj = new HashMap<String, Object>();
        
        // 可変ORDER BY構文が含まれない場合
        String sql = "select * from user where userId = :userId order by userId";
        String replaceSql = parser.convertToJdbcSql(sql, obj);
        assertThat(replaceSql, is("select * from user where userId = :userId order by userId"));
        
        // 可変ORDER BY構文のケースにヒットする場合(1つ目)
        obj = new HashMap<String, Object>();
        obj.put("sortId", Integer.valueOf(1));
        
        sql = "select * from user where userId = :userId $sort(sortId) {(1 userId asc) (2 userId desc) (3 userName asc) (4 userName desc) (default userId)}";
        replaceSql = parser.convertToJdbcSql(sql, obj);
        assertThat(replaceSql, is("select * from user where userId = :userId ORDER BY userId asc"));
        
        // 可変ORDER BY構文のケースにヒットする場合(2つ目)
        obj = new HashMap<String, Object>();
        obj.put("sortId", Integer.valueOf(2));
        
        sql = "select * from user where userId = :userId $sort(sortId) {(1 userId asc) (2 userId desc) (3 userName asc) (4 userName desc) (default userId)}";
        replaceSql = parser.convertToJdbcSql(sql, obj);
        assertThat(replaceSql, is("select * from user where userId = :userId ORDER BY userId desc"));
        
        // 可変ORDER BY構文のケースにヒットする場合(4つ目)
        obj = new HashMap<String, Object>();
        obj.put("sortId", Integer.valueOf(4));
        
        sql = "select * from user where userId = :userId $sort(sortId) {(1 userId asc) (2 userId desc) (3 userName asc) (4 userName desc) (default userId)}";
        replaceSql = parser.convertToJdbcSql(sql, obj);
        
        // 可変ORDER BY構文のケースにヒットしない場合(defaultあり)
        obj = new HashMap<String, Object>();
        obj.put("sortId", Integer.valueOf(5));
        
        sql = "select * from user where userId = :userId $sort(sortId) {(1 userId asc) (2 userId desc) (3 userName asc) (4 userName desc) (default userId)}";
        replaceSql = parser.convertToJdbcSql(sql, obj);
        assertThat(replaceSql, is("select * from user where userId = :userId ORDER BY userId"));

        // 可変ORDER BY構文のケースにヒットしない場合(defaultあり、かつパラメータの値がnull)
        obj = new HashMap<String, Object>();
        obj.put("sortId", null);
        
        sql = "select * from user where userId = :userId $sort(sortId) {(1 userId asc) (2 userId desc) (3 userName asc) (4 userName desc) (default userId)}";
        replaceSql = parser.convertToJdbcSql(sql, obj);
        assertThat(replaceSql, is("select * from user where userId = :userId ORDER BY userId"));

        // 可変ORDER BY構文のケースにヒットしない場合(defaultなし)
        obj = new HashMap<String, Object>();
        obj.put("sortId", Integer.valueOf(5));
        
        sql = "select * from user where userId = :userId $sort(sortId) {(1 userId asc) (2 userId desc) (3 userName asc) (4 userName desc)}";
        replaceSql = parser.convertToJdbcSql(sql, obj);
        assertThat(replaceSql, is("select * from user where userId = :userId "));

        // 可変ORDER BY構文のケースにヒットしない場合(defaultなし、かつパラメータの値がnull)
        obj = new HashMap<String, Object>();
        obj.put("sortId", null);
        
        sql = "select * from user where userId = :userId $sort(sortId) {(1 userId asc) (2 userId desc) (3 userName asc) (4 userName desc)}";
        replaceSql = parser.convertToJdbcSql(sql, obj);
        assertThat(replaceSql, is("select * from user where userId = :userId "));

        // 存在しないパラメータ名の場合
        obj = new HashMap<String, Object>();
        obj.put("sortId", Integer.valueOf(5));
        
        sql = "select * from user where userId = :userId $sort(unknown) {(1 userId asc) (2 userId desc) (3 userName asc) (4 userName desc)}";
        try {
            parser.convertToJdbcSql(sql, obj);
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("there is not sql parameter 'unknown' in the key of the Map."));
        }

        // SELECT句で$sort構文を使用しているため不正
        obj = new HashMap<String, Object>();
        obj.put("sortId", Integer.valueOf(1));
        
        sql = "select $sort(sortId) {(1 userId asc) (2 userId desc) (3 userName asc) (4 userName desc) (default userId) from user where userId = :userId}";
        replaceSql = parser.convertToJdbcSql(sql, obj);
        assertThat(replaceSql, is("select ORDER BY userId asc"));

        // WHERE句で$sort構文を使用しているため不正
        obj = new HashMap<String, Object>();
        obj.put("sortId", Integer.valueOf(1));
        
        sql = "select * from user where $sort(sortId) {(1 userId asc) (2 userId desc) (3 userName asc) (4 userName desc) (default userId) from user where userId = :userId}";
        replaceSql = parser.convertToJdbcSql(sql, obj);
        assertThat(replaceSql, is("select * from user where ORDER BY userId asc"));

        // 可変ORDER BY構文を複数使用している場合
        obj = new HashMap<String, Object>();
        obj.put("sortId1", Integer.valueOf(3));
        obj.put("sortId2", Integer.valueOf(2));
        
        sql = "select * from ("
                + "select * from user $sort(sortId1) {(1 userId asc) (2 userId desc) (3 userName asc) (4 userName desc) (default userId)}"
            + ") where rownum <= :number $sort(sortId2) {(1 userId asc) (2 userId desc) (3 userName asc) (4 userName desc) (default userId)}";
        replaceSql = parser.convertToJdbcSql(sql, obj);
        assertThat(replaceSql, is("select * from (select * from user ORDER BY userName asc) where rownum <= :number ORDER BY userId desc"));
    }

    /**
     * 全てのSQL文の拡張構文を使用した場合。
     */
    @Test
    public void testMixedSyntaxForObject() {

        BasicSqlParameterParser parser = new BasicSqlParameterParser();
        parser.setLikeEscapeChar('\\');

        // Object指定
        Map<String, Object> obj = new HashMap<String, Object>();
        obj.put("hoge", new String[1]);
        obj.put("hoge1", new ArrayList<String>() { { add("a"); add("b"); } });
        obj.put("hoge2", new String[3]);
        obj.put("hoge3", new String[0]);
        obj.put("hoge4", new ArrayList<String>());
        obj.put("hoge5", "2");


        String sql = "select * from hoge where $if (hoge) {hoge1 in (:hoge[])} and $if (hoge4) {hoge4 in (:hoge4[])} "
                   + "$sort (hoge5) {(1 hoge1) (2 hoge2) (3 hoge4) (default foo)}";
        
        String result = parser.convertToJdbcSql(sql, obj);
        // :hoge4[]は、置き換えられない。
        assertThat(result,
                is("select * from hoge where (0 = 1 or (hoge1 in (:hoge[0]))) and (0 = 0 or (hoge4 in (:hoge4[])))"
                 + " ORDER BY hoge2"));

        obj = new HashMap<String, Object>();
        obj.put("hoge", new String[1]);
        obj.put("hoge1", new ArrayList<String>() { { add("a"); add("b"); } });
        obj.put("hoge2", new String[3]);
        obj.put("hoge3", new String[0]);
        obj.put("hoge4", new ArrayList<String>());
        obj.put("hoge5", null);

        result = parser.convertToJdbcSql(sql, obj);
        // :hoge4[]は、置き換えられない。
        assertThat(result,
                is("select * from hoge where (0 = 1 or (hoge1 in (:hoge[0]))) and (0 = 0 or (hoge4 in (:hoge4[])))"
                 + " ORDER BY foo"));
    }
    
    /**
     * 全てのSQL文の拡張構文を使用した場合。
     */
    @Test
    public void testMixedSyntaxForMap() {

        BasicSqlParameterParser parser = new BasicSqlParameterParser();
        parser.setLikeEscapeChar('\\');

        // Map指定
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("hoge", new String[1]);
        map.put("hoge1", new ArrayList<String>() { { add("a"); add("b"); } });
        map.put("hoge2", new String[3]);
        map.put("hoge3", new String[0]);
        map.put("hoge4", new ArrayList<String>());
        map.put("hoge5", "2");
        
        String sql = "select * from hoge where $if (hoge) {hoge1 in (:hoge[])} and $if (hoge4) {hoge4 in (:hoge4[])} "
                   + "$sort (hoge5) {(1 hoge1) (2 hoge2) (3 hoge4) (default foo)}";
        
        String result = parser.convertToJdbcSql(sql, map);
        // :hoge4[]は、置き換えられない。
        assertThat(result,
                is("select * from hoge where (0 = 1 or (hoge1 in (:hoge[0]))) and (0 = 0 or (hoge4 in (:hoge4[])))"
                 + " ORDER BY hoge2"));
        
        map.put("hoge5", null);
        result = parser.convertToJdbcSql(sql, map);
        // :hoge4[]は、置き換えられない。
        assertThat(result,
                is("select * from hoge where (0 = 1 or (hoge1 in (:hoge[0]))) and (0 = 0 or (hoge4 in (:hoge4[])))"
                 + " ORDER BY foo"));
    }
    
    /**
     * {@link SqlConvertor}のリストをカスタマイズして動作すること。
     */
    @Test
    public void testCustomizeSqlConvertors() {
        
        List<SqlConvertor> convertors = new ArrayList<SqlConvertor>() {
            {
                add(new VariableInSyntaxConvertor());
                add(new UpperCaseSqlConvertor());
            }
        };

        BasicSqlParameterParser parser = new BasicSqlParameterParser();
        parser.setLikeEscapeChar('\\');

        parser.setSqlConvertors(convertors);
        
        // Map指定
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("hoge", new String[1]);
        map.put("hoge1", new ArrayList<String>() { { add("a"); add("b"); } });
        map.put("hoge2", new String[3]);
        map.put("hoge3", new String[0]);
        map.put("hoge4", new ArrayList<String>());
        map.put("hoge5", "2");
        
        String sql = "select * from hoge where $if (hoge) {hoge1 in (:hoge[])} and $if (hoge4) {hoge4 in (:hoge4[])} "
                   + "$sort (hoge5) {(1 hoge1) (2 hoge2) (3 hoge4) (default foo)}";
        
        String result = parser.convertToJdbcSql(sql, map);
        // IN構文と大文字変換以外は、置き換えられない。
        assertThat(result,
                is("SELECT * FROM HOGE WHERE $IF (HOGE) {HOGE1 IN (:HOGE[0])} AND $IF (HOGE4) {HOGE4 IN (:HOGE4[])} "
                 + "$SORT (HOGE5) {(1 HOGE1) (2 HOGE2) (3 HOGE4) (DEFAULT FOO)}"));
        
        map.put("hoge5", null);
        result = parser.convertToJdbcSql(sql, map);
        // IN構文と大文字変換以外は、置き換えられない。
        assertThat(result,
                is("SELECT * FROM HOGE WHERE $IF (HOGE) {HOGE1 IN (:HOGE[0])} AND $IF (HOGE4) {HOGE4 IN (:HOGE4[])} "
                 + "$SORT (HOGE5) {(1 HOGE1) (2 HOGE2) (3 HOGE4) (DEFAULT FOO)}"));
    }
    
    public static class Entity {

        private String userId;

        private String userName;

        private java.sql.Date date;
        
        private Integer sortId;
        private Integer sortId1;
        private Integer sortId2;

        public String getUserId() {
            return userId;
        }

        public String getUserName() {
            return userName;
        }

        public Date getDate() {
            return date;
        }

        public Integer getSortId() {
            return sortId;
        }

        public Integer getSortId1() {
            return sortId1;
        }

        public Integer getSortId2() {
            return sortId2;
        }
    }
    
    private static class UpperCaseSqlConvertor implements SqlConvertor {

        public String convert(
                String sql,
                Object obj) {
            return sql.toUpperCase();
        }
        
    }
}
