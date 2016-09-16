package nablarch.core.db.statement.sqlconvertor;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.Test;

/**
 * {@link VariableConditionSyntaxConvertor}のテストクラス。
 */
public class VariableConditionSyntaxConvertorTest {

    private VariableConditionSyntaxConvertor sut = new VariableConditionSyntaxConvertor();

    public static class TestBean {

        private String prop = "value";
        private Set<Integer> integerSet;
        private List<String> stringList;
        private Long[] longArray;

        public String getProp() {
            return prop;
        }

        public Set<Integer> getIntegerSet() {
            return integerSet;
        }

        public List<String> getStringList() {
            return stringList;
        }

        public Long[] getLongArray() {
            return longArray;
        }
    }

    /**
     * $if特殊構文を持たないSQLの場合、変換はされないこと。
     */
    @Test
    public void testHasNotIfCondition() throws Exception {
        final String result = sut.convert("select * from dual", new Object());
        assertThat(result, is("select * from dual"));
    }

    /**
     * $if構文のプロパティに値が含まれる場合、条件に含まれる形式に変換されること。
     *
     * {@code 0 = 1 or}が付加されることで条件に含まれること。
     */
    @Test
    public void testHasValue() throws Exception {
        final String result = sut.convert("$if(prop) {column = :prop}", new TestBean());
        assertThat(result, is("(0 = 1 or (column = :prop))"));
    }

    /**
     * $if構文のプロパティがnullの場合、条件に含まれない形式に変換されること。
     *
     * {@code 0 = 0 or}が付加されることで、該当条件が除外されること。
     */
    @Test
    public void testHasNullValue() throws Exception {
        final TestBean bean = new TestBean();
        bean.prop = null;
        final String result = sut.convert("where col = ? and $if(prop) {column = :prop} and col = ?", bean);
        assertThat(result, is("where col = ? and (0 = 0 or (column = :prop)) and col = ?"));
    }

    /**
     * $if構文のプロパティが空文字列の場合、条件に含まれない形式に変換されること。
     *
     * {@code 0 = 0 or}が付加されることで、該当条件が除外されること。
     */
    @Test
    public void testHasEmptyString() throws Exception {
        final TestBean bean = new TestBean();
        bean.prop = "";
        final String result = sut.convert("where col = ? and $if(prop) {column = :prop} and col = ?", bean);
        assertThat(result, is("where col = ? and (0 = 0 or (column = :prop)) and col = ?"));
    }

    /**
     * $if構文のプロパティがCollectionで要素を保つ場合、条件に含まれる形式に変換されること。
     *
     * {@code 0 = 1 or}が付加されることで条件に含まれること。
     */
    @Test
    public void testCollection() throws Exception {
        final TestBean bean = new TestBean();
        bean.integerSet = Collections.singleton(100);
        final String result = sut.convert("where col = ? and $if(integerSet) {column = :integerSet[0]} and col = ?", bean);
        assertThat(result, is("where col = ? and (0 = 1 or (column = :integerSet[0])) and col = ?"));
    }

    /**
     * デフォルト動作では、コレクション内の要素が空文字列でも条件に含まれること。
     */
    @Test
    public void testCollectionAndHasEmptyString() throws Exception {
        final TestBean bean = new TestBean();
        bean.stringList = Collections.singletonList("");

        final String result = sut.convert("where col = ? and $if(stringList) {column = :stringList[0]} and col = ?", bean);
        assertThat(result, is("where col = ? and (0 = 1 or (column = :stringList[0])) and col = ?"));
    }

    /**
     * 空文字列を許容しない場合で、コレクション内の要素が空文字列の場合条件から除外されること。
     */
    @Test
    public void testCollectionAndNotAllowEmptyString_EmptyString() throws Exception {
        final TestBean bean = new TestBean();
        bean.stringList = Collections.singletonList("");

        sut.setAllowArrayEmptyString(false);

        final String result = sut.convert("where col = ? and $if(stringList) {column = :stringList[0]} and col = ?", bean);
        assertThat(result, is("where col = ? and (0 = 0 or (column = :stringList[0])) and col = ?"));
    }

    /**
     * 空文字列を許容しない場合で、コレクション内の要素がnullの場合条件から除外されること。
     */
    @Test
    public void testCollectionAndNotAllowEmptyString_Null() throws Exception {
        final TestBean bean = new TestBean();
        bean.stringList = Collections.singletonList(null);
        sut.setAllowArrayEmptyString(false);

        final String result = sut.convert("where col = ? and $if(stringList) {column = :stringList[0]} and col = ?", bean);
        assertThat(result, is("where col = ? and (0 = 0 or (column = :stringList[0])) and col = ?"));
    }

    /**
     * 空文字列を許容しない場合で、コレクション内の要素が値を持つ場合条件にふくまれること
     */
    @Test
    public void tetCollectionAndNotAllowEmptyString_HasValue() throws Exception {
        sut.setAllowArrayEmptyString(false);
        final TestBean bean = new TestBean();
        bean.stringList = Collections.singletonList("1");

        final String result = sut.convert("where col = ? and $if(stringList) {column = :stringList[0]} and col = ?", bean);
        assertThat(result, is("where col = ? and (0 = 1 or (column = :stringList[0])) and col = ?"));
    }

    /**
     * 空のコレクションの場合、条件に含まれないこと
     */
    @Test
    public void testCollectionEmpty() throws Exception {
        final TestBean bean = new TestBean();
        bean.stringList = Collections.emptyList();

        final String result = sut.convert("where col = ? and $if(stringList) {column = :stringList[0]} and col = ?", bean);
        assertThat(result, is("where col = ? and (0 = 0 or (column = :stringList[0])) and col = ?"));
    }

    /**
     * コレクションで複数要素を保つ場合、条件に含まれること。
     */
    @Test
    public void testCollectionMultiElement() throws Exception {
        final TestBean bean = new TestBean();
        bean.stringList = Arrays.asList("1", "2");

        final String result = sut.convert("where col = ? and $if(stringList) {column = :stringList[0]} and col = ?", bean);
        assertThat(result, is("where col = ? and (0 = 1 or (column = :stringList[0])) and col = ?"));
    }

    /**
     * 複数の$if文がある場合でも正しく処理されること
     */
    @Test
    public void testMultipleIfCondition() throws Exception {
        final TestBean bean = new TestBean();
        bean.longArray = new Long[] {1L};
        bean.prop = null;

        final String result = sut.convert("where $if (longArray) {prop1 = :longArray} and name = :name and $if (prop) {prop2 = :prop}", bean);
        assertThat(result, is("where (0 = 1 or (prop1 = :longArray)) and name = :name and (0 = 0 or (prop2 = :prop))"));
    }
}
