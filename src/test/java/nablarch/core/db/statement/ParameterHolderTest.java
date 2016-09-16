package nablarch.core.db.statement;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.util.Map;

import nablarch.core.db.statement.ParameterHolder.BytesValue;
import nablarch.core.db.statement.ParameterHolder.InputStreamValue;
import nablarch.core.db.statement.ParameterHolder.NopParameterHolder;
import nablarch.core.db.statement.ParameterHolder.ObjectValue;
import nablarch.core.db.statement.ParameterHolder.ParamValue;

import org.junit.Test;

/**
 * {@link ParameterHolder}のテストクラス。
 *
 * @author T.Kawasaki
 */
public class ParameterHolderTest {

    /** テスト対象 */
    private ParameterHolder target = new ParameterHolder();

    /** インデックス指定のパラメータを追加できること。 */
    @Test
    public void testAddIndex() {
        target.add(1, "one");
        target.add(2, 2);
        target.add(3, new byte[] { 0x33 });
        target.add(4, new ByteArrayInputStream(new byte[0]));
        target.add(5, new BigDecimal("0.000000000001"));

        Map<String, ParamValue> params = target.getParameters();
        assertThat(params.get("01").toString(), is("one"));
        assertThat(params.get("02").toString(), is("2"));
        assertThat(params.get("03").toString(), is("bytes"));
        assertThat(params.get("04").toString(), is("InputStream"));
        assertThat(params.get("05").toString(), is("0.000000000001"));

        String s = target.toString();
        assertThat(s, containsString("01 = [one]"));
        assertThat(s, containsString("02 = [2]"));
        assertThat(s, containsString("03 = [bytes]"));
        assertThat(s, containsString("04 = [InputStream]"));
        assertThat(s, containsString("05 = [0.000000000001]"));

    }

    /** 名前つきパラメータを追加できること。 */
    @Test
    public void testAddNamed() {
        target.add("a", "one");
        target.add("b", 2);
        target.add("c", new byte[] { 0x33 });
        target.add("d", new ByteArrayInputStream(new byte[0]));
        target.add("e", new BigDecimal("0.000000000001"));

        Map<String, ParamValue> params = target.getParameters();
        assertThat(params.get("a").toString(), is("one"));
        assertThat(params.get("b").toString(), is("2"));
        assertThat(params.get("c").toString(), is("bytes"));
        assertThat(params.get("d").toString(), is("InputStream"));
        assertThat(params.get("e").toString(), is("0.000000000001"));


        String s = target.toString();
        assertThat(s, containsString("a = [one]"));
        assertThat(s, containsString("b = [2]"));
        assertThat(s, containsString("c = [bytes]"));
        assertThat(s, containsString("d = [InputStream]"));
        assertThat(s, containsString("e = [0.000000000001]"));
    }

    /** オブジェクトの等価判定テスト。*/
    @Test
    public void testObjectValueEquals() {
        ObjectValue v = new ObjectValue("1");
        assertThat("等値", v.equals(v), is(true));
        assertThat("null",v.equals(null), is(false));
        assertThat("異なるクラス",v.equals(new Object()), is(false));
        assertThat("等価でない", v.equals(new ObjectValue("2")), is(false));
        assertThat("等価である", v.equals(new ObjectValue("1")), is(true));
    }

    /** オブジェクトの等価判定テスト（包含する値がnullの場合） */
    @Test
    public void testObjectValueHasNull() {
        ObjectValue nullValue = new ObjectValue(null);
        assertThat("等価である", nullValue.equals(new ObjectValue(null)), is(true));
        assertThat("等価でない", nullValue.equals(new ObjectValue(new Object())), is(false));
        assertThat(nullValue.hashCode(), is(0));
    }

    /** byte[]の等価判定テスト。*/
    @Test
    public void testBytesValueEquals() {
        BytesValue v = new BytesValue(new byte[] {0x30, 0x31});
        assertThat("等値", v.equals(v), is(true));
        assertThat("null", v.equals(null), is(false));
        assertThat("異なるクラス", v.equals(new Object()), is(false));
        assertThat("等価でない", v.equals(new BytesValue(new byte[] {0x31, 0x32})), is(false));
        assertThat("等価である", v.equals(new BytesValue(new byte[] {0x30, 0x31})), is(true));
    }

    /** byte[]の等価判定テスト（包含する値がnullの場合） */
    @Test
    public void testBytesValueHasNull() {
        BytesValue nullValue = new BytesValue(null);
        assertThat("等価である", nullValue.equals(new BytesValue(null)), is(true));
        assertThat("等価でない", nullValue.equals(new BytesValue(new byte[0])), is(false));
        assertThat(nullValue.hashCode(), is(0));
    }

    /** InputStreamの等価判定テスト。*/
    @Test
    public void testInputStreamValueEquals() {
        InputStreamValue v = new InputStreamValue();
        assertThat("等値", v.equals(v), is(true));
        assertThat("null", v.equals(null), is(false));
        assertThat("等価でない", v.equals(new Object()), is(false));
        assertThat("等価でない", v.equals(new InputStreamValue()), is(false));
    }

    /** {@link NopParameterHolder}のテスト */
    @Test
    public void testNopParamHolder() {
        NopParameterHolder nop = NopParameterHolder.getInstance();
        nop.add(1, 1);
        nop.add("a", "a");
        assertThat("パラメータが追加されないこと。",
                   nop.getParameters().isEmpty(), is(true));
    }

}