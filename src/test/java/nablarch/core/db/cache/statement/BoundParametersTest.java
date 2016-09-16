package nablarch.core.db.cache.statement;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.matchers.JUnitMatchers.containsString;

import java.util.HashMap;

import nablarch.core.db.statement.ParameterHolder;
import nablarch.core.db.statement.ParameterHolder.ParamValue;

import org.junit.Test;

/**
 * {@link BoundParameters}のテストクラス。
 *
 * @author T.Kawasaki
 */
public class BoundParametersTest {

    /** null、異なるクラスと等価と判定されないこと。 */
    @Test
    public void testEqualsFail() {
        BoundParameters target = new BoundParameters(new HashMap<String, ParamValue>());
        assertThat(target.equals(null), is(false));
        assertThat(target.equals(new Object()), is(false));
    }

    /** 文字列表現のテスト */
    @Test
    public void testToString() {
        ParameterHolder holder = new ParameterHolder();
        holder.add("name", "yamada");
        holder.add("addr", "tokyo");

        BoundParameters target = new BoundParameters(holder);
        String actual = target.toString();
        assertThat(actual, containsString("name=yamada"));
        assertThat(actual, containsString("addr=tokyo}"));
    }
}
