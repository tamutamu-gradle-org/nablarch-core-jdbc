package nablarch.core.db.statement;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayInputStream;

import org.hamcrest.text.IsEqualIgnoringWhiteSpace;

import nablarch.core.db.statement.BatchParameterHolder.NopBatchParamHolder;

import org.junit.Test;

public class BatchParameterHolderTest {

    @Test
    public void testBatchParameterHolder() {
        BatchParameterHolder holder = new BatchParameterHolder();
        {
            ParameterHolder params = new ParameterHolder();
            params.add(1, "one");
            params.add(2, 2);
            params.add(3, new byte[]{0x33});
            params.add(4, new ByteArrayInputStream(new byte[0]));
            holder.add(params);
        }
        {
            ParameterHolder params = new ParameterHolder();
            params.add("a", "one");
            params.add("b", 2);
            params.add("c", new byte[]{0x33});
            params.add("d", new ByteArrayInputStream(new byte[0]));
            holder.add(params);
        }

        String s = holder.toString();
        String expected = "\n\tbatch count = [1]\n" +
                "\t\t01 = [one]\n" +
                "\t\t02 = [2]\n" +
                "\t\t03 = [bytes]\n" +
                "\t\t04 = [InputStream]\n" +
                "\tbatch count = [2]\n" +
                "\t\ta = [one]\n" +
                "\t\tb = [2]\n" +
                "\t\tc = [bytes]\n" +
                "\t\td = [InputStream]";
        assertThat(s, IsEqualIgnoringWhiteSpace.equalToIgnoringWhiteSpace(expected));

        assertThat(holder.size(), is(2));
        holder.clear();
        assertThat(holder.size(), is(0));
    }


    @Test
    public void testNopBatchParamHolder() {
        NopBatchParamHolder nop = NopBatchParamHolder.getInstance();
        nop.add(new ParameterHolder());
        assertThat(nop.size(), is(0));

    }
}
