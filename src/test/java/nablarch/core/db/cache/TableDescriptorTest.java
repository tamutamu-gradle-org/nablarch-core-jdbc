package nablarch.core.db.cache;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/**
 * @author ryo asato
 */
public class TableDescriptorTest {

    private String tableName = "TEST";

    private Map<String, ColumnDescriptor> columnDescriptorMap;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testCaseSensitive() {
        columnDescriptorMap = new HashMap<String, ColumnDescriptor>();
        columnDescriptorMap.put("TEST_COLUMN", new ColumnDescriptor("TEST_COLUMN", Types.VARCHAR));

        // 大文字小文字を区別しない場合
        TableDescriptor sut = new TableDescriptor(tableName, false, columnDescriptorMap);
        ColumnDescriptor actual = sut.getColumnDescriptor("TEST_COLUMN");
        assertThat("大文字を指定", actual.getColumnType(), is(Types.VARCHAR));
        actual = sut.getColumnDescriptor("test_column");
        assertThat("小文字を指定", actual.getColumnType(), is(Types.VARCHAR));

        // 大文字小文字を区別する場合
        sut = new TableDescriptor(tableName, true, columnDescriptorMap);
        actual = sut.getColumnDescriptor("TEST_COLUMN");
        assertThat("大文字を指定", actual.getColumnType(), is(Types.VARCHAR));
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("column not found. column: test_column");
        sut.getColumnDescriptor("test_column");
    }
}
