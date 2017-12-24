package hsqldbinvestigation;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class MyTest {

    private Connection cn;

    @Before
    public void setUp() throws Exception {
        cn = DriverManager.getConnection("jdbc:hsqldb:file:target/testdb", "SA", "");
        cn.setAutoCommit(false);
        try (Statement st = cn.createStatement()) {
            st.executeUpdate(
                    "DECLARE LOCAL TEMPORARY TABLE MY_TABLE " +
                            "(COL1 BIGINT PRIMARY KEY," +
                            " COL2 FLOAT," +
                            " COL3 FLOAT," +
                            " COL4 FLOAT)");

            st.executeUpdate("INSERT INTO MY_TABLE (COL1, COL2, COL3, COL4) VALUES (" +
                    "1," +
                    "2.0," +
                    "NULL," +
                    "4.0)");
        }
    }

    @Test
    public void name() throws Exception {
        try (Statement st = cn.createStatement();
             ResultSet rs = st.executeQuery("SELECT * FROM MY_TABLE")) {
            rs.next();

            final Long col1 = rs.getObject("COL1", Long.class);
            final Double col2 = rs.getObject("COL2", Double.class);
            final Double col3 = rs.getObject("COL3", Double.class);
            final Double col4 = rs.getObject("COL4", Double.class);

            // This yields: col1=1, col2=2.0, col3=0.0, col4=null
            System.out.println("col1=" + col1 + ", col2=" + col2 + ", col3=" + col3 + ", col4=" + col4);

            // I expect the following but...
            assertThat(col1, is(1L));
            assertThat(col2, is(2D));
            assertThat(col3, is(nullValue())); // this fails
            assertThat(col4, is(4D)); // this fails as well
        }
    }

    @After
    public void tearDown() throws Exception {
        cn.rollback();
        cn.close();
    }
}
