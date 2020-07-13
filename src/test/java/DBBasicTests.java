import com.mixer.dbserver.DB;
import com.mixer.dbserver.DBServer;
import com.mixer.raw.Index;
import com.mixer.raw.Person;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class DBBasicTests {
    private DB db;
    private String dbFileName = "testdb.db";


    @Test
    public void testAdd() {
        try {
            this.db.add("John", 44, "Berlin", "www-404", "This is a description");
            Assert.assertEquals(Index.getInstance().getTotalNumberOfRows(), 1);
        } catch (IOException e) {
            Assert.fail();
        }
    }

    @Test
    public void testRead() {
        try {
            this.db.add("John", 44, "Berlin", "www-404", "This is a description");
            Assert.assertEquals(Index.getInstance().getTotalNumberOfRows(), 1);
            Person person = this.db.read(0);
            Assert.assertEquals(person.name, "John");
            Assert.assertEquals(person.address, "Berlin");
            Assert.assertEquals(person.carPlateNumber, "www-404");
            Assert.assertEquals(person.description, "This is a description");


        } catch (IOException e) {
            Assert.fail();
        }
    }

    @Test
    public void testDelete() {
        try {
            this.db.add("John", 44, "Berlin", "www-404", "This is a description");
            Assert.assertEquals(Index.getInstance().getTotalNumberOfRows(), 1);
            this.db.delete(0);
            Assert.assertEquals(Index.getInstance().getTotalNumberOfRows(), 0);

        } catch (IOException e) {
            Assert.fail();
        }
    }

    @Before
    public void setup() throws IOException {
        File file = new File(dbFileName);
        if (file.exists())
            file.delete();
        this.db = new DBServer(dbFileName);
    }

    @After
    public void after() {
        try {
            this.db.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
