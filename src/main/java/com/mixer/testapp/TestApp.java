package com.mixer.testapp;

import com.mixer.dbserver.DB;
import com.mixer.dbserver.DBServer;
import com.mixer.raw.Index;
import com.mixer.raw.Person;
import com.mixer.util.DebugRowInfo;

import java.util.List;

public class TestApp {
    final static String dbFile = "Dbserver.db";

    public static void main(String args[]) {
        new TestApp().performTest();
    }

    private void performTest() {
        try {
            fillDB(10);
            delete(0);
            delete(2);
            delete(5);
            listAllRecords();

        }catch(Exception e) {
            e.printStackTrace();
        }
    }

    void listAllRecords() throws Exception {
        DBServer db = new DBServer(dbFile);
        List<DebugRowInfo> result = db.listAllRowsWithDebug();
        System.out.println("Total row number: " + Index.getInstance().getTotalNumberOfRows());
        for (DebugRowInfo dri : result) {
            prettyPrintRow(dri);
        }
        db.close();
    }

    private void prettyPrintRow(DebugRowInfo dri) {
        Person p = dri.person();
        boolean isDeleted = dri.isDeleted();
        String debugChar = isDeleted?"-":"+";
        String s = String.format(" %s name: %s, age: %d, address: %s, carplatenum: %s, desc: %s", debugChar,
                p.name,
                p.age,
                p.address,
                p.carPlateNumber,
                p.description);
        System.out.println(s);
    }

    void delete(int number) throws Exception {

        DB db = new DBServer(dbFile);
        db.delete(number);
        db.close();
    }

    void fillDB(int rowNumber) throws Exception {

        DB db = new DBServer(dbFile);
        for (int i = 0; i < rowNumber; i++) {
            Person p = new Person("John" + i, 44, "Berlin", "www-404", "This is a description");
            db.add(p);
        }
        db.close();
    }
}
