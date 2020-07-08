package com.mixer.testapp;

import com.mixer.dbserver.DB;
import com.mixer.dbserver.DBServer;
import com.mixer.raw.Index;
import com.mixer.raw.Person;

public class TestApp {

    public static void main(String args[]) {
        try {
            final String dbFile = "DbServer.db";
            DB db = new DBServer(dbFile);
            db.add("John", 44, "Berlin", "www-404", "This is a description");
            db.close();

            db = new DBServer(dbFile);
            Person person = db.read(0);


            System.out.println("Total number of rows in database: " + Index.getInstance().getTotalNumberOfRows());
            System.out.println(person);

            System.out.println(Index.getInstance().getTotalNumberOfRows());
            db.close();

        }catch(Exception e) {
            e.printStackTrace();
        }
    }
}
