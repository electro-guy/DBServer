package com.mixer.testapp;

import com.mixer.dbserver.DB;
import com.mixer.dbserver.DBServer;
import com.mixer.raw.Index;

public class TestApp {

    public static void main(String args[]) {
        try {
            final String dbFile = "Dbserver.db";
            DB db = new DBServer(dbFile);
            db.add("John", 44, "Berlin", "www-404", "This is a description");
            System.out.println("Total number of rows in database: " + Index.getInstance().getTotalNumberOfRows());

            db.delete(0);

            System.out.println(Index.getInstance().getTotalNumberOfRows());
            db.close();

        }catch(Exception e) {
            e.printStackTrace();
        }
    }
}
