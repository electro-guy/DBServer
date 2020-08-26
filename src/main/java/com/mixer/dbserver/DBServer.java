package com.mixer.dbserver;

import com.mixer.exceptions.DuplicateNameException;
import com.mixer.raw.FileHandler;
import com.mixer.raw.Index;
import com.mixer.raw.Person;
import com.mixer.util.DebugRowInfo;

import java.io.IOException;
import java.util.List;

public final class DBServer implements DB {
    private FileHandler fileHandler;

    public DBServer(final String dbFileName) throws IOException {
        this.fileHandler = new FileHandler(dbFileName);
        this.fileHandler.loadAllDataToIndex();
    }

    @Override
    public void close() throws IOException {
        Index.getInstance().clear();
        this.fileHandler.close();
    }

    @Override
    public void add(Person person) throws IOException, DuplicateNameException {
        this.fileHandler.add(person.name, person.age, person.address, person.carPlateNumber, person.description);

    }

    @Override
    public void delete(long rowNumber) throws IOException {
        if (rowNumber < 0)
            throw new IOException("Row Number is less than 0");
        this.fileHandler.deleteRow(rowNumber);
    }

    @Override
    public void update(long rowNumber, final Person person) throws IOException, DuplicateNameException {
        this.fileHandler.update(rowNumber, person.name, person.age, person.address, person.carPlateNumber, person.description);

    }

    @Override
    public void update(String name, Person person) throws IOException, DuplicateNameException {
        this.fileHandler.update(name,person.name, person.age, person.address, person.carPlateNumber, person.description);
    }

    @Override
    public Person read(long rowNumber) throws IOException {
        return this.fileHandler.readRow(rowNumber);
    }

    public List<DebugRowInfo> listAllRowsWithDebug() throws IOException {
        return this.fileHandler.loadAllDataFromFile();

    }

}
