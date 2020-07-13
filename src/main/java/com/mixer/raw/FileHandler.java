package com.mixer.raw;

import java.io.*;

public class FileHandler {
    private RandomAccessFile dbFile;

    public FileHandler(final String dbFileName) throws FileNotFoundException {
        this.dbFile = new RandomAccessFile(dbFileName, "rw");
    }

    public boolean add(String name,
                       int age,
                       String address,
                       String carPlateNumber,
                       String description) throws IOException {
        // seek to the end of the file
        long currentPositionToInsert = this.dbFile.length();
        this.dbFile.seek(currentPositionToInsert);
        // isDeleted byte
        // record length : int
        // name length : int
        // name
        // address length : int
        // address
        // carplatenumber length : int
        // carplatenumber
        // description length : int
        // description

        // calculate record length
        int length = 4 + // name length
                     name.length() +
                     4 + // age
                     4 + // address
                     address.length() +
                     4 + // carplate length
                     carPlateNumber.length() +
                     4 + // description
                     description.length();

        // it is deleted
        this.dbFile.writeBoolean(false);
        // record length
        this.dbFile.writeInt(length);

        // store the name
        this.dbFile.writeInt(name.length());
        this.dbFile.write(name.getBytes());

        // store age
        this.dbFile.writeInt(age);

        // store the address
        this.dbFile.writeInt(address.length());
        this.dbFile.write(address.getBytes());

        // store the carplatenumber
        this.dbFile.writeInt(carPlateNumber.length());
        this.dbFile.write(carPlateNumber.getBytes());

        // store the description
        this.dbFile.writeInt(description.length());
        this.dbFile.write(description.getBytes());

        Index.getInstance().add(currentPositionToInsert);

        return true;
    }

    public Person readRow(int rowNumber) throws IOException {
        long bytePosition = Index.getInstance().getBytePosition(rowNumber);
        if (bytePosition == -1) {
            return null;
        }

        byte[] row = this.readRawRecord(bytePosition);
        Person person = new Person();
        DataInputStream stream = new DataInputStream(new ByteArrayInputStream(row));

        int nameLength = stream.readInt();
        byte[] b = new byte[nameLength];
        stream.read(b);
        person.name = new String(b);

        // age
        person.age = stream.readInt();

        // address
        b = new byte[stream.readInt()];
        stream.read(b);
        person.address = new String(b);

        // carplatenum
        b = new byte[stream.readInt()];
        stream.read(b);
        person.carPlateNumber = new String(b);

        // description
        b = new byte[stream.readInt()];
        stream.read(b);
        person.description = new String(b);

        return person;

    }

    private byte[] readRawRecord(long bytePositionOfRow) throws IOException {
        this.dbFile.seek(bytePositionOfRow);
        if (this.dbFile.readBoolean())
            return new byte[0];
        this.dbFile.seek(bytePositionOfRow + 1);
        int recordLength = this.dbFile.readInt();
        this.dbFile.seek(bytePositionOfRow + 5);

        byte[] data = new byte[recordLength];
        this.dbFile.read(data);

        return data;
    }

    public void loadAllDataToIndex() throws IOException {
        if (this.dbFile.length() == 0)
            return;

        long currentPos = 0;
        long rowNum = 0;
        long deletedRows = 0;

        while(currentPos < dbFile.length()) {
            this.dbFile.seek(currentPos);
            boolean isDeleted = this.dbFile.readBoolean();
            if (!isDeleted) {
                Index.getInstance().add(currentPos);
                rowNum++;
            }
            else
                deletedRows++;
            currentPos += 1;
            this.dbFile.seek(currentPos);
            int recordLength = this.dbFile.readInt();
            currentPos += 4;
            currentPos += recordLength;
        }
        System.out.println("After startup: Total row number in database: " + rowNum);
        System.out.println("After startup: Total deleted row number in database: " + deletedRows);
    }

    public void close() throws IOException {
        this.dbFile.close();
    }

    public void deleteRow(int rowNumber) throws IOException {
        long bytePositionOfRecord = Index.getInstance().getBytePosition(rowNumber);
        if (bytePositionOfRecord == -1) {
            throw new IOException("Row does not exist in Index");
        }
        this.dbFile.seek(bytePositionOfRecord);
        this.dbFile.writeBoolean(true);

        // update the index
        Index.getInstance().remove(rowNumber);
    }
}
