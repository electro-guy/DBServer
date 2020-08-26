package com.mixer.raw;

public class Person {
    public String name;
    public int age;
    public String address;
    public String carPlateNumber;
    public String description;

    public Person() {
    }

    public Person(final String name,
                  final int age,
                  final String address,
                  final String carPlateNumber,
                  final String description) {
        this.name = name;
        this.age = age;
        this.address = address;
        this.carPlateNumber = carPlateNumber;
        this.description = description;
    }

    @Override
    public String toString() {
        return String.format("name: %s, age: %d, address: %s, carplate: %s, description: %s",
                this.name,
                this.age,
                this.address,
                this.carPlateNumber, this.description);
    }
}
