package com.mixer.raw;

public class Person {
    public String name;
    public int age;
    public String address;
    public String carPlateNumber;
    public String description;

    @Override
    public String toString() {
        return String.format("name: %s, age: %d, address: %s, carplate: %s, description: %s",
                this.name,
                this.age,
                this.address,
                this.carPlateNumber, this.description);
    }
}
