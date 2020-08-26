package com.mixer.util;

import com.mixer.raw.Person;

public final class DebugRowInfo {
    private Person person;
    private boolean isDeleted;

    public DebugRowInfo(Person person, boolean isDeleted) {
        this.person = person;
        this.isDeleted = isDeleted;
    }

    public Person person() {
        return this.person;
    }

    public boolean isDeleted() {
        return this.isDeleted;
    }
}
