package com.example.ehpoc;

/** The request to send an event was not acceptable (maps to HTTP 400). */
public class InvalidEventException extends RuntimeException {

    public InvalidEventException(String message) {
        super(message);
    }
}
