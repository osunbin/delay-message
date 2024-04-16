package com.bin.delay.file.common.errors;

public class DelayException extends RuntimeException{
    private final static long serialVersionUID = 1L;

    public DelayException(String message, Throwable cause) {
        super(message, cause);
    }

    public DelayException(String message) {
        super(message);
    }

    public DelayException(Throwable cause) {
        super(cause);
    }

    public DelayException() {
        super();
    }
}
