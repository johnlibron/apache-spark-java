package com.virtualpairprogrammers.rdd;

public class IntegerWithSquareRoot {

    private final int originalNumber;
    private final double squareRoot;

    public IntegerWithSquareRoot(int i) {
        this.originalNumber = i;
        this.squareRoot = Math.sqrt(this.originalNumber);
    }

    @Override
    public String toString() {
        return "IntegerWithSquareRoot{" +
                "originalNumber=" + originalNumber +
                ", squareRoot=" + squareRoot +
                '}';
    }
}
