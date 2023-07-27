package io.e6x;

class ArrayElement implements Comparable<ArrayElement> {
    int value;

    int chunkNumber;

    public ArrayElement(int value, int chunkNumber) {
        this.value = value;
        this.chunkNumber = chunkNumber;
    }

    public int getValue() {
        return value;
    }


    public int getChunkNumber() {
        return chunkNumber;
    }

    @Override
    public int compareTo(ArrayElement other) {
        return Integer.compare(this.value, other.value);
    }
}


