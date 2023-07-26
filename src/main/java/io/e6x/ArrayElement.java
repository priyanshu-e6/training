package io.e6x;

class ArrayElement implements Comparable<ArrayElement> {
    int value;
    int elemIndex;
    int chunkNumber;

    public ArrayElement(int value, int elemIndex, int chunkNumber) {
        this.value = value;
        this.elemIndex = elemIndex;
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


