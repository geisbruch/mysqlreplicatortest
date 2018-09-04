public class BinlogPosition {
    String filename;
    Long position;

    public BinlogPosition(String binlogFilename, long binlogPosition) {
        this.filename = binlogFilename;
        this.position = binlogPosition;
    }
}
