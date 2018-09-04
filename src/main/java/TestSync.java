import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.google.gson.GsonBuilder;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.nio.charset.Charset;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TestSync {

    private String password;
    private String host;
    private String username;
    private Integer port;
    private Connection mysqlConn;
    private BinlogPosition lastBinlogPosition;
    private Map<String,TableMetadata> tableMetadataByName = new HashMap<String, TableMetadata>();
    private Map<Long,TableMetadata> tableMetadataById = new HashMap<Long, TableMetadata>();

    public TestSync(String host, Integer port, String username, String password, String database){
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            mysqlConn = DriverManager.getConnection("jdbc:mysql://"+host+":"+port+"/"+database+"?" +
                    "user="+username+"&password="+password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }



    private void loadTableMetadata() throws SQLException {
        tableMetadataByName.clear();
        Statement stmt = mysqlConn.createStatement();
        try {
            ResultSet rsTables = stmt.executeQuery("show tables;");
            ArrayList<String> tables = new ArrayList<String>();
            while (rsTables.next()) {
                tables.add(rsTables.getString(1));
            }

            for (String table : tables) {
                ResultSet descTable = stmt.executeQuery("describe " + table + ";");
                TableMetadata metadata = new TableMetadata(table);
                while(descTable.next()) {
                    metadata.addField(descTable.getString("Field"),
                            descTable.getString("Type"),
                            "PRI".equals(descTable.getString("Key")));
                }
                tableMetadataByName.put(table, metadata);
            }
        } finally {
             stmt.close();
        }
    }

    private BinlogPosition getLastBinlogPosition() {
        try {
            return new GsonBuilder().create().fromJson(new FileReader("/tmp/binlog.json"), BinlogPosition.class);
        } catch(Exception e) {
            return null;
        }
    }

    private void updateBinlogPosition(Event event) throws IOException {
        this.lastBinlogPosition.position = ((EventHeaderV4)event.getHeader()).getPosition();
        saveLastBinglogPostion(this.lastBinlogPosition);
    }

    private void updateBinlogPosition(BinlogPosition binlogPosition) throws IOException {
        this.lastBinlogPosition = binlogPosition;
        saveLastBinglogPostion(this.lastBinlogPosition);
    }

    private void saveLastBinglogPostion(BinlogPosition lastBinlogPosition) throws IOException {
        if(lastBinlogPosition.position > 0) {
            FileUtils.writeStringToFile(new File("/tmp/binlog.json"),
                    new GsonBuilder().create().toJson(lastBinlogPosition),
                    Charset.defaultCharset());
        }
    }


    public void startConsumption() throws SQLException, IOException {
        loadTableMetadata();
        BinaryLogClient client = new BinaryLogClient(this.host, this.port, this.username,this.password);
        client.setConnectTimeout(1000);
        client.setHeartbeatInterval(500);
        client.setKeepAliveInterval(800);
        client.setKeepAlive(true);
        client.setServerId(8888);
        BinlogPosition binlogPosition = getLastBinlogPosition();
        if(binlogPosition != null) {
            client.setBinlogPosition(binlogPosition.position);
            client.setBinlogFilename(binlogPosition.filename);
        }
        client.registerEventListener(new BinaryLogClient.EventListener() {

            @Override
            public void onEvent(Event event) {
                try {
                    if (event.getHeader().getEventType().equals(EventType.TABLE_MAP)) {
                        Long id = ((TableMapEventData) event.getData()).getTableId();
                        String name = ((TableMapEventData) event.getData()).getTable();
                        tableMetadataById.put(id, tableMetadataByName.get(name));
                    } else if (event.getHeader().getEventType().equals(EventType.EXT_WRITE_ROWS) ||
                            event.getHeader().getEventType().equals(EventType.DELETE_ROWS)) {
                        showRowChanges((WriteRowsEventData) event.getData());
                        updateBinlogPosition(event);
                    } else if (event.getHeader().getEventType().equals(EventType.EXT_DELETE_ROWS)) {
                        showRowChanges((DeleteRowsEventData) event.getData());
                        updateBinlogPosition(event);
                    } else if (event.getHeader().getEventType().equals(EventType.EXT_UPDATE_ROWS)) {
                        showRowChanges((UpdateRowsEventData) event.getData());
                        updateBinlogPosition(event);
                    } else if (event.getHeader().getEventType().equals(EventType.ROTATE)) {
                        BinlogPosition binlogPosition = new BinlogPosition(((RotateEventData) event.getData()).getBinlogFilename(), ((RotateEventData) event.getData()).getBinlogPosition());
                        updateBinlogPosition(binlogPosition);
                    }
                }catch(IOException e) {
                    e.printStackTrace();
                }
            }
        });
        client.connect();
    }


    private void showRowChanges(UpdateRowsEventData data) {
        TableMetadata tableMetadata = tableMetadataById.get(data.getTableId());
        List<Serializable[]> after = new ArrayList<Serializable[]>();
        for(Map.Entry<Serializable[], Serializable[]> e : data.getRows()) {
            after.add(e.getValue());
        }
        showRowChanges(tableMetadata,after, "Update - After");
    }

    private void showRowChanges(WriteRowsEventData data) {
        TableMetadata tableMetadata = tableMetadataById.get(data.getTableId());
        showRowChanges(tableMetadata,data.getRows(), "Insert");
    }

    private void showRowChanges(DeleteRowsEventData data) {
        TableMetadata tableMetadata = tableMetadataById.get(data.getTableId());
        showRowChanges(tableMetadata,data.getRows(), "Delete");
    }

    private void showRowChanges(TableMetadata tableMetadata, List<Serializable[]> rows, String operation) {
        if(tableMetadata == null) {
            System.out.println("No table metadata found");
        } else {
            for(Serializable[] row : rows) {
                System.out.println("------------- New Operation: "+operation+" ---------------");
                System.out.println("Table: "+tableMetadata.name);
                System.out.println("Fields: ");
                int i = 0;
                for(Serializable field : row) {
                    System.out.println("\t"+tableMetadata.fields.get(i).fieldName+": "+field.toString()
                            +(tableMetadata.fields.get(i).isKey?" -> PRIMARY":""));
                    i++;
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, SQLException {
        Logger.getLogger(BinaryLogClient.class.getName()).setLevel(Level.INFO);
        new TestSync("127.0.0.1",33033,"root","root","test").startConsumption();

    }
}



