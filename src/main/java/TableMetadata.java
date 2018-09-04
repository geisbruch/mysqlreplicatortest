import java.util.ArrayList;

public class TableMetadata {

    class Field {
        String fieldName;
        String type;
        Boolean isKey;

    }

    String name;
    ArrayList<Field> fields = new ArrayList<Field>();

    public TableMetadata(String name) {
        this.name = name;
    }

    public ArrayList<Field> getFields() {
        return fields;
    }

    public void addField(String fieldName, String type, boolean key) {
        Field field = new Field();
        field.fieldName = fieldName;
        field.type = type;
        field.isKey = key;
        this.fields.add(field);
    }
}
