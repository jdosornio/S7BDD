/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package modelo.dto;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import javax.swing.table.AbstractTableModel;

/**
 *
 * @author jdosornio
 */
public class DataTable extends AbstractTableModel {

    /**
     * Nombres de las columnas de la tabla
     */
    private String[] columns;
    /**
     * Matriz con los datos de la tabla
     */
    private Object[][] data;
    /**
     * Cursor que apunta a la posición actual en la que se lee-escribe
     */
    private int currentIndex = -1;
    /**
     * bandera para indicar si se puede escribir o no
     */
    private boolean readOnly = false;
    
    /**
     * Nombres de las tablas de las que se extrajo la información, puede ser null
     * si no se obtuvieron los datos de un resultset.
     */
    private String[] tableNames;
    
    /**
     * Crea un nuevo DataTable vacío en modo escritura.
     */
    public DataTable() {
        
    }
    
    /**
     * Crea un nuevo DataTable con los nombres de las columnas especificadas,
     * además también crea una matriz de datos vacía con el número de filas y
     * columnas especificado. También posiciona el cursor una posición antes del
     * primer registro.
     * El DataTable se crea en modo escritura.
     * 
     * @param columns un arreglo con los nombres de las columnas de la tabla
     * @param noRows el número de filas que tendrá el DataTable (mayor a 0)
     * @param noCols el número de columnas que tendrá el DataTable (mayor a 0)
     */
    public DataTable(String[] columns, int noRows, int noCols) {
        this.columns = columns;
        this.data = new Object[noRows][noCols];
    }
    
    /**
     * Crea un nuevo DataTable con las columnas y la matriz proporcionada,
     * también posiciona el cursor una posición antes del primer registro.
     * El DataTable se crea en modo de escritura.
     * 
     * @param columns el arreglo de nombres de columnas
     * @param data la matriz de datos
     */
    public DataTable(String[] columns, Object[][] data) {

        this.columns = columns;
        this.data = data;
        
    }
    
    /**
     * Obtiene el arreglo con los nombres de columnas de este DataTable
     * 
     * @return el arreglo de columnas o null en caso de que no tenga información
     */
    public String[] getColumns() {
        return columns;
    }
    
    /**
     * Obtiene los nombres de las tablas de las que se extrajeron las columnas
     * de este DataTable
     * 
     * @return un arreglo de nombres únicos de tablas de las que se extrajo la
     * información del DataTable. Puede ser que se regrese un elemento vacío en
     * caso de que los nombres de las tablas no sean aplicables en el DBMS. En
     * caso de que no existan datos en el DataTable se devolverá null.
     */
    public String[] getTableNames() {
        return tableNames;
    }
    
    /**
     * Obtiene el total de filas almacenadas en este DataTable
     * 
     * @return el total de filas o 0 en caso de que no haya ninguna
     */
    @Override
    public int getRowCount() {
        
        return (data != null) ? data.length : 0;
    }

    /**
     * Obtiene el total de columnas de este DataTable
     * 
     * @return el total de columnas o 0 en caso de que este DataTable no contenga
     * nada
     */
    @Override
    public int getColumnCount() {
        
        return (columns != null) ? columns.length : 0;
    }

    /**
     * Obtiene el valor que se encuentra en el índice de la fila y columna
     * especificados, será necesario realizar un cast para el tipo de dato que
     * se devuelva. Este método no altera la posición del cursor
     * 
     * @param rowIndex el índice de la fila (base 0)
     * @param columnIndex el índice de la columna (base 0)
     * @return el valor en la posición deseada, null si no existe
     * @throws ArrayIndexOutOfBoundsException en caso de que no existan las
     * posiciones en el objeto
     */
    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        return data[rowIndex][columnIndex];
    }
    
    /**
     * Almacena el objeto especificado en la fila y columna especificada.
     * Este método no altera la posición del cursor
     * 
     * @param rowIndex el índice de la fila (base 0)
     * @param columnIndex el índice de la columna (base 0)
     * @param value el objeto que se desea almacenar en el DataTable
     * @throws ArrayIndexOutOfBoundsException en caso de que no existan las
     * posiciones en la matriz del DataTable
     */
    private void setValueAt(int rowIndex, int columnIndex, Object value) {
        data[rowIndex][columnIndex] = value;
    }
    
    /**
     * Obtiene el nombre de la columna en el índice especificado
     * 
     * @param column índice de la columna (base 0)
     * 
     * @return el nombre de la columna en la posición dada.
     * @throws ArrayIndexOutOfBoundsException en caso de que no exista el índice
     * especificado
     */
    @Override
    public String getColumnName(int column) {
        return columns[column];
    }
    
    /**
     * Obtiene el índice de una columna en base a su nombre
     * 
     * @param name el nombre exacto de la columna (sin ignorar mayúsculas o
     * minúsculas)
     * 
     * @return el índice en base 0 de la columna o -1 en caso de que no exista
     * el nombre especificado.
     */
    private int getColumnIndex(String name) {
        int indx = -1;
        
        for (int i = 0; i < columns.length; i++) {
            if(columns[i].equalsIgnoreCase(name)) {
                indx = i;
                break;
            }
        }
        
        return indx;
    }
    
    /**
     * Obtiene el tipo de dato de la columna del DataTable
     * 
     * @param column el índice base 0 de la columna
     * 
     * @return el tipo de dato de la columna
     * @throws ArrayIndexOutOfBoundsException en caso de que no exista el índice
     * especificado
     */
    @Override
    public Class getColumnClass(int column) {
        return getValueAt(0, column).getClass();
    }
    
    /**
     * Regresa el tipo de la columna basándose en su nombre
     * 
     * @param columnName el nombre de la columna de la cuál se quiere saber su
     * tipo
     * 
     * @return el tipo de dato de la columna
     * @throws ArrayIndexOutOfBoundsException en caso de que no exista ninguna
     * columna con el nombre especificado
     */
    public Class getColumnClass(String columnName) {
        return getColumnClass(getColumnIndex(columnName));
    }
    
    /**
     * Llena el DataTable con el resultset completo y reinicia el cursor para
     * posicionarlo una posición antes del primer registro. Cambia el estado del
     * DataTable a sólo lectura.
     * 
     * @see next()
     * @param rs el resultset de la base de datos que contiene la información
     * (no debe ser leído antes para que se puedan cargar todos los registros)
     * 
     * @throws SQLException en caso de que ocurra un error de la base de datos
     */
    public void populate(ResultSet rs) throws SQLException {
        
        if(rs != null) {
            
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            columns = new String[columnCount];
            List<Object[]> tempData = new ArrayList<>();
            List<String> tempTableNames = new ArrayList<>();


            for (int i = 0; i < columnCount; i++) {
                columns[i] = metaData.getColumnLabel(i + 1);
                String tableName = metaData.getTableName(i + 1);
                
                if (!tempTableNames.contains(tableName)) {
                    tempTableNames.add(tableName);
                }
            }

            //rs.beforeFirst();         //No se puede en todos los DBMS
            while(rs.next()) {
                Object[] row = new Object[columnCount];

                for (int i = 0; i < columnCount; i++) {
                    row[i] = rs.getObject(i + 1);
                }
                tempData.add(row);
            }

            data = new Object[tempData.size()][];
            tempData.toArray(data);
            
            tableNames = new String[tempTableNames.size()];
            tempTableNames.toArray(tableNames);
            
            currentIndex = -1;
            
            readOnly = true;
        }
        else {
            throw new IllegalArgumentException("ResultSet can't be null");
        }
    }
    
    /**
     * Posicionar el cursor en el siguiente registro. El DataTable por defecto tiene
     * el cursor en una posición antes del primer registro. Este método posiciona
     * el cursor en el siguiente registro y regresa true si quedan registros en
     * el DataTable.
     * 
     * @return true si quedan registros, false si no
     */
    public boolean next() {
        currentIndex++;
        
        return (data != null) ? currentIndex < data.length : false;
    }
    
    /**
     * Obtiene el valor del DataTable con el nombre de la columna especificado en
     * la fila en la que el cursor está posicionado actualmente
     * 
     * @param columnName el nombre de la columna de donde se quiere obtener el
     * valor
     * 
     * @return el valor de la columna
     * @throws ArrayIndexOutOfBoundsException en caso de que el cursor no se
     * encuentre en una posición válida o no exista el nombre de la columna
     * especificado
     */
    public Object getObject(String columnName) {
        return getValueAt(currentIndex, getColumnIndex(columnName));
    }
    
    /**
     * Obtiene el valor del DataTable con el nombre de la columna especificado en
     * la fila en la que el cursor está posicionado actualmente
     * 
     * @param columnName el nombre de la columna de donde se quiere obtener el
     * valor
     * 
     * @return El valor de la columna en tipo de dato int
     * @throws ArrayIndexOutOfBoundsException en caso de que el cursor no se
     * encuentre en una posición válida o no exista el nombre de la columna
     * especificado
     * @throws ClassCastException en caso de que el valor en la columna con el
     * nombre especificado no sea de tipo int
     */
    public int getInt(String columnName) {
        return (int) getObject(columnName);
    }
    
    /**
     * Obtiene el valor del DataTable con el nombre de la columna especificado en
     * la fila en la que el cursor está posicionado actualmente
     * 
     * @param columnName el nombre de la columna de donde se quiere obtener el
     * valor
     * 
     * @return El valor de la columna en tipo de dato String
     * @throws ArrayIndexOutOfBoundsException en caso de que el cursor no se
     * encuentre en una posición válida o no exista el nombre de la columna
     * especificado
     * @throws ClassCastException en caso de que el valor en la columna con el
     * nombre especificado no sea de tipo String
     */
    public String getString(String columnName) {
        return (String) getObject(columnName);
    }
    
    /**
     * Obtiene el valor del DataTable con el nombre de la columna especificado en
     * la fila en la que el cursor está posicionado actualmente
     * 
     * @param columnName el nombre de la columna de donde se quiere obtener el
     * valor
     * 
     * @return El valor de la columna en tipo de dato double
     * @throws ArrayIndexOutOfBoundsException en caso de que el cursor no se
     * encuentre en una posición válida o no exista el nombre de la columna
     * especificado
     * @throws ClassCastException en caso de que el valor en la columna con el
     * nombre especificado no sea de tipo double
     */
    public double getDouble(String columnName) {
        return (double) getObject(columnName);
    }
    
    /**
     * Obtiene el valor del DataTable con el nombre de la columna especificado en
     * la fila en la que el cursor está posicionado actualmente
     * 
     * @param columnName el nombre de la columna de donde se quiere obtener el
     * valor
     * 
     * @return El valor de la columna en tipo de dato boolean
     * @throws ArrayIndexOutOfBoundsException en caso de que el cursor no se
     * encuentre en una posición válida o no exista el nombre de la columna
     * especificado
     * @throws ClassCastException en caso de que el valor en la columna con el
     * nombre especificado no sea de tipo boolean
     */
    public boolean getBoolean(String columnName) {
        return (boolean) getObject(columnName);
    }
    
    /**
     * Obtiene el valor del DataTable con el nombre de la columna especificado en
     * la fila en la que el cursor está posicionado actualmente
     * 
     * @param columnName el nombre de la columna de donde se quiere obtener el
     * valor
     * 
     * @return El valor de la columna en tipo de dato Date
     * @throws ArrayIndexOutOfBoundsException en caso de que el cursor no se
     * encuentre en una posición válida o no exista el nombre de la columna
     * especificado
     * @throws ClassCastException en caso de que el valor en la columna con el
     * nombre especificado no sea de tipo Date
     */
    public Date getDate(String columnName) {
        return (Date) getObject(columnName);
    }
    
    /**
     * Almacena el valor especificado en el DataTable en la columna con el nombre
     * especificado y la fila en la que el cursor está posicionado actualmente
     * 
     * @param columnName el nombre de la columna
     * @param value el valor a almacenar
     * @return true en caso de que la operación se realice con éxito, false en
     * caso de que el DataTable esté en modo sólo lectura
     * @throws ArrayIndexOutOfBoundsException en caso de que el cursor no se
     * encuentre en una posición válida o no exista el nombre de la columna
     * especificado
     */
    public boolean setObject(String columnName, Object value) {
        boolean ok = false;
        
        if(!readOnly) {
            setValueAt(currentIndex, getColumnIndex(columnName), value);
            ok = true;
        }
        
        return ok;
    }
    
    /**
     * Reinicia el cursor una posición antes de la primera fila
     */
    public void rewind() {
        currentIndex = -1;
    }
    
    public DataTable[] fragmentarVertical(String[] frag1, String[] frag2) {
        DataTable dts[] = new DataTable[2];

        Object[][] data1 = new Object[this.getRowCount()][frag1.length];

        Object[][] data2 = new Object[this.getRowCount()][frag2.length];

        //Recorremos los atributos que se desea que tenga el fragmento 1
        for (int i = 0; i < frag1.length; i++) {
            //Obtenemos la columa a la que pertenece el atributo
            int ndx = this.getColumnIndex(frag1[i]);
            //Recorremos toda la columna para almacenar sus datos en una
            //nueva matriz
            for (int j = 0; j < this.getRowCount(); j++) {
                //Se almacena la información del atributo deseado 
                //j iterará por la columna de principio a fin
                //i es el indice de la columna del fragmento nuevo
                //ndx es el indice de la columna donde se encontro el nombre del
                //atributo deseado a copiar al fragmento nuevo
                data1[j][i] = this.getValueAt(j, ndx);
            }
        }

        //Recorremos los atributos que se desea que tenga el fragmento 1
        for (int i = 0; i < frag2.length; i++) {
            //Obtenemos la columa a la que pertenece el atributo
            int ndx = this.getColumnIndex(frag2[i]);
            //Recorremos toda la columna para almacenar sus datos en una
            //nueva matriz
            for (int j = 0; j < this.getRowCount(); j++) {
                //Se almacena la información del atributo deseado 
                //j iterará por la columna de principio a fin
                //i es el indice de la columna del fragmento nuevo
                //ndx es el indice de la columna donde se encontro el nombre del
                //atributo deseado a copiar al fragmento nuevo
                data2[j][i] = this.getValueAt(j, ndx);
            }
        }

        dts[0] = new DataTable(frag1, data1);
        dts[1] = new DataTable(frag2, data2);

        return dts;
    }
}