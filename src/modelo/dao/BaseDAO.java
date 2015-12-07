/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package modelo.dao;

import modelo.util.ConnectionManager;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import modelo.dto.DataTable;

/**
 *
 * @author jdosornio
 */
public class BaseDAO {

    public boolean add(String tableName, DataTable data, boolean savePKs) {
        PreparedStatement ps = null;
        Connection conexion;
        boolean ok = true;
        String insertQuery = "INSERT INTO " + tableName + "( ";
        String valuesSection = "VALUES ( ";

        if (data == null || data.getRowCount() == 0) {
            return false;
        }

        try {
            //Crear query
            for (int i = 0; i < data.getColumnCount() - 1; i++) {
                if (savePKs && i == 0) {
                    //Si se quieren guardar las llaves primarias entonces
                    //ignorar la llave primaria que es la columna 1.
                    continue;
                }
                
                insertQuery += data.getColumnName(i) + ", ";
                valuesSection += "?, ";
            }
            insertQuery += data.getColumnName(data.getColumnCount() - 1) + " ) ";
            valuesSection += "? )";

            //Query completo...
            insertQuery += valuesSection;

            System.out.println(insertQuery);

            conexion = ConnectionManager.conectar();

            //Si se desea guardar las llaves generadas o no
            if(savePKs) {
                //Devolver la columna de llave primaria (primera)
                ps = conexion.prepareStatement(insertQuery,
                        new String[] {data.getColumnName(0)});
            } else {
                ps = conexion.prepareStatement(insertQuery);
            }
            
            //Cargar datos...
            //Reiniciar por si las dudas...
            data.rewind();
            
            while (data.next()) {
                for (int i = 0; i < data.getColumnCount(); i++) {
                    //Posicion del preparedStatement
                    int psPos = i + 1;
                    //Si se desea guardar las llaves generadas ignorar la pk
                    if (savePKs) {
                        if (i == 0) {
                            continue;
                        }
                        else {
                            psPos--;
                        }
                    }
                    
                    ps.setObject(psPos, data.getObject(data.getColumnName(i)));
                }
                //Ejecutar insert uno a uno para obtener la llave primaria
                ps.executeUpdate();
                
                if (savePKs) {
                    //Obtener llave primaria generada
                    try (ResultSet rsPK = ps.getGeneratedKeys()) {
                        
                        if(rsPK.next()) {
                            //Guardar llave primaria generada en el DataTable
                            data.setObject(data.getColumnName(0), rsPK.getObject(1));       
                        }
                    }
                }
            }
            
            //Si se guardaron llaves primarias regresar al inicio
            if (savePKs) {
                data.rewind();
            }

            //ConnectionManager.commit();
        } catch (SQLException ex) {
            ConnectionManager.rollback();
            ConnectionManager.cerrar();
            ok = false;
            Logger.getLogger(BaseDAO.class.getName()).log(Level.SEVERE, null, ex);

        } finally {
            ConnectionManager.cerrar(ps);
            //ConnectionManager.cerrarTodo(ps, null);
        }

        return ok;
    }

    public boolean update(String tableName, DataTable data, Map<String, ?> attrWhere) {
        PreparedStatement ps = null;
        Connection conexion;
        boolean ok = true;
        String updateQuery = "UPDATE " + tableName + " SET ";

        if (data == null || data.getRowCount() == 0) {
            return false;
        }

        try {
            //Crear query
            for (int i = 0; i < data.getColumnCount() - 1; i++) {
                updateQuery += data.getColumnName(i) + " = ?, ";
            }
            updateQuery += data.getColumnName(data.getColumnCount() - 1) + " = ?";

            //WHERE clauses...
            if (attrWhere != null && !attrWhere.isEmpty()) {
                updateQuery += " WHERE ";
                List<String> attrs = new ArrayList<>(attrWhere.keySet());

                for (int i = 0; i < attrs.size() - 1; i++) {
                    String key = attrs.get(i).toLowerCase();

                    updateQuery += key + " ";

                    if (!key.contains("=") && !key.contains("<")
                            && !key.contains(">") && !key.endsWith("like")) {
                        updateQuery += "= ";
                    }

                    updateQuery += "? AND ";
                }
                //Last one
                String key = attrs.get(attrs.size() - 1).toLowerCase();

                updateQuery += key + " ";

                if (!key.contains("=") && !key.contains("<")
                        && !key.contains(">") && !key.endsWith("like")) {
                    updateQuery += "= ";
                }

                updateQuery += "?";
            }

            System.out.println(updateQuery);

            conexion = ConnectionManager.conectar();

            ps = conexion.prepareStatement(updateQuery);
            
            //Cargar datos...
            //Reiniciar por si las dudas
            data.rewind();
            
            data.next();

            //SET
            for (int i = 0; i < data.getColumnCount(); i++) {
                ps.setObject(i + 1, data.getObject(data.getColumnName(i)));
            }

            //WHERE
            if (attrWhere != null && !attrWhere.isEmpty()) {
                int paramNumber = data.getColumnCount();

                for (String key : attrWhere.keySet()) {
                    ps.setObject(paramNumber + 1, attrWhere.get(key));
                    paramNumber++;
                }
            }

            ps.executeUpdate();

            //ConnectionManager.commit();
        } catch (SQLException ex) {
            ConnectionManager.rollback();
            ConnectionManager.cerrar();
            ok = false;
            Logger.getLogger(BaseDAO.class.getName()).log(Level.SEVERE, null, ex);

        } finally {
            ConnectionManager.cerrar(ps);
            //ConnectionManager.cerrarTodo(ps, null);
        }

        return ok;
    }

    public boolean delete(String tableName, Map<String, ?> attrWhere) {
        PreparedStatement ps = null;
        Connection conexion;
        boolean ok = true;
        String deleteQuery = "DELETE FROM " + tableName;

        try {
        //Crear query
        //WHERE clauses...
            if (attrWhere != null && !attrWhere.isEmpty()) {
                deleteQuery += " WHERE ";
                List<String> attrs = new ArrayList<>(attrWhere.keySet());

                for (int i = 0; i < attrs.size() - 1; i++) {
                    String key = attrs.get(i).toLowerCase();

                    deleteQuery += key + " ";

                    if (!key.contains("=") && !key.contains("<")
                            && !key.contains(">") && !key.endsWith("like")) {
                        deleteQuery += "= ";
                    }

                    deleteQuery += "? AND ";
                }
                //Last one
                String key = attrs.get(attrs.size() - 1).toLowerCase();

                deleteQuery += key + " ";

                if (!key.contains("=") && !key.contains("<")
                        && !key.contains(">") && !key.endsWith("like")) {
                    deleteQuery += "= ";
                }

                deleteQuery += "?";
            }

        System.out.println(deleteQuery);

            conexion = ConnectionManager.conectar();

            ps = conexion.prepareStatement(deleteQuery);
            //Cargar datos...
            //WHERE
            if (attrWhere != null && !attrWhere.isEmpty()) {
                int paramNumber = 0;
                
                for (String key : attrWhere.keySet()) {
                    ps.setObject(paramNumber + 1, attrWhere.get(key));
                    paramNumber++;
                }
            }

            ps.executeUpdate();

            //ConnectionManager.commit();
        } catch (SQLException ex) {
            ConnectionManager.rollback();
            ConnectionManager.cerrar();
            ok = false;
            Logger.getLogger(BaseDAO.class.getName()).log(Level.SEVERE, null, ex);

        } finally {
            ConnectionManager.cerrar(ps);
            //ConnectionManager.cerrarTodo(ps, null);
        }
        return ok;
    }

//    public static void main(String[] args) {
//        String[] columns = {"segundo_nombre", "apellido_materno"};
//        Object[][] data = {{"Carlo", "Di Angelo"}};
//        HashMap<String, Object> attrWhere = new HashMap<>();
//        
//        attrWhere.put("numero", "777");
//        
//        DataTable dt = new DataTable(columns, data);
//        
//        new BaseDAO().update("empleado", dt, attrWhere);
//        
//        ConnectionManager.commit();
//    }
//    public static void main(String[] args) {
//        HashMap<String, Object> attrWhere = new HashMap<>();
//        
//        attrWhere.put("numero", "777");
//        
//        new BaseDAO().delete("empleado", attrWhere);
//        
//        ConnectionManager.commit();
//    }
    
//    public static void main(String[] args) {
//        //Prueba insert con y sin recuperación de llaves primarias
//        String[] columns = {"id", "nombre", "descripcion", "tipo_evento_id"};
//        Object[][] data = {
//            {null, "EVENTO DE PRUEBA 1", "DESCRIPCION 1", 2},
//            {null, "EVENTO DE PRUEBA 2", "DESCRIPCION 2", 1},
//            {null, "EVENTO DE PRUEBA 3", "DESCRIPCION 3", 2}
//        };
//        
//        DataTable dtInsert = new DataTable(columns, data);
//        
//        //Regresar los ids...
//        new BaseDAO().add("evento", dtInsert, true);
//        
//        int i = 0;
//        while(dtInsert.next()) {
//            System.out.println("PK [" + (i + 1) + "]: " + dtInsert.getInt("id"));
//            i++;
//        }
//        
//        ConnectionManager.commit();
//        ConnectionManager.cerrar();
//    }
    
    public static void main(String[] args) {
        //Prueba insert con y sin recuperación de llaves primarias
        String[] columns = {"id", "nombre", "descripcion", "tipo_evento_id"};
        Object[][] data = {
            {7, "EVENTO DE PRUEBA 1", "DESCRIPCION 1", 2},
            {8, "EVENTO DE PRUEBA 2", "DESCRIPCION 2", 1},
            {9, "EVENTO DE PRUEBA 3", "DESCRIPCION 3", 2}
        };
        
        DataTable dtInsert = new DataTable(columns, data);
        
        //Regresar los ids...
        new BaseDAO().add("evento", dtInsert, false);
        
        int i = 0;
        while(dtInsert.next()) {
            System.out.println("PK [" + (i + 1) + "]: " + dtInsert.getInt("id"));
            i++;
        }
        
        ConnectionManager.commit();
        ConnectionManager.cerrar();
    }
}
