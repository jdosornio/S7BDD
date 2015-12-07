/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package modelo.dao;

import modelo.util.ConnectionManager;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
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

    public boolean add(String tableName, DataTable data) {
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
                insertQuery += data.getColumnName(i) + ", ";
                valuesSection += "?, ";
            }
            insertQuery += data.getColumnName(data.getColumnCount() - 1) + " ) ";
            valuesSection += "? )";

            //Query completo...
            insertQuery += valuesSection;

            System.out.println(insertQuery);

            conexion = ConnectionManager.conectar();

            ps = conexion.prepareStatement(insertQuery);
            //Cargar datos...
            //Reiniciar por si las dudas...
            data.rewind();
            
            while (data.next()) {
                for (int i = 0; i < data.getColumnCount(); i++) {
                    ps.setObject(i + 1, data.getObject(data.getColumnName(i)));
                }
                ps.addBatch();
            }

            ps.executeBatch();

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
    public static void main(String[] args) {
        HashMap<String, Object> attrWhere = new HashMap<>();
        
        attrWhere.put("numero", "777");
        
        new BaseDAO().delete("empleado", attrWhere);
        
        ConnectionManager.commit();
    }
}
