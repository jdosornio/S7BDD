/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package modelo.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Scanner;
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

    public static void main(String[] args) throws SQLException, InterruptedException {
        String[] columns = {"numero", "primer_nombre", "segundo_nombre",
            "apellido_paterno", "apellido_materno", "puesto_id"};

        Object[][] data = new Object[3][];

        data[0] = new Object[]{"277", "JESUS", "DONALDO", "OSORNIO", "HERNANDEZ", 6};
        data[1] = new Object[]{"278", "ALFREDO", "", "ROUSE", "MADRIGAL", 3};
        data[2] = new Object[]{"279", "EFRAIN", "IVAN", "MARISCAL", "MARTINEZ", 89};

        DataTable dt = new DataTable(columns, data);

        new BaseDAO().add("empleado", dt);

//        Runnable r = () -> {
//            ConnectionManager.commit();
//        };
//        
//        Thread t = new Thread(r);
//        t.start();
//        
//        t.join();
//        
//        new Scanner(System.in).next();
        
        ConnectionManager.commit();
    }
}
