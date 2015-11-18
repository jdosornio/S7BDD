/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package modelo.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author jdosornio
 */
public class ConnectionManager {
    
    private static final ThreadLocal<Connection> conexion = new ThreadLocal<>();
    private static final String user = "postgres";
    private static final String pass = "postgres";
    private static final String url = "jdbc:postgresql://localhost:5432/int_stu12_dec03";
    private static final String driver = "org.postgresql.Driver";
    
    public static Connection conectar() {
        Connection con = conexion.get();
        
        try {    
            
            if (con != null && !con.isClosed()) {
                con.close();
            }
            
            Class.forName(driver);

            con = DriverManager.getConnection(url, user, pass);
            con.setAutoCommit(false);
            
        } catch (SQLException | ClassNotFoundException ex) {
            Logger.getLogger(ConnectionManager.class.getName()).log(Level.SEVERE, null, ex);
            con = null;
        }
        
        conexion.set(con);
        
        return con;
    }
    
    public static void cerrar(PreparedStatement ps) throws SQLException {
        if(ps != null) {
            ps.close();
        }
    }
    
    public static void cerrar(ResultSet rs) throws SQLException {
        if(rs != null) {
            rs.close();
        }
    }
    
    public static void commit() {
        Connection con = conexion.get();
        
        try {
            if(con != null && !con.isClosed()) {
                con.commit();
            }
        } catch (SQLException ex) {
            Logger.getLogger(ConnectionManager.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public static void rollback() {
        Connection con = conexion.get();
        
        try {
            if(con != null && !con.isClosed()) {
                con.rollback();
            }
        } catch (SQLException ex) {
            Logger.getLogger(ConnectionManager.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public static void cerrar() {
        Connection con = conexion.get();
        
        try {
            if(con != null && !con.isClosed()) {
                con.close();
                conexion.set(null);
            }
        } catch (SQLException ex) {
            Logger.getLogger(ConnectionManager.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public static void cerrarTodo(PreparedStatement ps, ResultSet rs) {
        try {
            cerrar(ps);
            cerrar(rs);
            cerrar();
        } catch (SQLException ex) {
            Logger.getLogger(ConnectionManager.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
}