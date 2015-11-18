/*
 * Copyright (C) 2015 Jesús Donaldo Osornio Hernández
 *
 * This file is part of MatExámenes.
 *
 * MatExámenes is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation; either version 2 of the License, or (at your option) any later
 * version.
 *
 * MatExámenes is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc., 51
 * Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */
package remote;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.Map;

/**
 * Esta clase se encarga de hacer la conexión remota con un registro en la
 * aplicación de persistencia por medio de la interface Persistencia
 *
 * @author Jesus Donaldo Osornio Hernández
 * @version 1 18 Mayo 2015
 */
public class InterfaceManager {

    private static final Map<String, Sitio> sitios = new HashMap<>();
    

//    public static Persistencia getPersistencia() throws RemoteException,
//            NotBoundException {
//        if (persistencia == null) {
//            Registry registro = LocateRegistry.getRegistry(ip, puerto);
//            persistencia = (Persistencia) registro.lookup("MatExPersist");
//        }
//        return persistencia;
//    }

//    public static void setPuerto(int puerto) {
//        Enlace.puerto = puerto;
//    }
//
//    /**
//     * 
//     * @param ip La ip del servidor.
//     */
//    public static void setIp(String ip) {
//        Enlace.ip = ip;
//    }
    
    public static void addInterface(String ip, int puerto, String servicio)
            throws RemoteException, NotBoundException {
        
        if(!sitios.containsKey(servicio)) {
            Registry registro = LocateRegistry.getRegistry(ip, puerto);
            Sitio sitio = (Sitio) registro.lookup(servicio);
            
            sitios.put(servicio, sitio);
        }
    }
    
    public static Sitio getInterface(String servicio)
            throws RemoteException, NotBoundException {
     
        return sitios.get(servicio);
    }
    
}