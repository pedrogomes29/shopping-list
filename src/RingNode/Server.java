package RingNode;

import LoadBalancer.TokenNode;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

import Database.Database;

public class Server extends Node.Server
{

    private final Database db;

    public Server( int nodePort ) throws IOException {
        super(nodePort, new MessageProcessorBuilder());
        try {
            db = new Database("database/"+nodeId+".db");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Database getDB(){
        return db;
    }

}
