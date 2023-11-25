package RingNode;

import LoadBalancer.TokenNode;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

import Database.Database;

public class Server extends Node.Server
{

    private final Database db;

    public Server(String confFilePath, int nodePort ) throws IOException {
        super(confFilePath, nodePort, new MessageProcessorBuilder());

        nodeHashes.add(nodeId);
        addRumour("ADD_NODE" + " " + nodeId + " " + port );

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
