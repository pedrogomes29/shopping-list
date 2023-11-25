package RingNode;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;

import Database.Database;
import Utils.Hasher;

public class Server extends Node.Server
{

    private final Database db;

    public Server(String confFilePath, int nodePort, int nReplicas ) throws IOException {
        super(confFilePath, nodePort, nReplicas, new MessageProcessorBuilder());

        try {
            String nodeHash = Hasher.md5(nodeId);

            consistentHashing.getNodeHashes().add(nodeHash);
            gossiper.addRumour("ADD_NODE" + " " + nodeId + " " + port );

            db = new Database("database/"+nodeId+".db");
        } catch (SQLException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public Database getDB(){
        return db;
    }

}
