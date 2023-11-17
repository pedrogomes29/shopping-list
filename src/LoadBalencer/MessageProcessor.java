package LoadBalencer;

import NIOChannels.Message;
import NIOChannels.Server;

public class MessageProcessor extends NIOChannels.MessageProcessor {
    public MessageProcessor(Server server, NIOChannels.Message message) {
        super(server, message);
    }

    @Override
    public void run() {

    }
}
