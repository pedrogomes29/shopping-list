package LoadBalancer;

import NioChannels.Message.Message;
import Node.Server;

public class MessageProcessorBuilder extends NioChannels.Message.MessageProcessorBuilder {

    @Override
    public MessageProcessor build(Message message) {
        return new MessageProcessor((Server) server,message);
    }

}
