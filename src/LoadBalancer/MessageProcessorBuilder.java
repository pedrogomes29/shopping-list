package LoadBalancer;

import Node.Message.Message;

public class MessageProcessorBuilder extends Node.Message.MessageProcessorBuilder {

    @Override
    public MessageProcessor build(Message message) {
        return new MessageProcessor(server,message);
    }

}
