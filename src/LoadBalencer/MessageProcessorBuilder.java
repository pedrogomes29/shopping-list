package LoadBalencer;

import NIOChannels.Message;

public class MessageProcessorBuilder extends NIOChannels.MessageProcessorBuilder {

    @Override
    public MessageProcessor build(Message message) {
        return new MessageProcessor(server,message);
    }

}
