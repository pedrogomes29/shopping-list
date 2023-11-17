package LoadBalencer;

import NIOChannels.Message;
import NIOChannels.Server;

public class MessageProcessorBuilder extends NIOChannels.MessageProcessorBuilder {

    @Override
    public MessageProcessor build(Message message) {
        return new MessageProcessor(server,message);
    }

}
