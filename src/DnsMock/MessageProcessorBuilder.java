package DnsMock;

import NioChannels.Message.Message;

public class MessageProcessorBuilder extends NioChannels.Message.MessageProcessorBuilder {
    @Override
    public MessageProcessor build(Message message) {
        return new MessageProcessor((Server)server,message);
    }

}
