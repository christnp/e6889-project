import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/*
        W. Aldo Kusmik, WAK2116, ELEN-E6889, Spring 2019

        Final Project
        This is basic java client to test the operation of Google Cloud PubSub

*/

public class Traffic_PubSub_Client {

    // use the default project id
    private static final String PROJECT_ID = ServiceOptions.getDefaultProjectId();

    private static final BlockingQueue<PubsubMessage> messages = new LinkedBlockingDeque<>();

    static class MessageReceiverExample implements MessageReceiver {
        @Override
        public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
            messages.offer(message);
            consumer.ack();
        }
    }

    public static void main(String[] args) throws Exception {

        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(
               PROJECT_ID, "traffic-sub");

        Subscriber subscriber = null;

        try {
            System.out.println("Running PubSub Client");

            // create a subscriber bound to the asynchronous message receiver
            subscriber =
                    Subscriber.newBuilder(subscriptionName, new MessageReceiverExample()).build();
            subscriber.startAsync().awaitRunning();
            // Continue to listen to messages
            while (true) {
                PubsubMessage message = messages.take();
                //System.out.println("Message Id: " + message.getMessageId());
                System.out.println("Data: " + message.getData().toStringUtf8());
            }
        } finally {
            if (subscriber != null) {
                subscriber.stopAsync();
            }
            System.out.println("Done");
            }
        }
}


