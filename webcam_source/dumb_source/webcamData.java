package edu.columbia.wak2116;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;
import javax.imageio.ImageIO;

// Imports the Google Cloud Storage client library
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

// Imports the Google Cloud PubSub client library
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/*
        W. Aldo Kusmik, WAK2116, ELEN-E6889, Spring 2019
        Final Project
*/


public class webcamData {

    Integer cameraID = -1;
    Integer img_height = -1;
    Integer img_width = -1;
    //Image img_obj = null;
    Image img_obj = null;
    String datetime = "none";

    // For Google Cloud Storage
    Storage storage = StorageOptions.getDefaultInstance().getService();

    // For Google Cloud PubSub
    // use the default project id
    private static final String PROJECT_ID = ServiceOptions.getDefaultProjectId();


    public void setImage(Image img) {
        img_obj = img;
    }

    public void setImage(Integer id, Integer height, Integer width, Image img) {
        cameraID = id;
        img_height = height;
        img_width = width;
        img_obj = img;
    }

    public Image getImage() {
        return img_obj;
    }

    public void showImage() {
        if (img_obj != null){
            SimpleImageLoad appwin = new SimpleImageLoad(img_obj);
            appwin.setTitle(String.valueOf("Camera " + cameraID + " - " + datetime));
            appwin.setVisible(true);
        }
       else {
            System.out.println("No Image: Camera " + cameraID + " - " + datetime);;
        }
    }

    public void setDateTime(Integer id, String date_header) {
        cameraID = id;
        datetime = date_header;
    }

    public void logToDisk() throws IOException {
        FileOutputStream fout = null;

        try {

            fout = new FileOutputStream("logfile2.txt",true);

            String log = cameraID+","+datetime+","+img_height+","+img_width + System.getProperty("line.separator");
            fout.write(log.getBytes());

        } catch (IOException exc) {
            System.out.println(exc);
        } finally {
            try {
                if (fout != null) {fout.close();}
            } catch (IOException ex) {
                System.out.println("Error closing log file");
            }
        }
    }

    public void pubToBucket() throws IOException {

        try {
            //System.out.println("Publishing image data to Google Cloud Pub/Sub");
            //Storage storage = StorageOptions.getDefaultInstance().getService();

            // Specify where to write the image file in the Google bucket
            String filename = "nyc_webcams/"+cameraID+"/"+datetime;
            BlobId blobId = BlobId.of("storage.kusmik.nyc", filename);

            // Code to test connection
            //BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
            //Blob blob = storage.create(blobInfo, "Hello, Cloud Storage!".getBytes(java.nio.charset.StandardCharsets.UTF_8));

            // Convert traffic cam Image into a BufferedImage
            BufferedImage buf_img_obj = new BufferedImage(img_obj.getWidth(null), img_obj.getHeight(null), BufferedImage.TYPE_INT_RGB);
            Graphics2D bGr = buf_img_obj.createGraphics();
            bGr.drawImage(img_obj, 0, 0, null);
            bGr.dispose();

            //SimpleImageLoad appwin = new SimpleImageLoad(buf_img_obj);
            //appwin.setTitle(String.valueOf("Camera " + cameraID + " - " + datetime));
            //appwin.setVisible(true);

            // convert BufferedImage to Byte Array
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ImageIO.write(buf_img_obj, "jpg", baos);
            byte[] imageInByte = baos.toByteArray();

            // Publish Image
            BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("image/jpeg").build();
            Blob blob = storage.create(blobInfo, imageInByte);

        } catch (Exception exc) {
            System.out.println(exc);
            System.out.println("Error publishing image data to Google Cloud Storage");
        }
    }

    public void pubToPubSub() throws Exception {

        // PubSub message contains file name of new image
        //System.out.println("Google Cloud Pub/Sub: traffic-topic");
        String message_data = "nyc_webcams/" + cameraID + "/" + datetime;

        // Traffic-Topic
        String topicId = "traffic-topic";
        ProjectTopicName topicName = ProjectTopicName.of(PROJECT_ID, topicId);
        Publisher publisher = null;
        List<ApiFuture<String>> futures = new ArrayList<>();

        try {
            // Create a publisher instance with default settings bound to the topic
            publisher = Publisher.newBuilder(topicName).build();

            // convert message to bytes
            ByteString data = ByteString.copyFromUtf8(message_data);
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                    .setData(data)
                    .build();

            // Schedule a message to be published. Messages are automatically batched.
            ApiFuture<String> future = publisher.publish(pubsubMessage);
            futures.add(future);

        } finally {
            // Wait on any pending requests
            List<String> messageIds = ApiFutures.allAsList(futures).get();

            /*
            for (String messageId : messageIds) {
                System.out.println(messageId);
            }
            */

            if (publisher != null) {
                // When finished with the publisher, shutdown to free up resources.
                publisher.shutdown();
            }
        }
    }
}
