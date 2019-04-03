package edu.columbia.wak2116;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.http.Header;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.util.Random;
import java.io.*;

/*
        W. Aldo Kusmik, WAK2116, ELEN-E6889, Spring 2019
        Final Project
*/

public class nyc_webcams {


    Random rand = new Random();

    public webcamData httpRequest(int camera_id) throws Exception {

        webcamData data_obj = new webcamData();

        String nyc_URL = "http://207.251.86.238/cctv" + camera_id + ".jpg?math=" + rand.nextDouble();
        CloseableHttpClient httpclient = HttpClients.createDefault();

        try {

            HttpGet httpGet = new HttpGet(nyc_URL);
            CloseableHttpResponse response1 = httpclient.execute(httpGet);

            try {
                //System.out.println(camera_id + ": " + response1.getStatusLine());
                HttpEntity entity1 = response1.getEntity();
                InputStream image_obj = entity1.getContent();

                Header[] request_headers = response1.getAllHeaders();
                /*for (Integer x = 0; x < request_headers.length; x = x+1) {
                    System.out.println(request_headers[x]);
                }*/

                String raw_date_time = String.valueOf(request_headers[1]);
                String http_date_time = raw_date_time.substring(11);
                data_obj.setDateTime(camera_id, http_date_time);
                System.out.print(camera_id + ": " + response1.getStatusLine() + " :  ");

                BufferedImage sourceImage = null;

                try {
                    sourceImage = ImageIO.read(image_obj);

                    try {
                        System.out.print(sourceImage.getHeight() + "," + sourceImage.getWidth() + " - ");
                        data_obj.setImage(camera_id, sourceImage.getHeight(), sourceImage.getWidth(), sourceImage);
                    } catch (NullPointerException exc) {
                        System.out.println("No image");
                    }

                    System.out.println(http_date_time);

                } catch (IOException exc) {
                    System.out.println("BufferImage exception");
                }

                EntityUtils.consume(entity1);

            } finally {
                response1.close();
                httpclient.close();
            }

        } catch (Exception exc) {
            System.out.println("httpRequest threw an exception");

        } finally {

            httpclient.close();
        }

        return data_obj;

    }


    public webcamData readImageFile(String file_location) {

        webcamData data_obj = new webcamData();

        try {
            Image img_new;
            File imageFile = new File(file_location);
            img_new = ImageIO.read(imageFile);
            data_obj.setImage(img_new);

        } catch (IOException exc) {
            System.out.println("Cannot Load Image File");
            System.exit(0);
        }

        return data_obj;

    }


}
