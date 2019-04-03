import edu.columbia.wak2116.nyc_webcams;
import edu.columbia.wak2116.webcamData;

/*
        W. Aldo Kusmik, WAK2116, ELEN-E6889, Spring 2019

        Final Project
        This java file connects to the NYC traffic camera system website, retrieves jpg traffic camera images, and
        forwards the images from specific cameras to Google Cloud Pub/Sub to serve as a candidate source for
        the ELEN-E6889 final project

        Images are published for three cameras that are in close proximity to the Columbia University COSMOS testbed
        - cctv1005 is located at Broadway & West 125th
        - cctv689 is located at Amsterdam & West 125th
        - cctv688 is located at St Nicholas Ave & West 125th

*/



public class main {

    public static void main(String[] args) throws Exception {

        webcamData[] data_array = new webcamData[3];


        int[] traffic_cam = new int[3];

        traffic_cam[0] = 688; //traffic camera at St Nicholas Ave & West 125th
        traffic_cam[1] = 689; //traffic camera at Amsterdam & West 125th
        traffic_cam[2] = 1005; //traffic camera at Broadway & West 125th

        try {
            nyc_webcams webcam = new nyc_webcams();

            for (Integer x = 0; x < 3; x = x+1 ) {
                data_array[x] = webcam.httpRequest(traffic_cam[x]);
                //data_array[x].logToDisk();

                if (data_array[x].getImage() != null) {
                    //data_array[x].showImage();
                    data_array[x].pubToBucket();
                    data_array[x].pubToPubSub();
                }
            }



        }
        finally {
            System.out.println("Done");
        }
    }

}
