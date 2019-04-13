package edu.columbia.wak2116;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.io.IOException;

/*
        W. Aldo Kusmik, WAK2116, ELEN-E6889, Spring 2019
        Final Project
*/

public class SimpleImageLoad extends Frame {
    Image img;

    public SimpleImageLoad() {
        try {
            File imageFile = new File("./src/main/java/images/last_image114.jpg");
            img = ImageIO.read(imageFile);
        } catch (IOException exc) {
            System.out.println("Cannot Load Image File");
            System.exit(0);
        }

        addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent we) {
                System.exit(0);
            }
        });
    }

    public SimpleImageLoad(Image img2) {

        img = img2;

        addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent we) {
                System.exit(0);
            }
        });
    }

    public void paint(Graphics g) {
        g.drawImage(img, getInsets().left, getInsets().top, null);
    }
}

