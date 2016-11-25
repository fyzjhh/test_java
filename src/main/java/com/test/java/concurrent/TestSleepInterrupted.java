package com.test.java.concurrent;


public class TestSleepInterrupted {
    /**
     * @param args
     */
    public static void main(String[] args) {
        TestSleepInterrupted main = new TestSleepInterrupted();
        Thread t = new Thread(main.runnable);
        System.out.println("mainmainmain");
        t.start();
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        t.interrupt();
    }

    Runnable runnable = new Runnable() {
        @Override
        public void run() {
            int i = 0;
            try {
                while (i < 1000) {
                    Thread.sleep(5);
                    System.out.println(i++);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.out.println("xxx");
            }
        }
    };
}