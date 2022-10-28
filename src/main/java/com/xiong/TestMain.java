package com.xiong;

public class TestMain {
    public static void main(String[] args) {
        String path = ClassLoader.getSystemClassLoader().getResource("caocao.txt").getPath();
        System.out.println(path);

        String path2 = TestMain.class.getClassLoader().getResource("caocao.txt").getPath();
        System.out.println(path2);

        String path3 = ClassLoader.getSystemClassLoader().getResource("").getPath();
        System.out.println(path3);

    }
}
