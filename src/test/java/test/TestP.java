package test;

public class TestP extends TestG{
    static {
        age = 0;
        System.out.println("TestP");
    }
    public static int age = 1;
    public static int hight = 0;
    static {
        age = 2;
        System.out.println("TestP");
    }


}


