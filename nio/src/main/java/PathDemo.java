import java.nio.file.Path;
import java.nio.file.Paths;

public class PathDemo {

    public static void main(String[] args) {
        Path path = Paths.get(".", "test");
        System.out.println("path1 = " + path);
        System.out.println("path2 = " + path.toAbsolutePath());
        System.out.println("path3 = " + path.toAbsolutePath().normalize());
    }
}





